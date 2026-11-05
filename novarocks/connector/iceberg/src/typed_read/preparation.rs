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

//! Speculative Iceberg data input. Metadata decisions stay on the scan CPU;
//! the shared range service remains the only owner of physical read dispatch.

use std::sync::{Arc, Mutex};

use novarocks_fs::{
    BoundFile, FileCancellation, FileIoJob, FileRangeBinding, FileRangeClass, FileRangeControl,
    FileRangeStart, FileReadContext, FileReadRange, PreparedFileInput,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorPreparationControl, ConnectorPreparationExit, ConnectorPreparationProgress,
};
use tokio::sync::Notify;

use crate::file_reader::map_file_error;

pub(super) struct PlannedInput {
    pub file: BoundFile,
    pub range: FileReadRange,
}

type Planner =
    dyn Fn(FileReadContext) -> Result<Option<PlannedInput>, ConnectorError> + Send + Sync;

enum Phase {
    New,
    Planned(Option<Box<PlannedInput>>),
    Reading,
    Ready,
    Failed,
    Stopped,
}

struct State {
    generation: u64,
    phase: Phase,
    paused: bool,
    active_jobs: usize,
    /// The service allocates exactly this requested capacity before dispatch.
    reserved_bytes: u64,
    ready_input: Option<PreparedFileInput>,
    present_input: Option<PreparedFileInput>,
    failure: Option<ConnectorError>,
    cancellation: Option<FileCancellation>,
    request_control: Option<FileRangeControl>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            generation: 0,
            phase: Phase::New,
            paused: false,
            active_jobs: 0,
            reserved_bytes: 0,
            ready_input: None,
            present_input: None,
            failure: None,
            cancellation: None,
            request_control: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparationWatcherExitFailure {
    Join,
    Spawn,
    SpawnErrorProjection,
    SpawnErrorDrop,
}
impl std::fmt::Display for PreparationWatcherExitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Join => "preparation watcher join did not prove resource exit",
            Self::Spawn => "preparation watcher spawn did not prove resource exit",
            Self::SpawnErrorProjection => "preparation spawn error projection panicked",
            Self::SpawnErrorDrop => "preparation spawn error destruction did not prove exit",
        })
    }
}
impl std::error::Error for PreparationWatcherExitFailure {}
impl novarocks_failure_contract::FailureProjection for PreparationWatcherExitFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::Join => 184,
            Self::Spawn => 185,
            Self::SpawnErrorProjection => 186,
            Self::SpawnErrorDrop => 187,
        };
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WatcherPhase {
    Exited,
    Spawning,
    Running,
    Polling,
    Completing,
    ProjectingSpawnError,
    DroppingSpawnError,
    Unresolved,
}
struct WatcherSlot {
    phase: WatcherPhase,
    task: Option<std::mem::ManuallyDrop<FileIoJob>>,
    ticket: Option<
        std::mem::ManuallyDrop<novarocks_spi::connector::read_stack::ConnectorOperationTicket>,
    >,
    body: Option<std::mem::ManuallyDrop<novarocks_fs::FileIoFuture>>,
    spawn_error: Option<std::mem::ManuallyDrop<novarocks_fs::FileError>>,
    join_error: Option<std::mem::ManuallyDrop<tokio::task::JoinError>>,
    // Spawn/cause projection and error-holder Drop are separate once roles.
    payloads: [Option<std::mem::ManuallyDrop<Box<dyn std::any::Any + Send>>>; 2],
    outcome: ConnectorPreparationExit,
}
impl WatcherSlot {
    fn exited() -> Self {
        Self {
            phase: WatcherPhase::Exited,
            task: None,
            ticket: None,
            body: None,
            spawn_error: None,
            join_error: None,
            payloads: [None, None],
            outcome: ConnectorPreparationExit::Exited,
        }
    }
}
struct WatcherWake(Arc<Notify>);
impl std::task::Wake for WatcherWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_waiters();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_waiters();
    }
}
#[cfg(test)]
type BooleanObservationHook = Arc<dyn Fn(bool) + Send + Sync>;
#[cfg(test)]
type PreparedInputDropHook = Arc<dyn Fn(&PreparedFileInput) + Send + Sync>;

struct Shared {
    state: Mutex<State>,
    watcher: Mutex<WatcherSlot>,
    changed: Arc<Notify>,
    join_wake: Arc<WatcherWake>,
    original: std::sync::Weak<Shared>,
    // Cold finite original custody, without a Task control or another registry.
    custody: Mutex<Option<Arc<Shared>>>,
    #[cfg(test)]
    before_watcher_return: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_result_publication: Mutex<Option<BooleanObservationHook>>,
    #[cfg(test)]
    before_result_transaction: Mutex<Option<BooleanObservationHook>>,
    #[cfg(test)]
    before_unused_input_drop: Mutex<Option<PreparedInputDropHook>>,
    #[cfg(test)]
    after_unused_input_drop: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

// A borrowed foreign source remains in the original named role even if a
// projection panics. Returning the loan never runs that source's destructor.
struct SpawnErrorLoan<'a> {
    original: &'a Shared,
    error: Option<std::mem::ManuallyDrop<novarocks_fs::FileError>>,
}
impl Drop for SpawnErrorLoan<'_> {
    fn drop(&mut self) {
        if let Some(error) = self.error.take() {
            self.original
                .watcher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .spawn_error = Some(error);
        }
    }
}

struct WatcherTicketLoan<'a> {
    original: &'a Shared,
    ticket: Option<
        std::mem::ManuallyDrop<novarocks_spi::connector::read_stack::ConnectorOperationTicket>,
    >,
}
impl Drop for WatcherTicketLoan<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.original.watcher_lock().ticket = Some(ticket);
        }
    }
}

// FileIoJob polling borrows the original named role without holding its
// state mutex. Cancellation/unwind returns the same handle to that role.
struct WatcherJobLoan<'a> {
    original: &'a Shared,
    task: Option<std::mem::ManuallyDrop<FileIoJob>>,
}
impl Drop for WatcherJobLoan<'_> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let mut slot = self.original.watcher_lock();
            slot.task = Some(task);
            if slot.phase == WatcherPhase::Polling {
                slot.phase = WatcherPhase::Running;
            }
        }
    }
}

impl Shared {
    #[cfg(test)]
    fn new() -> Arc<Self> {
        Self::new_with_state(State::default())
    }
    fn new_with_state(state: State) -> Arc<Self> {
        Arc::new_cyclic(|original| {
            let changed = Arc::new(Notify::new());
            Self {
                state: Mutex::new(state),
                watcher: Mutex::new(WatcherSlot::exited()),
                changed: changed.clone(),
                join_wake: Arc::new(WatcherWake(changed)),
                original: original.clone(),
                custody: Mutex::new(None),
                #[cfg(test)]
                before_watcher_return: Mutex::new(None),
                #[cfg(test)]
                before_result_publication: Mutex::new(None),
                #[cfg(test)]
                before_result_transaction: Mutex::new(None),
                #[cfg(test)]
                before_unused_input_drop: Mutex::new(None),
                #[cfg(test)]
                after_unused_input_drop: Mutex::new(None),
            }
        })
    }
    fn watcher_lock(&self) -> std::sync::MutexGuard<'_, WatcherSlot> {
        self.watcher.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn install_watcher(
        &self,
        ticket: novarocks_spi::connector::read_stack::ConnectorOperationTicket,
    ) {
        let mut slot = self.watcher_lock();
        assert!(slot.phase == WatcherPhase::Exited && slot.task.is_none());
        slot.phase = WatcherPhase::Spawning;
        slot.ticket = Some(std::mem::ManuallyDrop::new(ticket));
    }
    fn watcher_unknown(&self, failure: PreparationWatcherExitFailure) -> ConnectorPreparationExit {
        let cause = ConnectorError::from_domain(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            failure,
        );
        let (ticket, outcome) = {
            let mut slot = self.watcher_lock();
            if slot.phase == WatcherPhase::Unresolved {
                return slot.outcome.clone();
            }
            slot.phase = WatcherPhase::Unresolved;
            slot.outcome = ConnectorPreparationExit::Unresolved(cause.clone());
            (slot.ticket.take(), slot.outcome.clone())
        };
        *self.custody.lock().unwrap_or_else(|e| e.into_inner()) = self.original.upgrade();
        if let Some(ticket) = ticket {
            std::mem::ManuallyDrop::into_inner(ticket).end(
                novarocks_spi::connector::read_stack::ConnectorOperationExit::CleanupUnresolved(
                    cause,
                ),
            );
        }
        self.changed.notify_waiters();
        outcome
    }
    fn watcher_exited(&self) {
        let (task, ticket) = {
            let mut slot = self.watcher_lock();
            assert!(slot.phase != WatcherPhase::Unresolved);
            slot.phase = WatcherPhase::Exited;
            slot.outcome = ConnectorPreparationExit::Exited;
            (slot.task.take(), slot.ticket.take())
        };
        // poll_drain already consumed the completed join. This FileIoJob is
        // inert nominal metadata; its Drop can no longer abort a future.
        drop(task.map(std::mem::ManuallyDrop::into_inner));
        {
            let mut state = self.lock();
            state.active_jobs = state.active_jobs.saturating_sub(1);
            state.reserved_bytes = 0;
        }
        if let Some(ticket) = ticket {
            std::mem::ManuallyDrop::into_inner(ticket)
                .end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        }
        self.changed.notify_waiters();
    }
    fn spawn_watcher(
        &self,
        spawner: &dyn novarocks_fs::FileIoSpawner,
        body: novarocks_fs::FileIoFuture,
    ) {
        {
            let mut slot = self.watcher_lock();
            assert!(slot.phase == WatcherPhase::Spawning);
            slot.body = Some(std::mem::ManuallyDrop::new(body));
        }
        let body = self
            .watcher_lock()
            .body
            .take()
            .expect("the original installed watcher body");
        let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            spawner.spawn(std::mem::ManuallyDrop::into_inner(body))
        }));
        match spawned {
            Err(payload) => {
                self.watcher_lock().payloads[0] = Some(std::mem::ManuallyDrop::new(payload));
                self.watcher_unknown(PreparationWatcherExitFailure::Spawn);
            }
            Ok(Ok(task)) => {
                let mut slot = self.watcher_lock();
                slot.task = Some(std::mem::ManuallyDrop::new(task));
                slot.phase = WatcherPhase::Running;
                drop(slot);
                self.changed.notify_waiters();
            }
            Ok(Err(error)) => {
                {
                    let mut slot = self.watcher_lock();
                    slot.spawn_error = Some(std::mem::ManuallyDrop::new(error));
                    slot.phase = WatcherPhase::ProjectingSpawnError;
                }
                let mut loan = SpawnErrorLoan {
                    original: self,
                    error: self.watcher_lock().spawn_error.take(),
                };
                let projected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    ConnectorError::from(&**loan.error.as_ref().expect("original spawn error"))
                }));
                let projected = match projected {
                    Ok(cause) => cause,
                    Err(payload) => {
                        self.watcher_lock().payloads[0] =
                            Some(std::mem::ManuallyDrop::new(payload));
                        drop(loan);
                        self.watcher_unknown(PreparationWatcherExitFailure::SpawnErrorProjection);
                        return;
                    }
                };
                // Authored work failure is published before its source holder's
                // foreign Drop; that later cleanup cannot erase the first cause.
                let ticket = WatcherTicketLoan {
                    original: self,
                    ticket: self.watcher_lock().ticket.take(),
                };
                ticket
                    .ticket
                    .as_ref()
                    .expect("original watcher ticket")
                    .report_work_failure(projected.clone());
                drop(ticket);
                self.watcher_lock().phase = WatcherPhase::DroppingSpawnError;
                let error = loan.error.take().expect("once spawn error destruction");
                let disposed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drop(std::mem::ManuallyDrop::into_inner(error));
                }));
                match disposed {
                    Ok(()) => {
                        // Production TokioFileIoSpawner returns a handle.
                        // A supported spawner's Err promises no delivered job
                        // and completed destruction of its supplied future.
                        let mut state = self.lock();
                        if !matches!(state.phase, Phase::Stopped) {
                            state.failure.get_or_insert(projected);
                            state.phase = Phase::Failed;
                        }
                        drop(state);
                        self.watcher_exited();
                    }
                    Err(payload) => {
                        self.watcher_lock().payloads[1] =
                            Some(std::mem::ManuallyDrop::new(payload));
                        self.watcher_unknown(PreparationWatcherExitFailure::SpawnErrorDrop);
                    }
                }
            }
        }
    }
    fn poll_watcher(&self) -> std::task::Poll<ConnectorPreparationExit> {
        let mut loan = {
            let mut slot = self.watcher_lock();
            match slot.phase {
                WatcherPhase::Exited | WatcherPhase::Unresolved => {
                    return std::task::Poll::Ready(slot.outcome.clone());
                }
                WatcherPhase::Running => {}
                _ => return std::task::Poll::Pending,
            }
            slot.phase = WatcherPhase::Polling;
            WatcherJobLoan {
                original: self,
                task: slot.task.take(),
            }
        };
        let waker = std::task::Waker::from(self.join_wake.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            loan.task
                .as_mut()
                .expect("original installed FileIoJob")
                .poll_drain(&mut cx)
        }));
        let joined = match polled {
            Ok(std::task::Poll::Pending) => return std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(Ok(()))) => true,
            Ok(std::task::Poll::Ready(Err(error))) if error.is_cancelled() => {
                // Tokio's completed Cancelled receipt has no opaque payload.
                drop(error);
                true
            }
            Ok(std::task::Poll::Ready(Err(error))) => {
                self.watcher_lock().join_error = Some(std::mem::ManuallyDrop::new(error));
                false
            }
            Err(payload) => {
                self.watcher_lock().payloads[0] = Some(std::mem::ManuallyDrop::new(payload));
                false
            }
        };
        self.watcher_lock().phase = WatcherPhase::Completing;
        drop(loan);
        if joined {
            self.watcher_exited();
            std::task::Poll::Ready(ConnectorPreparationExit::Exited)
        } else {
            std::task::Poll::Ready(self.watcher_unknown(PreparationWatcherExitFailure::Join))
        }
    }
    fn reap_finished_watcher(&self) {
        let ready = {
            let slot = self.watcher_lock();
            slot.phase == WatcherPhase::Running
                && slot.task.as_ref().is_some_and(|task| task.is_finished())
        };
        if ready {
            let _ = self.poll_watcher();
        }
    }
    fn watcher_is_exited(&self) -> bool {
        self.reap_finished_watcher();
        self.watcher_lock().phase == WatcherPhase::Exited
    }
}

#[derive(Default)]
struct GroupState {
    epoch: u64,
    paused: bool,
    stopped: bool,
    reclaim_epoch: u64,
    held_input: Option<PreparedFileInput>,
    controls: Vec<Arc<dyn ConnectorPreparationControl>>,
}

/// Stable control for the lifetime of one active source. A row-group candidate
/// can retire while its physical request is still draining; replacing the
/// candidate never makes those bytes disappear from the source's B ledger.
pub(super) struct SuccessorPreparationGroup {
    state: Mutex<GroupState>,
    changed: Notify,
}

impl SuccessorPreparationGroup {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(GroupState::default()),
            changed: Notify::new(),
        }
    }

    pub fn add(&self, control: Arc<dyn ConnectorPreparationControl>) {
        let drained = self
            .controls()
            .into_iter()
            .filter(|entry| entry.is_drained())
            .collect::<Vec<_>>();
        let (retired, stopped, paused) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let old = std::mem::take(&mut state.controls);
            let mut retired = Vec::new();
            for entry in old {
                if drained.iter().any(|done| Arc::ptr_eq(done, &entry)) {
                    retired.push(entry);
                } else {
                    state.controls.push(entry);
                }
            }
            state.controls.push(control.clone());
            state.epoch = state.epoch.wrapping_add(1);
            (retired, state.stopped, state.paused)
        };
        drop(retired);
        if stopped {
            control.request_stop();
        } else if paused {
            control.request_pause();
        }
        self.changed.notify_waiters();
    }

    pub fn reclaim_epoch(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reclaim_epoch
    }

    pub fn hold_input(&self, input: PreparedFileInput) {
        let retired = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .held_input
            .replace(input);
        drop(retired);
    }

    pub fn take_input(&self) -> Option<PreparedFileInput> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .held_input
            .take()
    }

    pub fn input(&self) -> Option<PreparedFileInput> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .held_input
            .clone()
    }

    pub fn undrained_candidate_count(&self) -> usize {
        self.controls()
            .iter()
            .filter(|control| !control.is_drained())
            .count()
    }

    fn controls(&self) -> Vec<Arc<dyn ConnectorPreparationControl>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .controls
            .clone()
    }
}

impl ConnectorPreparationControl for SuccessorPreparationGroup {
    fn request_pause(&self) {
        let controls = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.paused = true;
            state.controls.clone()
        };
        for control in controls {
            control.request_pause();
        }
    }

    fn request_resume(&self) {
        let controls = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.paused = false;
            state.controls.clone()
        };
        for control in controls {
            control.request_resume();
        }
    }

    fn request_reclaim(&self) {
        let (held, controls) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.reclaim_epoch = state.reclaim_epoch.wrapping_add(1);
            state.epoch = state.epoch.wrapping_add(1);
            (state.held_input.take(), state.controls.clone())
        };
        drop(held);
        for control in controls {
            control.request_reclaim();
        }
        self.changed.notify_waiters();
    }

    fn request_stop(&self) {
        let (held, controls) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.stopped = true;
            state.epoch = state.epoch.wrapping_add(1);
            (state.held_input.take(), state.controls.clone())
        };
        drop(held);
        for control in controls {
            control.request_stop();
        }
        self.changed.notify_waiters();
    }

    fn retained_input_bytes(&self) -> u64 {
        let (held, controls) = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            (
                state
                    .held_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64),
                state.controls.clone(),
            )
        };
        controls.iter().fold(held, |sum, control| {
            sum.saturating_add(control.retained_input_bytes())
        })
    }

    fn is_drained(&self) -> bool {
        self.controls().iter().all(|control| control.is_drained())
    }

    fn wait_drained(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ConnectorPreparationExit> + Send + '_>>
    {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let (epoch, controls) = {
                    let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    (state.epoch, state.controls.clone())
                };
                let joined = futures::future::join_all(
                    controls.iter().map(|control| control.wait_drained()),
                );
                tokio::pin!(joined);
                tokio::select! {
                    outcomes = &mut joined => {
                        if self.state.lock().unwrap_or_else(|e| e.into_inner()).epoch != epoch { continue; }
                        return outcomes.into_iter().find(|outcome| matches!(outcome, ConnectorPreparationExit::Unresolved(_)))
                            .unwrap_or(ConnectorPreparationExit::Exited);
                    }
                    _ = &mut changed => {},
                }
            }
        })
    }
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn publish_job(
        &self,
        generation: u64,
        result: Result<PreparedFileInput, ConnectorError>,
        drain: Result<(), ConnectorError>,
    ) {
        let (mut input, mut failure) = match result {
            Ok(input) => (Some(input), None),
            Err(error) => (None, Some(error)),
        };
        let mut later_failure = drain.err();
        if failure.is_none() {
            failure = later_failure.take();
        }
        #[cfg(test)]
        {
            // Observation only: never authorizes publication. A concurrent
            // stop/reclaim must still win at the one transaction below.
            let observed_current = {
                let state = self.lock();
                state.generation == generation && !matches!(state.phase, Phase::Stopped)
            };
            let hook = self.before_result_transaction.lock().unwrap().take();
            if let Some(hook) = hook {
                hook(observed_current);
            }
        }
        let (retired_control, retired_phase, retired_ready) = {
            let mut state = self.lock();
            let current = state.generation == generation && !matches!(state.phase, Phase::Stopped);
            let mut retired_control = None;
            let mut retired_phase = None;
            let mut retired_ready = None;
            if current {
                retired_control = state.request_control.take();
                if failure.is_some() || state.failure.is_some() {
                    if state.failure.is_none() {
                        state.failure = failure.take();
                    }
                    retired_phase = Some(std::mem::replace(&mut state.phase, Phase::Failed));
                    // An unused successful input may still need destruction.
                    // Keep its original reservation until the real watcher join.
                } else {
                    retired_ready = state
                        .ready_input
                        .replace(input.take().expect("successful prepared input"));
                    retired_phase = Some(std::mem::replace(&mut state.phase, Phase::Ready));
                    // Move responsibility to the exact ready backing. If an
                    // older holder was replaced, its exit remains in this job.
                    state.reserved_bytes = retired_ready
                        .as_ref()
                        .map_or(0, |input| input.retained_backing_capacity() as u64);
                }
            } else if !matches!(state.phase, Phase::Stopped)
                && failure.as_ref().is_some_and(|error| {
                    error.kind() != novarocks_spi::connector::ConnectorErrorKind::Cancelled
                })
            {
                // Reclaim invalidates generation, not an already observed
                // non-cancellation I/O verdict. Stop does not resurrect it.
                if state.failure.is_none() {
                    state.failure = failure.take();
                }
                retired_phase = Some(std::mem::replace(&mut state.phase, Phase::Failed));
            }
            (retired_control, retired_phase, retired_ready)
        };
        // No actual holder or rich source is destroyed under the state lock.
        drop(retired_control);
        drop(retired_phase);
        drop(retired_ready);
        #[cfg(test)]
        if let Some(input) = &input {
            let hook = self.before_unused_input_drop.lock().unwrap().take();
            if let Some(hook) = hook {
                hook(input);
            }
        }
        drop(input);
        #[cfg(test)]
        {
            let hook = self.after_unused_input_drop.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        drop(failure);
        drop(later_failure);
        self.changed.notify_waiters();
    }
}

impl ConnectorPreparationControl for Shared {
    fn request_pause(&self) {
        let control = {
            let mut state = self.lock();
            state.paused = true;
            state.request_control.clone()
        };
        if let Some(control) = control {
            control.request_pause();
        }
    }

    fn request_resume(&self) {
        let control = {
            let mut state = self.lock();
            state.paused = false;
            state.request_control.clone()
        };
        if let Some(control) = control {
            control.request_resume();
        }
        self.changed.notify_waiters();
    }

    fn request_reclaim(&self) {
        let (cancel, control, ready, present, retired_phase) = {
            let mut state = self.lock();
            state.generation = state.generation.wrapping_add(1);
            let retired_phase = if matches!(state.phase, Phase::Failed) {
                None
            } else {
                Some(std::mem::replace(&mut state.phase, Phase::New))
            };
            if state.active_jobs == 0 {
                state.reserved_bytes = 0;
            }
            (
                state.cancellation.take(),
                state.request_control.take(),
                state.ready_input.take(),
                state.present_input.take(),
                retired_phase,
            )
        };
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
        drop(control);
        drop(ready);
        drop(present);
        drop(retired_phase);
        self.changed.notify_waiters();
    }

    fn request_stop(&self) {
        let (cancel, control, ready, present, failure, retired_phase) = {
            let mut state = self.lock();
            state.generation = state.generation.wrapping_add(1);
            let retired_phase = std::mem::replace(&mut state.phase, Phase::Stopped);
            if state.active_jobs == 0 {
                state.reserved_bytes = 0;
            }
            (
                state.cancellation.take(),
                state.request_control.take(),
                state.ready_input.take(),
                state.present_input.take(),
                state.failure.take(),
                retired_phase,
            )
        };
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
        drop(control);
        drop(ready);
        drop(present);
        drop(failure);
        drop(retired_phase);
        self.changed.notify_waiters();
    }

    fn retained_input_bytes(&self) -> u64 {
        let state = self.lock();
        state
            .reserved_bytes
            .saturating_add(
                state
                    .present_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64),
            )
            .saturating_add(
                state
                    .ready_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64),
            )
    }

    fn is_drained(&self) -> bool {
        if !self.watcher_is_exited() {
            return false;
        }
        let state = self.lock();
        state.active_jobs == 0 && state.reserved_bytes == 0
    }

    fn wait_drained(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ConnectorPreparationExit> + Send + '_>>
    {
        Box::pin(async move {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match self.poll_watcher() {
                    std::task::Poll::Ready(outcome @ ConnectorPreparationExit::Unresolved(_)) => {
                        return outcome;
                    }
                    std::task::Poll::Ready(ConnectorPreparationExit::Exited)
                        if self.is_drained() =>
                    {
                        return ConnectorPreparationExit::Exited;
                    }
                    _ => notified.await,
                }
            }
        })
    }
}

/// One bounded candidate. Reclaim discards its input without affecting the
/// active source and allows the same immutable planner to re-arm later.
pub(super) struct PreparedRangeCandidate {
    context: FileReadContext,
    range: FileRangeBinding,
    planner: Arc<Planner>,
    allow_partial: bool,
    shared: Arc<Shared>,
}

impl PreparedRangeCandidate {
    pub fn new(context: FileReadContext, planner: Arc<Planner>) -> Option<Self> {
        Self::new_with_present(context, planner, None, true)
    }

    pub fn new_with_present(
        context: FileReadContext,
        planner: Arc<Planner>,
        present_input: Option<PreparedFileInput>,
        allow_partial: bool,
    ) -> Option<Self> {
        let state = State {
            present_input,
            ..State::default()
        };
        Some(Self {
            range: context.range.clone()?,
            context,
            planner,
            allow_partial,
            shared: Shared::new_with_state(state),
        })
    }

    pub fn control(&self) -> Arc<dyn ConnectorPreparationControl> {
        Arc::clone(&self.shared) as Arc<dyn ConnectorPreparationControl>
    }

    pub fn advance(
        &mut self,
        remaining_input_bytes: u64,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        self.shared.reap_finished_watcher();
        if self.shared.watcher_lock().phase == WatcherPhase::Unresolved {
            return Ok(ConnectorPreparationProgress::Deferred);
        }
        let mut state = self.shared.lock();
        if state.paused || state.active_jobs > 0 && matches!(state.phase, Phase::New) {
            return Ok(ConnectorPreparationProgress::Pending);
        }
        match &state.phase {
            Phase::New => {
                let mut context = self.context.clone();
                context.cancellation = self.context.cancellation.child();
                let generation = state.generation;
                drop(state);
                let planned = (self.planner)(context);
                let mut state = self.shared.lock();
                if state.generation != generation || matches!(state.phase, Phase::Stopped) {
                    drop(state);
                    drop(planned);
                    return Ok(ConnectorPreparationProgress::Deferred);
                }
                match planned {
                    Ok(planned) => state.phase = Phase::Planned(planned.map(Box::new)),
                    Err(error) => {
                        state.failure = Some(error);
                        state.phase = Phase::Failed;
                    }
                }
                Ok(ConnectorPreparationProgress::Pending)
            }
            Phase::Reading => Ok(ConnectorPreparationProgress::Pending),
            Phase::Ready | Phase::Failed => Ok(ConnectorPreparationProgress::Ready),
            Phase::Stopped => Ok(ConnectorPreparationProgress::Deferred),
            Phase::Planned(planned) => {
                let Some(planned) = planned.as_ref() else {
                    state.phase = Phase::Ready;
                    return Ok(ConnectorPreparationProgress::Ready);
                };
                let (offset, available) = match planned.range {
                    FileReadRange::Bounded { offset, length } => (offset, length),
                    FileReadRange::WholeFile => (0, planned.file.identity().file_size()),
                };
                let length = if !self.allow_partial {
                    if available > remaining_input_bytes {
                        return Ok(ConnectorPreparationProgress::Deferred);
                    }
                    available
                } else {
                    available.min(8 * 1024 * 1024).min(remaining_input_bytes)
                };
                if length == 0 {
                    return Ok(ConnectorPreparationProgress::Deferred);
                }
                let planned_snapshot = Some(Box::new(PlannedInput {
                    file: planned.file.clone(),
                    range: planned.range,
                }));
                let cancellation = self.context.cancellation.child();
                let range = FileReadRange::bounded(offset, length).map_err(map_file_error)?;
                let file = planned.file.clone();
                let present_bytes = state
                    .present_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64);
                let retained_after_start = length.checked_add(present_bytes).ok_or_else(|| {
                    ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::Internal,
                        "prepared range capacity overflow",
                    )
                })?;
                // Install original ownership before allocation, dispatch or spawn.
                state.reserved_bytes = length;
                state.active_jobs += 1;
                state.cancellation = Some(cancellation.clone());
                state.phase = Phase::Reading;
                let generation = state.generation;
                let present = state.present_input.clone();
                drop(state);
                let weak = Arc::downgrade(&self.shared);
                let ticket = self.range.operations().admit(Arc::new(move || {
                    if let Some(shared) = weak.upgrade() {
                        shared.request_stop();
                    }
                }));
                let ticket = match ticket {
                    Ok(ticket) => ticket,
                    Err(error) => {
                        let mut state = self.shared.lock();
                        state.active_jobs = state.active_jobs.saturating_sub(1);
                        state.reserved_bytes = 0;
                        if state.generation == generation && !matches!(state.phase, Phase::Stopped)
                        {
                            state.failure = Some(error);
                            state.phase = Phase::Failed;
                        }
                        drop(state);
                        self.shared.changed.notify_waiters();
                        return Ok(ConnectorPreparationProgress::Deferred);
                    }
                };
                self.shared.install_watcher(ticket);
                let start = self
                    .range
                    .try_start_with_present(
                        FileRangeClass::Prefetch,
                        file,
                        range,
                        cancellation,
                        present,
                    )
                    .map_err(map_file_error);
                let mut request = match start {
                    Ok(FileRangeStart::Started(request)) => request,
                    outcome => {
                        let mut state = self.shared.lock();
                        if state.generation == generation && !matches!(state.phase, Phase::Stopped)
                        {
                            match outcome {
                                Ok(FileRangeStart::Deferred) => {
                                    state.phase = Phase::Planned(planned_snapshot)
                                }
                                Err(error) => {
                                    state.failure = Some(error);
                                    state.phase = Phase::Failed;
                                }
                                Ok(FileRangeStart::Started(_)) => unreachable!(),
                            }
                        }
                        drop(state);
                        self.shared.watcher_exited();
                        return Ok(ConnectorPreparationProgress::Deferred);
                    }
                };
                let retired_present = {
                    let mut state = self.shared.lock();
                    if state.generation == generation && !matches!(state.phase, Phase::Stopped) {
                        state.request_control = Some(request.control());
                    }
                    state.reserved_bytes = retained_after_start;
                    state.present_input.take()
                };
                drop(retired_present);
                let weak = Arc::downgrade(&self.shared);
                let body = Box::pin(async move {
                    let result = request.prepared_input_ready().await;
                    let drain = request.drained().await;
                    let result = result.map_err(map_file_error);
                    let drain = drain.map_err(map_file_error);
                    let Some(shared) = weak.upgrade() else {
                        return;
                    };
                    #[cfg(test)]
                    {
                        let hook = shared.before_result_publication.lock().unwrap().take();
                        if let Some(hook) = hook {
                            hook(result.is_ok());
                        }
                    }
                    shared.publish_job(generation, result, drain);
                    #[cfg(test)]
                    {
                        let hook = shared.before_watcher_return.lock().unwrap().take();
                        if let Some(hook) = hook {
                            hook();
                        }
                    }
                });
                self.shared
                    .spawn_watcher(self.context.io_spawner.as_ref(), body);
                Ok(ConnectorPreparationProgress::Pending)
            }
        }
    }

    pub fn take_ready(&mut self) -> Result<Option<PreparedFileInput>, ConnectorError> {
        let mut state = self.shared.lock();
        if let Some(error) = state.failure.take() {
            return Err(error);
        }
        let input = state.ready_input.take();
        if input.is_some() {
            state.phase = Phase::Stopped;
        }
        Ok(input)
    }
}

impl Drop for PreparedRangeCandidate {
    fn drop(&mut self) {
        self.shared.request_stop();
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use novarocks_fs::{
        FileIdentity, FileIoRuntime, FileIoSpawner, FileRangeScope, FileRangeService,
        FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner,
    };
    use novarocks_spi::connector::StorageAccessDomainId;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn prefetch_respects_remaining_capacity_and_transfers_exact_input() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("input.parquet");
        std::fs::write(&path, b"abcdefghijklmnop").expect("write input");
        let location = path.to_string_lossy().to_string();
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([7; 32]),
                location.clone(),
                None,
            )
            .expect("resolve input");
        let file = access
            .bind(0, FileIdentity::new(location, 16, None))
            .expect("bind input");
        let io_spawner: Arc<dyn FileIoSpawner> =
            Arc::new(TokioFileIoSpawner::new(tokio::runtime::Handle::current()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::clone(&io_spawner),
            tokio::runtime::Handle::current(),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(10)),
            runtime: Arc::new(TokioFileIoRuntime::new(tokio::runtime::Handle::current()))
                as Arc<dyn FileIoRuntime>,
            io_spawner,
            range: Some(service.bind(
                FileRangeScope::try_new(1, 0, 1, 2, 0, 3).unwrap(),
                novarocks_spi::connector::read_stack::ConnectorSourceOperations::new(),
            )),
        };
        let planner = Arc::new(move |_context: FileReadContext| {
            Ok(Some(PlannedInput {
                file: file.clone(),
                range: FileReadRange::bounded(0, 16).map_err(map_file_error)?,
            }))
        });
        let mut candidate = PreparedRangeCandidate::new(context, planner).expect("candidate");
        let control = candidate.control();
        let input = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let progress = candidate.advance(4).expect("advance candidate");
                assert!(candidate.control().retained_input_bytes() <= 4);
                if progress == ConnectorPreparationProgress::Ready {
                    return candidate.take_ready().expect("ready input");
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let state = candidate.shared.lock();
            panic!(
                "candidate readiness: phase={} jobs={} reserved={} paused={}",
                match state.phase {
                    Phase::New => "new",
                    Phase::Planned(_) => "planned",
                    Phase::Reading => "reading",
                    Phase::Ready => "ready",
                    Phase::Failed => "failed",
                    Phase::Stopped => "stopped",
                },
                state.active_jobs,
                state.reserved_bytes,
                state.paused,
            );
        })
        .expect("physical prepared input");
        assert_eq!(input.range(), 0..4);
        assert_eq!(input.retained_backing_capacity(), 4);
        assert_eq!(candidate.control().retained_input_bytes(), 0);
        drop(candidate);
        assert!(matches!(
            control.wait_drained().await,
            ConnectorPreparationExit::Exited
        ));
        assert!(control.is_drained());
    }

    fn start_owned_test_watcher(
        shared: &Arc<Shared>,
        operations: &novarocks_spi::connector::read_stack::ConnectorSourceOperations,
        body: novarocks_fs::FileIoFuture,
    ) {
        {
            let mut state = shared.lock();
            state.active_jobs = 1;
            state.reserved_bytes = 8;
        }
        shared.install_watcher(
            operations
                .admit(Arc::new(|| {}))
                .expect("watcher admission"),
        );
        shared.spawn_watcher(
            &TokioFileIoSpawner::new(tokio::runtime::Handle::current()),
            body,
        );
    }

    #[tokio::test]
    async fn reclaim_preserves_a_real_error_and_wakes_actual_drain_waiter() {
        let shared = Shared::new();
        let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
        let original = ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            "saved candidate error",
        );
        shared.lock().failure = Some(original.clone());
        shared.lock().phase = Phase::Failed;
        let (release, gate) = tokio::sync::oneshot::channel();
        let backing = Arc::new(vec![1_u8; 8]);
        let weak = Arc::downgrade(&backing);
        start_owned_test_watcher(
            &shared,
            &operations,
            Box::pin(async move {
                let _ = gate.await;
                drop(backing);
            }),
        );
        shared.request_reclaim();
        let state_before = {
            let state = shared.lock();
            (
                matches!(state.phase, Phase::Failed),
                state.failure.clone(),
                state.reserved_bytes,
            )
        };
        let mut waiter = shared.wait_drained();
        let pending = futures::poll!(waiter.as_mut()).is_pending();
        let owned_before = weak.upgrade().is_some();
        let _ = release.send(());
        let outcome = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("actual join wake");
        assert_eq!(state_before, (true, Some(original), 8));
        assert!(pending && owned_before);
        assert!(matches!(outcome, ConnectorPreparationExit::Exited));
        assert!(weak.upgrade().is_none());
        assert!(shared.is_drained());
        assert_eq!(operations.live_operations(), 0);
    }

    #[tokio::test]
    async fn reclaim_keeps_a_physical_error_published_after_generation_changed() {
        let shared = Shared::new();
        let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
        shared.lock().phase = Phase::Reading;
        let (release, gate) = tokio::sync::oneshot::channel();
        start_owned_test_watcher(
            &shared,
            &operations,
            Box::pin(async move {
                let _ = gate.await;
            }),
        );
        shared.request_reclaim();
        let original = ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            "physical corruption before cancellation",
        );
        shared.publish_job(0, Err(original.clone()), Ok(()));
        let state_before = {
            let state = shared.lock();
            (
                matches!(state.phase, Phase::Failed),
                state.failure.clone(),
                state.reserved_bytes,
            )
        };
        let not_exited = !shared.is_drained();
        let _ = release.send(());
        let outcome = tokio::time::timeout(Duration::from_secs(1), shared.wait_drained())
            .await
            .expect("real watcher join");
        assert!(not_exited);
        assert_eq!(state_before, (true, Some(original.clone()), 8));
        assert!(matches!(outcome, ConnectorPreparationExit::Exited));
        assert_eq!(shared.lock().failure.as_ref(), Some(&original));
        assert_eq!(shared.lock().reserved_bytes, 0);
        assert!(shared.is_drained());
    }

    struct WatcherWakeCount(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for WatcherWakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    struct TailRelease(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for TailRelease {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actual_input_and_original_join_survive_reclaim_tail_and_cancelled_waiter() {
        use super::super::test_spawners::{CountingFileIoSpawner, route_reads_through};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("prepared-input");
        std::fs::write(&path, b"abcdefghijklmnop").expect("input");
        let location = path.to_string_lossy().to_string();
        let file = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([9; 32]),
                location.clone(),
                None,
            )
            .expect("access")
            .bind(0, FileIdentity::new(location, 16, None))
            .expect("file");
        let spawner = Arc::new(CountingFileIoSpawner {
            handle: tokio::runtime::Handle::current(),
            spawned: AtomicUsize::new(0),
        });
        let mut context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(10)),
            runtime: Arc::new(TokioFileIoRuntime::new(tokio::runtime::Handle::current())),
            io_spawner: spawner.clone(),
            range: None,
        };
        let operations = route_reads_through(
            &mut context,
            spawner.clone(),
            tokio::runtime::Handle::current(),
        );
        let planner = Arc::new(move |_: FileReadContext| {
            Ok(Some(PlannedInput {
                file: file.clone(),
                range: FileReadRange::bounded(0, 16).map_err(map_file_error)?,
            }))
        });
        let mut candidate = PreparedRangeCandidate::new(context, planner).expect("candidate");
        let control = candidate.control();
        let had_real_input = Arc::new(AtomicBool::new(false));
        let weak = Arc::downgrade(&candidate.shared);
        let observed = had_real_input.clone();
        *candidate.shared.before_result_publication.lock().unwrap() = Some(Arc::new(move |ok| {
            observed.store(ok, Ordering::SeqCst);
            weak.upgrade().expect("original owner").request_reclaim();
        }));
        let entered = Arc::new(Notify::new());
        let entered_hook = entered.clone();
        let (release, gate) = std::sync::mpsc::channel();
        let gate = Mutex::new(gate);
        let release = TailRelease(Some(release));
        *candidate.shared.before_watcher_return.lock().unwrap() = Some(Arc::new(move || {
            entered_hook.notify_one();
            let _ = gate.lock().unwrap().recv();
        }));
        candidate.advance(4).expect("plan");
        candidate.advance(4).expect("dispatch");
        tokio::time::timeout(Duration::from_secs(10), entered.notified())
            .await
            .expect("actual result tail");
        let calls_before = spawner.spawned.load(Ordering::SeqCst);
        let bytes_before = control.retained_input_bytes();
        let jobs_before = candidate.shared.lock().active_jobs;
        let progress = candidate
            .advance(4)
            .expect("cannot rearm original live join");
        let calls_after = spawner.spawned.load(Ordering::SeqCst);
        // Seal invokes the real stop hook and notifies existing drain waiters.
        // Complete that transition before registering the waiters whose wake
        // below must come from the still-gated original join.
        operations.seal();
        let wake_one = Arc::new(WatcherWakeCount(AtomicUsize::new(0)));
        let wake_two = Arc::new(WatcherWakeCount(AtomicUsize::new(0)));
        let one = std::task::Waker::from(wake_one);
        let two = std::task::Waker::from(wake_two.clone());
        let mut cancelled = control.wait_drained();
        let mut survivor = control.wait_drained();
        let first_pending = cancelled
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&one))
            .is_pending();
        let second_pending = survivor
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&two))
            .is_pending();
        drop(cancelled);
        let mut late = control.wait_drained();
        let late_pending = futures::poll!(late.as_mut()).is_pending();
        let source_before = operations.live_operations();
        let physical_pending = !control.is_drained();
        drop(release);
        tokio::time::timeout(Duration::from_secs(10), async {
            while wake_two.0.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original join metadata wake reaches the surviving registered waiter");
        let (survivor_exit, late_exit) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(survivor, late)
        })
        .await
        .expect("same original join wakes all remaining waiters");
        let source_exit = operations.exited().await;
        assert!(had_real_input.load(Ordering::SeqCst));
        assert_eq!((bytes_before, jobs_before), (4, 1));
        assert_eq!(calls_before, calls_after);
        assert_eq!(progress, ConnectorPreparationProgress::Pending);
        assert!(first_pending && second_pending && late_pending && physical_pending);
        assert_eq!(
            source_before, 1,
            "the FS request retired but its watcher body still owns input"
        );
        assert!(matches!(survivor_exit, ConnectorPreparationExit::Exited));
        assert!(matches!(late_exit, ConnectorPreparationExit::Exited));
        assert_eq!(
            source_exit,
            novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited
        );
        assert!(control.is_drained());
        assert_eq!(control.retained_input_bytes(), 0);
        assert_eq!(candidate.shared.lock().active_jobs, 0);
    }

    struct JoinOpaque(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for JoinOpaque {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("an opaque join payload must not be destructed by observation");
        }
    }

    #[tokio::test]
    async fn watcher_join_panic_is_nominal_cleanup_and_does_not_hide_later_work_failure() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::read_stack::{
            ConnectorOperationExit, ConnectorSourceExitProof, ConnectorSourceFailureEvent,
        };
        let root = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
        let child = root.child().expect("child");
        let mut events = root.claim_failure_receiver().expect("unique root receiver");
        let shared = Shared::new();
        let weak = Arc::downgrade(&shared);
        let payload_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let payload = payload_drops.clone();
        start_owned_test_watcher(
            &shared,
            &child,
            Box::pin(async move {
                std::panic::panic_any(JoinOpaque(payload));
            }),
        );
        let later = child.admit(Arc::new(|| {})).expect("accepted sibling");
        let observed = tokio::time::timeout(Duration::from_secs(1), shared.wait_drained())
            .await
            .expect("join observation");
        let ConnectorPreparationExit::Unresolved(cause) = observed else {
            panic!("panic never proves exit");
        };
        assert_eq!(
            cause.failure_fact(),
            PreparationWatcherExitFailure::Join.failure_fact()
        );
        assert_eq!(
            std::error::Error::source(&cause)
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<PreparationWatcherExitFailure>(),
            Some(&PreparationWatcherExitFailure::Join)
        );
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        let std::task::Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(event))) =
            events.poll_event(&mut cx)
        else {
            panic!("cleanup event");
        };
        assert_eq!(
            event, cause,
            "same nominal occurrence crosses the original root ledger"
        );
        assert!(events.poll_event(&mut cx).is_pending());
        assert_eq!(shared.lock().reserved_bytes, 8);
        assert!(!shared.is_drained());
        let original = ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            "later actual data failure",
        );
        later.report_work_failure(original.clone());
        let std::task::Poll::Ready(Some(ConnectorSourceFailureEvent::Required(event))) =
            events.poll_event(&mut cx)
        else {
            panic!("required event after unknown cleanup");
        };
        assert_eq!(event, original);
        assert_eq!(
            child.live_operations(),
            1,
            "reporting a failure never completes its original slot"
        );
        later.end(ConnectorOperationExit::Exited);
        root.seal();
        assert_eq!(root.exited().await, ConnectorSourceExitProof::Unresolved);
        assert!(matches!(
            events.poll_event(&mut cx),
            std::task::Poll::Ready(None)
        ));
        let repeated = shared.wait_drained().await;
        assert!(
            matches!(repeated, ConnectorPreparationExit::Unresolved(ref again) if *again == cause)
        );
        drop(shared);
        assert!(
            weak.upgrade().is_some(),
            "unknown original join remains in finite original custody"
        );
        assert_eq!(payload_drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RejectMode {
        Normal,
        SpawnPanic,
        ProjectionPanic,
        SourceDropPanic,
    }
    #[derive(Debug)]
    struct RejectSource {
        mode: RejectMode,
        projections: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        payload_drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl std::fmt::Display for RejectSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.projections
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.mode == RejectMode::ProjectionPanic {
                std::panic::panic_any(JoinOpaque(self.payload_drops.clone()));
            }
            f.write_str("original watcher spawn capacity was exhausted")
        }
    }
    impl std::error::Error for RejectSource {}
    impl Drop for RejectSource {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.mode == RejectMode::SourceDropPanic {
                std::panic::panic_any(JoinOpaque(self.payload_drops.clone()));
            }
        }
    }
    struct RejectWatcherSpawner {
        mode: RejectMode,
        projections: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        payload_drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl FileIoSpawner for RejectWatcherSpawner {
        fn spawn(&self, task: novarocks_fs::FileIoFuture) -> novarocks_fs::FileResult<FileIoJob> {
            // This exact spawner delivers no job and returns only after the
            // future and its real captures have been destructed normally.
            drop(task);
            if self.mode == RejectMode::SpawnPanic {
                std::panic::panic_any(JoinOpaque(self.payload_drops.clone()));
            }
            Err(novarocks_fs::FileError::with_source(
                novarocks_fs::FileErrorKind::ResourceExhausted,
                "watcher rejected",
                RejectSource {
                    mode: self.mode,
                    projections: self.projections.clone(),
                    drops: self.drops.clone(),
                    payload_drops: self.payload_drops.clone(),
                },
            ))
        }
        fn spawn_detached_blocking(&self, _: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!("not used by a watcher");
        }
    }
    #[tokio::test]
    async fn watcher_spawn_roles_publish_known_work_before_unknown_source_retirement_once() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::read_stack::{
            ConnectorSourceExitProof, ConnectorSourceFailureEvent,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        for mode in [
            RejectMode::Normal,
            RejectMode::SpawnPanic,
            RejectMode::ProjectionPanic,
            RejectMode::SourceDropPanic,
        ] {
            let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
            let mut events = operations.claim_failure_receiver().unwrap();
            let shared = Shared::new();
            {
                let mut state = shared.lock();
                state.active_jobs = 1;
                state.reserved_bytes = 8;
                state.phase = Phase::Reading;
            }
            shared.install_watcher(operations.admit(Arc::new(|| {})).unwrap());
            let spawner = RejectWatcherSpawner {
                mode,
                projections: Arc::new(AtomicUsize::new(0)),
                drops: Arc::new(AtomicUsize::new(0)),
                payload_drops: Arc::new(AtomicUsize::new(0)),
            };
            let input = Arc::new(vec![4_u8; 8]);
            let weak = Arc::downgrade(&input);
            shared.spawn_watcher(
                &spawner,
                Box::pin(async move {
                    drop(input);
                }),
            );
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            let known = matches!(mode, RejectMode::Normal | RejectMode::SourceDropPanic);
            if known {
                let std::task::Poll::Ready(Some(ConnectorSourceFailureEvent::Required(cause))) =
                    events.poll_event(&mut cx)
                else {
                    panic!("successfully projected source error is Required");
                };
                assert_eq!(
                    cause.kind(),
                    novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
                );
                assert_eq!(
                    cause.to_string(),
                    "ResourceExhausted: ResourceExhausted: watcher rejected: original watcher spawn capacity was exhausted"
                );
                assert_eq!(spawner.projections.load(Ordering::SeqCst), 1);
            }
            let proof = shared.wait_drained().await;
            if mode == RejectMode::Normal {
                assert!(matches!(proof, ConnectorPreparationExit::Exited));
                assert!(shared.is_drained());
                assert_eq!(spawner.drops.load(Ordering::SeqCst), 1);
                assert_eq!(shared.lock().reserved_bytes, 0);
                assert!(events.poll_event(&mut cx).is_pending());
            } else {
                let ConnectorPreparationExit::Unresolved(cause) = proof else {
                    panic!("foreign panic is not NoJob proof");
                };
                let expected = match mode {
                    RejectMode::SpawnPanic => PreparationWatcherExitFailure::Spawn,
                    RejectMode::ProjectionPanic => {
                        PreparationWatcherExitFailure::SpawnErrorProjection
                    }
                    RejectMode::SourceDropPanic => PreparationWatcherExitFailure::SpawnErrorDrop,
                    RejectMode::Normal => unreachable!(),
                };
                assert_eq!(cause.failure_fact(), expected.failure_fact());
                let std::task::Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(
                    event,
                ))) = events.poll_event(&mut cx)
                else {
                    panic!("once cleanup event");
                };
                assert_eq!(event, cause);
                assert!(
                    matches!(shared.wait_drained().await, ConnectorPreparationExit::Unresolved(ref repeat) if *repeat == cause)
                );
                assert_eq!(shared.lock().reserved_bytes, 8);
                assert!(!shared.is_drained());
                if mode == RejectMode::ProjectionPanic {
                    assert_eq!(spawner.drops.load(Ordering::SeqCst), 0);
                }
                if mode == RejectMode::SourceDropPanic {
                    assert_eq!(spawner.drops.load(Ordering::SeqCst), 1);
                }
            }
            operations.seal();
            let expected_proof = if mode == RejectMode::Normal {
                ConnectorSourceExitProof::Exited
            } else {
                ConnectorSourceExitProof::Unresolved
            };
            assert_eq!(operations.exited().await, expected_proof);
            assert!(matches!(
                events.poll_event(&mut cx),
                std::task::Poll::Ready(None)
            ));
            assert!(
                weak.upgrade().is_none(),
                "supported spawn Err/panic path did destruct the original supplied future"
            );
            assert_eq!(spawner.payload_drops.load(Ordering::SeqCst), 0);
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_and_reclaim_between_result_observation_and_commit_do_not_resurrect_real_input() {
        use super::super::test_spawners::{CountingFileIoSpawner, route_reads_through};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        for stop in [false, true] {
            let directory = tempfile::tempdir().expect("real input directory");
            let path = directory.path().join("publication-input");
            std::fs::write(&path, b"abcdefghijklmnop").expect("actual input");
            let location = path.to_string_lossy().to_string();
            let file = FsAccessResolver::new()
                .resolve_location(
                    StorageAccessDomainId::from_bytes([11; 32]),
                    location.clone(),
                    None,
                )
                .expect("access")
                .bind(0, FileIdentity::new(location, 16, None))
                .expect("file");
            let spawner = Arc::new(CountingFileIoSpawner {
                handle: tokio::runtime::Handle::current(),
                spawned: AtomicUsize::new(0),
            });
            let mut context = FileReadContext {
                cancellation: FileCancellation::new(),
                deadline: Some(Instant::now() + Duration::from_secs(10)),
                runtime: Arc::new(TokioFileIoRuntime::new(tokio::runtime::Handle::current())),
                io_spawner: spawner.clone(),
                range: None,
            };
            let operations =
                route_reads_through(&mut context, spawner, tokio::runtime::Handle::current());
            let planner = Arc::new(move |_: FileReadContext| {
                Ok(Some(PlannedInput {
                    file: file.clone(),
                    range: FileReadRange::bounded(0, 16).map_err(map_file_error)?,
                }))
            });
            let mut candidate = PreparedRangeCandidate::new(context, planner).expect("candidate");
            let control = candidate.control();
            let (transaction_send, transaction_entered) = std::sync::mpsc::channel();
            let (transaction_release, transaction_gate) = std::sync::mpsc::channel();
            let transaction_release = TailRelease(Some(transaction_release));
            let transaction_gate = Mutex::new(transaction_gate);
            *candidate.shared.before_result_transaction.lock().unwrap() =
                Some(Arc::new(move |observed_current| {
                    let _ = transaction_send.send(observed_current);
                    let _ = transaction_gate.lock().unwrap().recv();
                }));
            let (drop_send, drop_entered) = std::sync::mpsc::channel();
            let (drop_release, drop_gate) = std::sync::mpsc::channel();
            let drop_release = TailRelease(Some(drop_release));
            let drop_gate = Mutex::new(drop_gate);
            let original = Arc::downgrade(&candidate.shared);
            *candidate.shared.before_unused_input_drop.lock().unwrap() =
                Some(Arc::new(move |input| {
                    let owner = original.upgrade().expect("original shared role");
                    let state_available = owner.state.try_lock().is_ok();
                    let _ = drop_send.send((
                        input.range(),
                        input.retained_backing_capacity(),
                        state_available,
                    ));
                    let _ = drop_gate.lock().unwrap().recv();
                }));
            let input_drop_returned = Arc::new(AtomicBool::new(false));
            let retired = input_drop_returned.clone();
            *candidate.shared.after_unused_input_drop.lock().unwrap() = Some(Arc::new(move || {
                // This marker is emitted only after the real PreparedFileInput
                // Drop statement returned, never from a stop or counter change.
                retired.store(true, Ordering::SeqCst);
            }));
            let (tail_send, tail_entered) = std::sync::mpsc::channel();
            let (tail_release, tail_gate) = std::sync::mpsc::channel();
            let tail_release = TailRelease(Some(tail_release));
            let tail_gate = Mutex::new(tail_gate);
            *candidate.shared.before_watcher_return.lock().unwrap() = Some(Arc::new(move || {
                let _ = tail_send.send(());
                let _ = tail_gate.lock().unwrap().recv();
            }));
            candidate.advance(4).expect("actual planner");
            candidate.advance(4).expect("actual dispatch");
            let observed_current = transaction_entered
                .recv_timeout(Duration::from_secs(10))
                .expect("real result transaction gate");
            if stop {
                control.request_stop();
            } else {
                control.request_reclaim();
            }
            drop(transaction_release);
            let (range, capacity, state_available) = drop_entered
                .recv_timeout(Duration::from_secs(10))
                .expect("stale real input retirement");
            let before_drop = {
                let state = candidate.shared.lock();
                (
                    state.ready_input.is_none(),
                    state.failure.is_none(),
                    matches!(state.phase, Phase::Stopped),
                    matches!(state.phase, Phase::New),
                    state.active_jobs,
                    state.reserved_bytes,
                )
            };
            let source_before = operations.live_operations();
            let drop_had_not_returned = !input_drop_returned.load(Ordering::SeqCst);
            let mut waiter = control.wait_drained();
            let pending_before_drop = futures::poll!(waiter.as_mut()).is_pending();
            drop(drop_release);
            tail_entered
                .recv_timeout(Duration::from_secs(10))
                .expect("actual drop returned before original tail");
            let actual_drop_returned = input_drop_returned.load(Ordering::SeqCst);
            let pending_at_tail = futures::poll!(waiter.as_mut()).is_pending();
            let tail_responsibility = {
                let state = candidate.shared.lock();
                (
                    state.active_jobs,
                    state.reserved_bytes,
                    state.ready_input.is_none(),
                )
            };
            operations.seal();
            drop(tail_release);
            let outcome = tokio::time::timeout(Duration::from_secs(10), waiter)
                .await
                .expect("same original join");
            let source_exit = operations.exited().await;
            assert!(
                observed_current,
                "the earlier observation genuinely preceded stop/reclaim"
            );
            assert_eq!((range, capacity, state_available), (0..4, 4, true));
            assert_eq!(before_drop, (true, true, stop, !stop, 1, 4));
            assert!(drop_had_not_returned && pending_before_drop);
            assert_eq!(source_before, 1, "only the original watcher ticket remains");
            assert!(actual_drop_returned && pending_at_tail);
            assert_eq!(tail_responsibility, (1, 4, true));
            assert!(matches!(outcome, ConnectorPreparationExit::Exited));
            assert_eq!(
                source_exit,
                novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited
            );
            assert!(
                candidate
                    .take_ready()
                    .expect("stopped result has no error")
                    .is_none()
            );
            assert_eq!(control.retained_input_bytes(), 0);
            assert!(control.is_drained());
        }
    }
}
