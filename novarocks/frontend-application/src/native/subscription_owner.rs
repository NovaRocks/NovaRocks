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

//! Original Native subscription handles and their caller-owned responsibility.
//! Storage equals the SAME FE WorkloadConfig scope-record limit, not a new quota.

use super::original_retirement::OriginalRetirement;
use super::task_transport::SubscriptionState;
use crate::task_execution::status_intake::ObservationPublisher;
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_workload_control::joint_admission::{
    JointAdjustmentReceipt, JointAdmission, JointProposalSlotWait, JointTryBegin,
};
use novarocks_workload_control::{WorkError, WorkOwner, WorkScope};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Instant,
};
use tokio::{
    runtime::Handle,
    sync::Notify,
    task::{Id, JoinError, JoinHandle},
};

type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type RawValue = Box<dyn Any + Send>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubscriptionJoinFacts {
    pub(crate) id: Id,
    pub(crate) panic: bool,
    pub(crate) cancelled: bool,
}
impl SubscriptionJoinFacts {
    fn of(error: &JoinError) -> Self {
        Self {
            id: error.id(),
            panic: error.is_panic(),
            cancelled: error.is_cancelled(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationError {
    Closed,
    Storage,
    Generation,
    Work(WorkError),
    SpawnUnjoinable,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FailureFacts {
    pub(crate) original: Option<SubscriptionJoinFacts>,
    pub(crate) reaper: Option<SubscriptionJoinFacts>,
    pub(crate) spawn_unjoinable: bool,
    pub(crate) reaper_exited_early: bool,
    pub(crate) cleanup_failures: u64,
    pub(crate) counter_overflow: bool,
}
impl FailureFacts {
    fn failed(self) -> bool {
        self.original.is_some()
            || self.reaper.is_some()
            || self.spawn_unjoinable
            || self.reaper_exited_early
            || self.cleanup_failures != 0
            || self.counter_overflow
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainObservation {
    Joined,
    Failed(FailureFacts),
    Deadline,
}

/// No arbitrary payload is destroyed by this backing. Its final alias follows
/// original future/cause retirement; otherwise dropping the owner records an orphan.
struct Backing {
    owner: Mutex<Option<WorkOwner>>,
    publisher: Mutex<Option<Arc<ObservationPublisher>>>,
    retired: std::sync::atomic::AtomicBool,
}
impl Drop for Backing {
    fn drop(&mut self) {
        let owner = self
            .owner
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        // An AwaitingChild header has no Work to complete. Its same proposal
        // must actually retire before this original backing can leave.
        // Release the original publisher only after actual F/cause destruction.
        self.publisher
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let Some(owner) = owner else {
            return;
        };
        if self.retired.load(std::sync::atomic::Ordering::Acquire) {
            owner.complete_after_terminal_cancel_settled();
        } else {
            drop(owner);
        }
    }
}
enum PendingChildSource {
    LegacyIndividual,
    Cohort(crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort),
}
struct PendingChild {
    parent: WorkScope,
    source: PendingChildSource,
    proposal: Option<JointAdmission>,
    receipt: Option<JointAdjustmentReceipt>,
    wait: Option<JointProposalSlotWait>,
}
/// The original empty-input retirement F, with its actual same-source layout.
struct DropPendingChild(Option<PendingChild>);
impl Future for DropPendingChild {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
        drop(self.0.take());
        Poll::Ready(())
    }
}
struct OriginalInput {
    stopped: std::sync::atomic::AtomicBool,
    child: Mutex<Option<PendingChild>>,
    future: Mutex<Option<OriginalFuture>>,
    cause: Mutex<Option<RawValue>>,
}
struct OriginalTask {
    input: Arc<OriginalInput>,
}
impl Future for OriginalTask {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        if self
            .input
            .stopped
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Poll::Ready(());
        }
        let mut original = self.input.future.lock().unwrap_or_else(|e| e.into_inner());
        original
            .as_mut()
            .expect("one retained original subscription future")
            .as_mut()
            .poll(cx)
    }
}
enum OriginalState {
    AwaitingChild,
    ChildFailed(RegistrationError),
    Reserved,
    Publishing,
    Polling,
    Running(JoinHandle<()>),
    Retiring(SubscriptionRetirement),
    // The public spawn API did not return a handle. Keep original F and raw
    // spawn cause in this SAME record, with no new spawn or guessed exit.
    SpawnUnjoinable,
}
// Retire F before the raw JoinError, so a destructor panic cannot unwind
// through a second arbitrary raw destructor. Both phases reuse ONE record.
struct SubscriptionRetirement {
    future: Option<OriginalRetirement<Backing>>,
    cause: Option<RawValue>,
    cause_retirement: Option<OriginalRetirement<Backing>>,
    backing: Arc<Backing>,
    cleanup_failures: u64,
}
impl SubscriptionRetirement {
    fn new(input: &OriginalInput, backing: Arc<Backing>) -> Self {
        let future = input
            .future
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("one original joined F");
        let cause = input.cause.lock().unwrap_or_else(|e| e.into_inner()).take();
        Self {
            future: Some(OriginalRetirement::new(future, Arc::clone(&backing))),
            cause,
            cause_retirement: None,
            backing,
            cleanup_failures: 0,
        }
    }
    fn failures_so_far(&self) -> u64 {
        self.cleanup_failures
            .saturating_add(
                self.future
                    .as_ref()
                    .map_or(0, |future| future.cleanup_failures),
            )
            .saturating_add(
                self.cause_retirement
                    .as_ref()
                    .map_or(0, |cause| cause.cleanup_failures),
            )
    }
    fn poll(&mut self, handle: &Handle, cx: &mut std::task::Context<'_>) -> Poll<()> {
        if let Some(future) = self.future.as_mut() {
            if future.poll(handle, cx).is_pending() {
                return Poll::Pending;
            }
            self.cleanup_failures = self
                .cleanup_failures
                .saturating_add(future.cleanup_failures);
            self.future.take();
        }
        if let Some(cause) = self.cause.take() {
            self.cause_retirement = Some(OriginalRetirement::new(cause, Arc::clone(&self.backing)));
        }
        if let Some(cause) = self.cause_retirement.as_mut() {
            if cause.poll(handle, cx).is_pending() {
                return Poll::Pending;
            }
            self.cleanup_failures = self.cleanup_failures.saturating_add(cause.cleanup_failures);
            self.cause_retirement.take();
        }
        Poll::Ready(())
    }
}
struct Record {
    generation: u64,
    failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    input: Arc<OriginalInput>,
    backing: Arc<Backing>,
    stop_requested: bool,
    reservation_live: bool,
    unwound: bool,
    ready_wake: Option<Arc<dyn StatusIntakeWake>>,
    state: OriginalState,
}
struct State {
    started: bool,
    closed: bool,
    next_generation: u64,
    records: Box<[Option<Record>]>,
    failures: FailureFacts,
}
struct Core {
    handle: Handle,
    state: Mutex<State>,
    changed: Notify,
}

/// One process-owned continuous reaper, never one task per observer.
pub(crate) struct NativeSubscriptionOwner {
    core: Arc<Core>,
    reaper: Option<JoinHandle<()>>,
    reaper_retirement: Option<OriginalRetirement<Core>>,
    children_joined: bool,
    joined: bool,
    unjoinable_reaper: bool,
}
#[derive(Clone)]
pub(crate) struct NativeSubscriptionPort {
    core: Arc<Core>,
}
pub(crate) struct SubscriptionLease {
    core: Arc<Core>,
    index: usize,
    generation: u64,
    active: bool,
}
impl SubscriptionLease {
    pub(crate) fn stop(&self) {
        stop_record(&self.core, self.index, self.generation, false);
        self.core.changed.notify_waiters();
    }
    pub(crate) fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.core, &other.core)
            && self.index == other.index
            && self.generation == other.generation
    }
    pub(crate) fn disarm(mut self) {
        self.active = false;
    }
}
impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        if self.active {
            self.stop();
        }
    }
}
// Clone only original metadata under Core. Abort, state projection and their
// possible callbacks occur after Core exits; no detached snapshot is allocated.
fn stop_record(core: &Core, index: usize, generation: u64, reject: bool) {
    let (abort, failure) = {
        let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = state.records[index]
            .as_mut()
            .filter(|r| r.generation == generation)
        else {
            return;
        };
        record.stop_requested = true;
        record
            .input
            .stopped
            .store(true, std::sync::atomic::Ordering::Release);
        let abort = match &record.state {
            OriginalState::Running(join) => Some(join.abort_handle()),
            _ => None,
        };
        (
            abort,
            if reject {
                record.failure_state.clone()
            } else {
                None
            },
        )
    };
    if let Some(failure) = failure {
        *failure.lock().unwrap_or_else(|e| e.into_inner()) = SubscriptionState::Rejected;
    }
    if let Some(abort) = abort {
        abort.abort();
    }
}
fn close_records(core: &Core, reject: bool) {
    let count = {
        let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        state.records.len()
    };
    for index in 0..count {
        let generation = core.state.lock().unwrap_or_else(|e| e.into_inner()).records[index]
            .as_ref()
            .map(|r| r.generation);
        if let Some(generation) = generation {
            stop_record(core, index, generation, reject);
        }
    }
}

impl NativeSubscriptionOwner {
    /// Same original finite container plus actual source allocations and F
    /// pointees. Existing variable request/cause backing is not cloned here.
    #[cfg(test)]
    pub(crate) fn resident_storage_bytes(
        max_records: usize,
    ) -> Result<usize, novarocks_workload_control::WorkError> {
        use novarocks_workload_control::WorkError;
        use std::alloc::Layout;
        let records = Layout::array::<Option<Record>>(max_records)
            .map_err(|_| WorkError::ArithmeticOverflow)?;
        let core =
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Core>())
                .map_err(|_| WorkError::ArithmeticOverflow)?;
        let input = novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
            OriginalInput,
        >())
        .map_err(|_| WorkError::ArithmeticOverflow)?;
        let backing =
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Backing>())
                .map_err(|_| WorkError::ArithmeticOverflow)?;
        let wait = JointProposalSlotWait::metadata_layouts().1;
        // Pending retirement can retain its original wait while the same
        // DropPendingChild F exits. Running and that empty-input F are exclusive.
        let pending = wait
            .size()
            .checked_add(Layout::new::<DropPendingChild>().size())
            .ok_or(WorkError::ArithmeticOverflow)?;
        let future =
            pending.max(super::task_transport::covered_subscription_future_layout().size());
        let sources = input
            .size()
            .checked_add(backing.size())
            .and_then(|n| n.checked_add(future))
            .and_then(|n| n.checked_mul(max_records))
            .ok_or(WorkError::ArithmeticOverflow)?;
        records
            .size()
            .checked_add(sources)
            .and_then(|n| n.checked_add(core.size()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(WorkError::ArithmeticOverflow)
    }

    /// `scope_records_limit` is copied from the SAME configured FE Workload;
    /// production composition must not substitute a default or another owner.
    pub(crate) fn new(
        handle: Handle,
        scope_records_limit: usize,
    ) -> Result<(Self, NativeSubscriptionPort), std::collections::TryReserveError> {
        let mut records = Vec::new();
        records.try_reserve_exact(scope_records_limit)?;
        records.resize_with(scope_records_limit, || None);
        let core = Arc::new(Core {
            handle,
            state: Mutex::new(State {
                started: false,
                closed: false,
                next_generation: 0,
                records: records.into_boxed_slice(),
                failures: FailureFacts::default(),
            }),
            changed: Notify::new(),
        });
        let port = NativeSubscriptionPort {
            core: Arc::clone(&core),
        };
        Ok((
            Self {
                core,
                reaper: None,
                reaper_retirement: None,
                children_joined: false,
                joined: false,
                unjoinable_reaper: false,
            },
            port,
        ))
    }
    /// Host installs this owner BEFORE opening the port. No early task is minted.
    pub(crate) fn start_reaper(&mut self) -> Result<(), RegistrationError> {
        {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.started || state.closed {
                return Err(RegistrationError::Closed);
            }
            state.started = true;
        }
        let core = Arc::clone(&self.core);
        let guard = ReaperExitGuard {
            core: Arc::clone(&self.core),
        };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.core.handle.spawn(async move {
                let _guard = guard;
                reap_originals(core).await;
            })
        })) {
            Ok(original) => {
                self.reaper = Some(original);
                Ok(())
            }
            Err(original) => {
                self.unjoinable_reaper = true;
                self.reaper_retirement =
                    Some(OriginalRetirement::new(original, Arc::clone(&self.core)));
                self.request_abort();
                self.core
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failures
                    .spawn_unjoinable = true;
                Err(RegistrationError::SpawnUnjoinable)
            }
        }
    }
    /// Called only after original logical/query/Abort/Release owners converge.
    pub(crate) fn request_abort(&self) {
        close_records(&self.core, false);
        self.core.changed.notify_waiters();
    }
    pub(crate) fn failure_facts(&self) -> FailureFacts {
        self.core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failures
    }
    pub(crate) fn is_joined(&self) -> bool {
        self.joined
    }

    /// Host borrows this observer alongside its existing execution drain.
    /// It never closes a healthy status stream or starts a replacement task.
    pub(crate) async fn wait_for_failed_reaper(&self) {
        loop {
            let changed = self.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let facts = self.failure_facts();
            if self.unjoinable_reaper || facts.reaper_exited_early || facts.reaper.is_some() {
                return;
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn abort_original_reaper(&self) {
        self.reaper
            .as_ref()
            .expect("started original reaper")
            .abort();
    }
    /// Every timeout borrows the SAME handle/state. Cancellation and retry retain
    /// all original handles; neither a timeout nor F Drop is an exit receipt.
    pub(crate) async fn drain_until(&mut self, deadline: Instant) -> DrainObservation {
        self.request_abort();
        if Instant::now() >= deadline {
            return DrainObservation::Deadline;
        }
        if self.joined {
            return if self.failure_facts().failed() {
                DrainObservation::Failed(self.failure_facts())
            } else {
                DrainObservation::Joined
            };
        }
        if !self
            .core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .started
        {
            self.children_joined = true;
        }
        if !self.children_joined {
            if let Some(original) = self.reaper.as_mut() {
                match tokio::time::timeout_at(deadline.into(), original).await {
                    Err(_) => return DrainObservation::Deadline,
                    Ok(Ok(())) => self.children_joined = true,
                    Ok(Err(actual)) => {
                        self.core
                            .state
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .failures
                            .reaper
                            .get_or_insert(SubscriptionJoinFacts::of(&actual));
                        self.reaper_retirement =
                            Some(OriginalRetirement::new(actual, Arc::clone(&self.core)));
                    }
                }
                self.reaper.take();
            }
            if !self.children_joined {
                // Reaper failure does not detach children: the SAME core/handles
                // are now polled by this original owner, without another task.
                if tokio::time::timeout_at(deadline.into(), reap_originals(Arc::clone(&self.core)))
                    .await
                    .is_err()
                {
                    return DrainObservation::Deadline;
                }
                self.children_joined = true;
            }
        }
        if let Some(retirement) = self.reaper_retirement.as_mut() {
            if tokio::time::timeout_at(
                deadline.into(),
                std::future::poll_fn(|cx| retirement.poll(&self.core.handle, cx)),
            )
            .await
            .is_err()
            {
                return DrainObservation::Deadline;
            }
            let failures = retirement.cleanup_failures;
            add_cleanup_failures(
                &mut self
                    .core
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failures,
                failures,
            );
            self.reaper_retirement.take();
        }
        // A spawn with no returned handle still has no physical exit proof.
        if self.unjoinable_reaper || Instant::now() >= deadline {
            return DrainObservation::Deadline;
        }
        self.joined = true;
        let facts = self.failure_facts();
        if facts.failed() {
            DrainObservation::Failed(facts)
        } else {
            DrainObservation::Joined
        }
    }
}

pub(crate) struct SubscriptionReservation {
    core: Arc<Core>,
    index: usize,
    generation: u64,
    active: bool,
}
impl Drop for NativeSubscriptionOwner {
    fn drop(&mut self) {
        if !self.joined {
            // Illegal owner loss is NOT normal shutdown. Retain the SAME finite
            // inventory, original joins and opaque cleanup causes before any
            // foreign abort/wake can unwind. No replacement reaper is created.
            std::mem::forget(Arc::clone(&self.core));
            if let Some(original) = self.reaper.take() {
                std::mem::forget(original);
            }
            if let Some(original) = self.reaper_retirement.take() {
                std::mem::forget(original);
            }
        }
        self.request_abort();
    }
}

impl NativeSubscriptionPort {
    /// Observe this original process owner's installed records. Readiness is
    /// opened only after the composition starts its original reaper.
    pub(crate) fn installed_records(&self) -> Result<usize, novarocks_workload_control::WorkError> {
        let state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(novarocks_workload_control::WorkError::Closed);
        }
        Ok(state.records.len())
    }

    /// Reserve only this original finite header and parent facts. The one
    /// original service queues/polls the child outside Core and caller locks.
    /// No SDK request, watch, state or future is constructed by this operation.
    pub(crate) fn prepare(
        &self,
        scope: &WorkScope,
        publisher: Option<Arc<ObservationPublisher>>,
        failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    ) -> Result<SubscriptionReservation, RegistrationError> {
        self.prepare_source(
            scope,
            PendingChildSource::LegacyIndividual,
            publisher,
            failure_state,
        )
    }
    pub(crate) fn prepare_from_cohort(
        &self,
        scope: &WorkScope,
        cohort: crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
        publisher: Option<Arc<ObservationPublisher>>,
        failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    ) -> Result<SubscriptionReservation, RegistrationError> {
        self.prepare_source(
            scope,
            PendingChildSource::Cohort(cohort),
            publisher,
            failure_state,
        )
    }
    fn prepare_source(
        &self,
        scope: &WorkScope,
        source: PendingChildSource,
        publisher: Option<Arc<ObservationPublisher>>,
        failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    ) -> Result<SubscriptionReservation, RegistrationError> {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.started || state.closed {
            return Err(RegistrationError::Closed);
        }
        let index = state
            .records
            .iter()
            .position(Option::is_none)
            .ok_or(RegistrationError::Storage)?;
        let Some(generation) = state.next_generation.checked_add(1) else {
            state.failures.counter_overflow = true;
            return Err(RegistrationError::Generation);
        };
        let backing = Arc::new(Backing {
            owner: Mutex::new(None),
            publisher: Mutex::new(publisher),
            retired: std::sync::atomic::AtomicBool::new(false),
        });
        let input = Arc::new(OriginalInput {
            stopped: std::sync::atomic::AtomicBool::new(false),
            child: Mutex::new(Some(PendingChild {
                parent: scope.clone(),
                source,
                proposal: None,
                receipt: None,
                wait: None,
            })),
            future: Mutex::new(None),
            cause: Mutex::new(None),
        });
        state.next_generation = generation;
        state.records[index] = Some(Record {
            generation,
            failure_state,
            input,
            backing,
            stop_requested: false,
            reservation_live: true,
            unwound: false,
            ready_wake: None,
            state: OriginalState::AwaitingChild,
        });
        Ok(SubscriptionReservation {
            core: Arc::clone(&self.core),
            index,
            generation,
            active: true,
        })
    }
    /// Legacy unbound retirement fixtures only; never a production fallback.
    #[cfg(test)]
    pub(crate) fn reserve(
        &self,
        scope: &WorkScope,
        publisher: Option<Arc<ObservationPublisher>>,
        failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    ) -> Result<SubscriptionReservation, RegistrationError> {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.started || state.closed {
            return Err(RegistrationError::Closed);
        }
        let index = state
            .records
            .iter()
            .position(Option::is_none)
            .ok_or(RegistrationError::Storage)?;
        let generation = match state.next_generation.checked_add(1) {
            Some(value) => value,
            None => {
                state.failures.counter_overflow = true;
                return Err(RegistrationError::Generation);
            }
        };
        let owner = scope.child_inherited().map_err(RegistrationError::Work)?;
        let backing = Arc::new(Backing {
            owner: Mutex::new(Some(owner)),
            publisher: Mutex::new(publisher),
            retired: std::sync::atomic::AtomicBool::new(false),
        });
        let input = Arc::new(OriginalInput {
            stopped: std::sync::atomic::AtomicBool::new(false),
            child: Mutex::new(None),
            future: Mutex::new(None),
            cause: Mutex::new(None),
        });
        state.next_generation = generation;
        state.records[index] = Some(Record {
            generation,
            failure_state,
            input,
            backing,
            stop_requested: false,
            reservation_live: true,
            unwound: false,
            ready_wake: None,
            state: OriginalState::Reserved,
        });
        Ok(SubscriptionReservation {
            core: Arc::clone(&self.core),
            index,
            generation,
            active: true,
        })
    }
    /// Call after the caller's active/source lock exits. A wake is not an
    /// admission receipt; the same original reaper checks the installed level.
    pub(crate) fn notify(&self) {
        self.core.changed.notify_waiters();
    }
}
impl SubscriptionReservation {
    /// Register the existing runner wake before checking the same record.
    /// This function performs metadata only; notify the port after unlocking.
    pub(crate) fn ready(
        &self,
        wake: &Arc<dyn StatusIntakeWake>,
    ) -> Result<bool, RegistrationError> {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(RegistrationError::Closed);
        }
        let record = state.records[self.index]
            .as_mut()
            .filter(|r| r.generation == self.generation)
            .ok_or(RegistrationError::Closed)?;
        // The runner is immutable for this original record. Borrowing avoids
        // releasing any foreign wake alias while caller metadata locks exist.
        if record.ready_wake.is_none() {
            record.ready_wake = Some(Arc::clone(wake));
        }
        match &record.state {
            OriginalState::Reserved => Ok(true),
            OriginalState::ChildFailed(e) => Err(e.clone()),
            _ => Ok(false),
        }
    }
    pub(crate) fn stop_view(&self) -> SubscriptionLease {
        SubscriptionLease {
            core: Arc::clone(&self.core),
            index: self.index,
            generation: self.generation,
            active: true,
        }
    }
    /// Attach original receiver fields only AFTER the actual child exists.
    pub(crate) fn attach(
        &self,
        publisher: Arc<ObservationPublisher>,
        failure: Arc<Mutex<SubscriptionState>>,
    ) {
        let (previous, old_failure) = {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            let record = state.records[self.index]
                .as_mut()
                .filter(|r| r.generation == self.generation)
                .expect("same original reservation");
            let previous = record
                .backing
                .publisher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .replace(publisher);
            (previous, record.failure_state.replace(failure))
        };
        drop(previous);
        drop(old_failure);
    }
    /// Commit original F and its join receiving phase before spawn or callback.
    pub(crate) fn start<F>(mut self, future: F) -> Result<SubscriptionLease, RegistrationError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (input, stopped) = {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            let closed = state.closed;
            let record = state.records[self.index]
                .as_mut()
                .filter(|r| r.generation == self.generation)
                .expect("same original reservation");
            assert!(
                matches!(record.state, OriginalState::Reserved),
                "start requires actual child grant"
            );
            *record
                .input
                .future
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(Box::pin(future));
            record.reservation_live = false;
            let stopped = closed || record.stop_requested;
            record.state = if stopped {
                OriginalState::Retiring(SubscriptionRetirement::new(
                    &record.input,
                    Arc::clone(&record.backing),
                ))
            } else {
                OriginalState::Publishing
            };
            self.active = false;
            (Arc::clone(&record.input), stopped)
        };
        if stopped {
            self.core.changed.notify_waiters();
            return Err(RegistrationError::Closed);
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.core.handle.spawn(OriginalTask {
                input: Arc::clone(&input),
            })
        }));
        let mut abort = None;
        let mut rejected = None;
        let failed = match result {
            Ok(join) => {
                let abort_handle = join.abort_handle();
                let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
                let closed = state.closed;
                let record = state.records[self.index]
                    .as_mut()
                    .expect("original publishing record");
                assert_eq!(record.generation, self.generation);
                assert!(matches!(record.state, OriginalState::Publishing));
                if closed || record.stop_requested {
                    abort = Some(abort_handle);
                }
                record.state = OriginalState::Running(join);
                false
            }
            Err(actual) => {
                *input.cause.lock().unwrap_or_else(|e| e.into_inner()) = Some(actual);
                let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
                let record = state.records[self.index]
                    .as_mut()
                    .expect("original publishing record");
                record.state = OriginalState::SpawnUnjoinable;
                rejected = record.failure_state.clone();
                state.failures.spawn_unjoinable = true;
                true
            }
        };
        if let Some(rejected) = rejected {
            *rejected.lock().unwrap_or_else(|e| e.into_inner()) = SubscriptionState::Rejected;
        }
        if let Some(abort) = abort {
            abort.abort();
        }
        self.core.changed.notify_waiters();
        if failed {
            Err(RegistrationError::SpawnUnjoinable)
        } else {
            Ok(SubscriptionLease {
                core: Arc::clone(&self.core),
                index: self.index,
                generation: self.generation,
                active: true,
            })
        }
    }
}

impl Drop for SubscriptionReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let empty = {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            let record = state.records[self.index]
                .as_mut()
                .filter(|r| r.generation == self.generation)
                .expect("same original reservation");
            record.stop_requested = true;
            record.reservation_live = false;
            record
                .input
                .stopped
                .store(true, std::sync::atomic::Ordering::Release);
            // An installed child with no F/cause has no foreign retirement.
            // A pending/failed proposal remains in its original record instead.
            if matches!(record.state, OriginalState::Reserved) && !record.unwound {
                state.records[self.index].take()
            } else {
                None
            }
        };
        if let Some(empty) = empty {
            empty
                .backing
                .retired
                .store(true, std::sync::atomic::Ordering::Release);
            drop(empty);
        }
        self.core.changed.notify_waiters();
    }
}

struct ReaperExitGuard {
    core: Arc<Core>,
}
impl Drop for ReaperExitGuard {
    fn drop(&mut self) {
        let failed = {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            let failed = !state.closed || state.records.iter().any(Option::is_some);
            if failed {
                state.failures.reaper_exited_early = true;
                state.closed = true;
            }
            failed
        };
        if failed {
            close_records(&self.core, true);
        }
        self.core.changed.notify_waiters();
    }
}
fn add_cleanup_failures(facts: &mut FailureFacts, delta: u64) {
    match facts.cleanup_failures.checked_add(delta) {
        Some(value) => facts.cleanup_failures = value,
        None => facts.counter_overflow = true,
    }
}
/// Temporarily lend only the SAME record's phase. Its fixed index remains
/// occupied throughout poll, callbacks and unwind; another reserve cannot reuse it.
struct RecordLease {
    core: Arc<Core>,
    index: usize,
    generation: u64,
    phase: Option<OriginalState>,
    input: Arc<OriginalInput>,
    backing: Arc<Backing>,
    stopping: bool,
}
impl RecordLease {
    fn take(core: &Arc<Core>, index: usize) -> Option<Self> {
        let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
        let closed = state.closed;
        let record = state.records[index].as_mut()?;
        if record.unwound
            || (record.reservation_live && (closed || record.stop_requested))
            || matches!(
                record.state,
                OriginalState::Polling | OriginalState::Publishing | OriginalState::SpawnUnjoinable
            )
            || (matches!(
                record.state,
                OriginalState::Reserved | OriginalState::ChildFailed(_)
            ) && !closed
                && !record.stop_requested)
        {
            return None;
        }
        Some(Self {
            core: Arc::clone(core),
            index,
            generation: record.generation,
            phase: Some(std::mem::replace(&mut record.state, OriginalState::Polling)),
            input: Arc::clone(&record.input),
            backing: Arc::clone(&record.backing),
            stopping: closed || record.stop_requested,
        })
    }
    fn finish(mut self) {
        let record = {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            let record = state.records[self.index]
                .take()
                .expect("same retired original record");
            assert_eq!(record.generation, self.generation);
            record
        };
        self.backing
            .retired
            .store(true, std::sync::atomic::Ordering::Release);
        // All actual phase/record/owner destruction is outside Core.
        drop(self.phase.take());
        drop(record);
    }
}
impl Drop for RecordLease {
    fn drop(&mut self) {
        if let Some(phase) = self.phase.take() {
            let previous = {
                let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
                let record = state.records[self.index]
                    .as_mut()
                    .expect("same occupied poll record");
                assert_eq!(record.generation, self.generation);
                // Preserve this exact phase/F/proposal on unexpected unwind;
                // no later host recovery repolls a source with unknown state.
                record.unwound |= std::thread::panicking();
                std::mem::replace(&mut record.state, phase)
            };
            assert!(matches!(previous, OriginalState::Polling));
        }
    }
}
struct ChildLease {
    input: Arc<OriginalInput>,
    child: Option<PendingChild>,
}
impl Drop for ChildLease {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            let previous = self
                .input
                .child
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .replace(child);
            assert!(previous.is_none(), "same child proposal returns once");
        }
    }
}
fn retire_empty_input(lease: &mut RecordLease) {
    let child = lease
        .input
        .child
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    *lease.input.future.lock().unwrap_or_else(|e| e.into_inner()) =
        Some(Box::pin(DropPendingChild(child)));
    lease.phase = Some(OriginalState::Retiring(SubscriptionRetirement::new(
        &lease.input,
        Arc::clone(&lease.backing),
    )));
}
fn poll_record(core: &Arc<Core>, index: usize, cx: &mut std::task::Context<'_>) -> bool {
    let Some(mut lease) = RecordLease::take(core, index) else {
        return false;
    };
    let mut notify = None;
    let mut wake = None;
    let mut progress = false;
    let mut done = false;
    match lease.phase.as_mut().unwrap() {
        OriginalState::AwaitingChild => {
            if lease.stopping {
                retire_empty_input(&mut lease);
                progress = true;
            } else {
                let child = lease
                    .input
                    .child
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .expect("same pending child proposal");
                let mut child = ChildLease {
                    input: Arc::clone(&lease.input),
                    child: Some(child),
                };
                let pending = child.child.as_mut().unwrap();
                let mut waiting = false;
                if let PendingChildSource::Cohort(cohort) = &pending.source {
                    // RecordLease already lent the exact record, so neither
                    // Native Core nor the subscriber's caller lock is held.
                    let result = {
                        let mut destination = lease
                            .backing
                            .owner
                            .lock()
                            .unwrap_or_else(|e| e.into_inner());
                        cohort.take_child_in(&pending.parent, &mut destination)
                    };
                    match result {
                        Ok(notification) => {
                            notify = Some(notification);
                            lease.phase = Some(OriginalState::Reserved);
                            drop(child.child.take());
                        }
                        Err(error) => {
                            lease.phase =
                                Some(OriginalState::ChildFailed(RegistrationError::Work(error)))
                        }
                    }
                    progress = true;
                } else {
                    if let Some(wait) = pending.wait.as_mut() {
                        match Pin::new(wait).poll(cx) {
                            Poll::Pending => waiting = true,
                            Poll::Ready(Ok(())) => {
                                drop(pending.wait.take());
                            }
                            Poll::Ready(Err(error)) => {
                                lease.phase = Some(OriginalState::ChildFailed(
                                    RegistrationError::Work(error),
                                ));
                                progress = true;
                            }
                        }
                    }
                    if !waiting
                        && matches!(lease.phase, Some(OriginalState::AwaitingChild))
                        && pending.proposal.is_none()
                        && pending.receipt.is_none()
                    {
                        match pending.parent.try_request_joint_child() {
                            Ok(JointTryBegin::Started(proposal)) => {
                                pending.proposal = Some(proposal)
                            }
                            Ok(JointTryBegin::Full(signal)) => {
                                pending.wait = Some(signal.into_wait());
                                match Pin::new(pending.wait.as_mut().unwrap()).poll(cx) {
                                    Poll::Pending => {}
                                    Poll::Ready(Ok(())) => {
                                        drop(pending.wait.take());
                                        progress = true;
                                    }
                                    Poll::Ready(Err(error)) => {
                                        lease.phase = Some(OriginalState::ChildFailed(
                                            RegistrationError::Work(error),
                                        ));
                                        progress = true;
                                    }
                                }
                            }
                            Err(error) => {
                                lease.phase = Some(OriginalState::ChildFailed(
                                    RegistrationError::Work(error),
                                ));
                                progress = true;
                            }
                        }
                    }
                    if matches!(lease.phase, Some(OriginalState::AwaitingChild)) {
                        let granted = match pending.proposal.as_mut() {
                            Some(proposal) => match Pin::new(proposal).poll(cx) {
                                Poll::Pending => None,
                                Poll::Ready(result) => {
                                    pending.proposal.take();
                                    Some(result)
                                }
                            },
                            None => None,
                        };
                        if let Some(result) = granted {
                            match result {
                                Err(error) => {
                                    lease.phase = Some(OriginalState::ChildFailed(
                                        RegistrationError::Work(error.error),
                                    ));
                                }
                                Ok(mut batch) => {
                                    assert!(
                                        batch.new_claims.is_empty(),
                                        "child cannot mint physical claims"
                                    );
                                    pending.receipt = Some(
                                        batch.adjustment.take().expect("original child reply"),
                                    );
                                    let result = {
                                        let mut destination = lease
                                            .backing
                                            .owner
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner());
                                        pending
                                            .receipt
                                            .as_mut()
                                            .unwrap()
                                            .install_child_in(&pending.parent, &mut destination)
                                    };
                                    match result {
                                        Ok(notification) => {
                                            notify = Some(notification);
                                            lease.phase = Some(OriginalState::Reserved);
                                            drop(child.child.take());
                                        }
                                        Err(error) => {
                                            lease.phase = Some(OriginalState::ChildFailed(
                                                RegistrationError::Work(error),
                                            ))
                                        }
                                    }
                                }
                            }
                            progress = true;
                        }
                    }
                }
                drop(child); // Same pending source is stable before notifications.
                if progress
                    && matches!(
                        lease.phase,
                        Some(OriginalState::Reserved | OriginalState::ChildFailed(_))
                    )
                {
                    wake = core.state.lock().unwrap_or_else(|e| e.into_inner()).records[index]
                        .as_ref()
                        .and_then(|r| r.ready_wake.clone());
                }
            }
        }
        OriginalState::Reserved | OriginalState::ChildFailed(_) => {
            retire_empty_input(&mut lease);
            progress = true;
        }
        OriginalState::Running(original) => {
            if lease.stopping {
                original.abort();
            }
            if let Poll::Ready(joined) = Pin::new(original).poll(cx) {
                if let Err(original) = joined {
                    let facts = SubscriptionJoinFacts::of(&original);
                    let unexpected = facts.panic || !lease.stopping || !facts.cancelled;
                    *lease.input.cause.lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(Box::new(original));
                    if unexpected {
                        let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
                        state.failures.original.get_or_insert(facts);
                        let failure = state.records[index]
                            .as_ref()
                            .and_then(|r| r.failure_state.clone());
                        drop(state);
                        if let Some(failure) = failure {
                            *failure.lock().unwrap_or_else(|e| e.into_inner()) =
                                SubscriptionState::Rejected;
                        }
                    }
                }
                lease.phase = Some(OriginalState::Retiring(SubscriptionRetirement::new(
                    &lease.input,
                    Arc::clone(&lease.backing),
                )));
                progress = true;
            }
        }
        OriginalState::Retiring(retirement) => {
            let before = retirement.failures_so_far();
            done = retirement.poll(&core.handle, cx).is_ready();
            let failures = retirement.failures_so_far().saturating_sub(before);
            if failures != 0 {
                let state_view = {
                    let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
                    add_cleanup_failures(&mut state.failures, failures);
                    state.records[index]
                        .as_ref()
                        .and_then(|r| r.failure_state.clone())
                };
                if let Some(state_view) = state_view {
                    *state_view.lock().unwrap_or_else(|e| e.into_inner()) =
                        SubscriptionState::Rejected;
                }
            }
        }
        OriginalState::Polling | OriginalState::Publishing | OriginalState::SpawnUnjoinable => {
            unreachable!()
        }
    }
    if done {
        lease.finish();
    } else {
        drop(lease);
    }
    // No Core/active/source mutex is held at any notification boundary.
    if let Some(notification) = notify {
        notification.notify();
    }
    if let Some(wake) = wake {
        wake.wake();
    }
    if progress || done {
        cx.waker().wake_by_ref();
    }
    progress || done
}
async fn reap_originals(core: Arc<Core>) {
    loop {
        let notified = core.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let settled = std::future::poll_fn(|cx| {
            let count = core
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .records
                .len();
            for index in 0..count {
                poll_record(&core, index, cx);
            }
            let state = core.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.closed && state.records.iter().all(Option::is_none) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        tokio::pin!(settled);
        tokio::select! { _ = &mut settled => return, _ = &mut notified => {} }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
#[path = "subscription_owner_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "subscription_owner/bound_child_tests.rs"]
mod bound_child_tests;

#[cfg(test)]
#[path = "subscription_owner/cohort_tests.rs"]
mod cohort_tests;
