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

//! Process-wide completion ownership for native tasks.
//!
//! Pipeline workers publish only an immutable stopped fact. One fixed async
//! owner consumes those facts and performs the task's ordered completion work,
//! so a running task does not retain a dedicated supervising OS thread.

use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use novarocks_execution::runtime::fragment::FragmentTerminalFact;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc};

const COMPLETION_STATE_LOCK: &str = "task completion supervisor state lock";
const COMPLETION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Refusal at the exact-slot admission owner, before any task can start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionReservationError {
    ShuttingDown,
    Unavailable(novarocks_execution_contract::failure::BoundedDetail),
    Duplicate(TaskIdentity),
    StatusIdentity {
        reserved: TaskIdentity,
        reporter: TaskIdentity,
    },
    SlotCapacity {
        committed: usize,
        limit: usize,
    },
}
impl fmt::Display for CompletionReservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShuttingDown => f.write_str("task completion owner is shutting down"),
            Self::Unavailable(detail) => {
                write!(f, "task completion owner is unavailable: {detail}")
            }
            Self::Duplicate(identity) => write!(f, "task {identity} already has a completion slot"),
            Self::StatusIdentity { reserved, reporter } => write!(
                f,
                "completion slot for {reserved} cannot publish status for {reporter}"
            ),
            Self::SlotCapacity { limit, .. } => write!(
                f,
                "task completion owner reached its {limit} exact-slot bound"
            ),
        }
    }
}
impl std::error::Error for CompletionReservationError {}
impl novarocks_execution_contract::failure::FailureProjection for CompletionReservationError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        match self {
            Self::ShuttingDown => FailureFact::stop_observation(R::CompletionShuttingDown.reason()),
            Self::Unavailable(_) => {
                FailureFact::new(FailureClass::Unexpected, R::CompletionUnavailable.reason())
            }
            Self::Duplicate(_) => {
                FailureFact::new(FailureClass::Invariant, R::CompletionDuplicate.reason())
            }
            Self::StatusIdentity { .. } => FailureFact::new(
                FailureClass::Invariant,
                R::CompletionStatusIdentity.reason(),
            ),
            Self::SlotCapacity { committed, limit } => {
                FailureFact::new(FailureClass::Resource, R::CompletionSlotCapacity.reason())
                    .with_resource(ResourceFacts {
                        request_scope: Some(ResourceScope::Task),
                        constraint_scope: Some(ResourceScope::Process),
                        dimension: ResourceDimension::Positions,
                        requested: Some(1),
                        required_growth: Some(1),
                        limit: u64::try_from(*limit).ok(),
                        live: None,
                        committed: u64::try_from(*committed).ok(),
                    })
            }
        }
    }
}

/// Called at most once, borrowing the slot's fact and its captured owners.
/// The slot retains the action until its separately supervised destruction.
pub type TaskCompletionAction = Box<dyn FnMut(&FragmentTerminalFact) + Send + 'static>;
type PanicPayload = Box<dyn std::any::Any + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletionPhase {
    Waiting,
    Queued,
    Running,
    Unresolved,
}

struct CompletionWork {
    action: Option<TaskCompletionAction>,
    fact: Option<FragmentTerminalFact>,
    reporter: Option<crate::TaskStatusReporter>,
    // The original independently admitted sender outlives this work's own
    // reporter capture. It is never obtained from a callback-time factory.
    exit: Option<crate::task_lifecycle::CompletionExitNotice>,
    // At most one secondary payload per foreign invocation/destruction phase.
    // These are work custody, never accepted Task causes or wire diagnostics.
    unresolved_payloads: [Option<PanicPayload>; 5],
}

impl CompletionWork {
    fn dispose_panic(&mut self, payload: PanicPayload) {
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            let entry = self
                .unresolved_payloads
                .iter_mut()
                .find(|entry| entry.is_none())
                .expect("one unresolved payload per completion phase");
            *entry = Some(secondary);
        }
    }

    fn run(&mut self, withdrawn: bool) -> bool {
        let mut failed = false;
        if !withdrawn {
            let Self { action, fact, .. } = self;
            let result = catch_unwind(AssertUnwindSafe(|| {
                action.as_mut().expect("one completion action")(
                    fact.as_ref().expect("a committed completion has its fact"),
                );
            }));
            if let Err(payload) = result {
                failed = true;
                self.dispose_panic(payload);
            }
        }
        // Each holder is taken exactly once. A failed destructor is never retried.
        if let Some(action) = self.action.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(action)))
        {
            failed = true;
            self.dispose_panic(payload);
        }
        if let Some(fact) = self.fact.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(fact)))
        {
            failed = true;
            self.dispose_panic(payload);
        }
        if let Some(reporter) = self.reporter.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(reporter)))
        {
            failed = true;
            self.dispose_panic(payload);
        }
        if let Some(exit) = self.exit.take() {
            // No action, fact or reporter holder from this original work can
            // remain on the success path. A failed phase publishes no stopped
            // fact; the prepaid sender retains its unknown responsibility.
            let result = catch_unwind(AssertUnwindSafe(|| {
                if failed {
                    exit.unresolved();
                } else if withdrawn {
                    exit.withdrawn();
                } else {
                    exit.completed();
                }
            }));
            if let Err(payload) = result {
                failed = true;
                self.dispose_panic(payload);
            }
        }
        !failed
    }
}

struct CompletionSlot {
    ticket: Arc<()>,
    expected_fragment: novarocks_types::UniqueId,
    work: Arc<Mutex<CompletionWork>>,
    creation_committed: bool,
    fact_received: bool,
    withdrawn: bool,
    phase: CompletionPhase,
    dispatch_panic: Option<std::mem::ManuallyDrop<PanicPayload>>,
}

struct CompletionState {
    accepting: bool,
    slots: HashMap<TaskIdentity, CompletionSlot>,
    failure: Option<String>,
    // Explicit bounded custody if the runtime dies before it can drain. This
    // process owner releases its pin only after every accepted holder exits.
    // Unresolved slots retain the same capacity; timeout never drops them.
    custody: Option<Arc<CompletionShared>>,
    owner_panic: Option<std::mem::ManuallyDrop<PanicPayload>>,
}

struct CompletionShared {
    state: Mutex<CompletionState>,
    ready: mpsc::Sender<TaskIdentity>,
    changed: Notify,
}

impl CompletionShared {
    fn fail_locked(state: &mut CompletionState, detail: impl Into<String>) {
        if state.failure.is_none() {
            state.failure = Some(detail.into());
        }
    }

    fn publish(&self, identity: TaskIdentity, ticket: &Arc<()>, fact: FragmentTerminalFact) {
        let enqueue = {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            let Some(slot) = state.slots.get_mut(&identity) else {
                return;
            };
            if !Arc::ptr_eq(&slot.ticket, ticket) {
                return;
            }
            if fact.query_id() != identity.query_execution_id().query_id()
                || fact.fragment_instance_id() != slot.expected_fragment
            {
                Self::fail_locked(
                    &mut state,
                    format!("task {identity} published a stopped fact for another fragment"),
                );
                self.changed.notify_waiters();
                return;
            }
            if slot.fact_received || slot.withdrawn {
                return;
            }
            // Only Waiting can install a first fact. No job can hold this work
            // lock yet, and duplicate/reentrant publication never takes it.
            assert_eq!(slot.phase, CompletionPhase::Waiting);
            slot.work.lock().expect(COMPLETION_STATE_LOCK).fact = Some(fact);
            slot.fact_received = true;
            if slot.creation_committed {
                slot.phase = CompletionPhase::Queued;
                true
            } else {
                false
            }
        };
        if enqueue {
            self.enqueue(identity);
        }
    }

    fn commit_creation(&self, identity: TaskIdentity, ticket: &Arc<()>) {
        let enqueue = {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            let Some(slot) = state.slots.get_mut(&identity) else {
                Self::fail_locked(
                    &mut state,
                    format!("task {identity} committed without a completion slot"),
                );
                self.changed.notify_waiters();
                return;
            };
            if !Arc::ptr_eq(&slot.ticket, ticket) {
                return;
            }
            if slot.creation_committed {
                return;
            }
            assert!(!slot.withdrawn, "a withdrawn creation cannot commit");
            slot.creation_committed = true;
            if slot.fact_received {
                slot.phase = CompletionPhase::Queued;
                true
            } else {
                false
            }
        };
        if enqueue {
            self.enqueue(identity);
        }
    }

    fn withdraw_creation(&self, identity: TaskIdentity, ticket: &Arc<()>) {
        let enqueue = {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            let Some(slot) = state.slots.get_mut(&identity) else {
                return;
            };
            if !Arc::ptr_eq(&slot.ticket, ticket) {
                return;
            }
            if slot.creation_committed || slot.withdrawn {
                return;
            }
            slot.withdrawn = true;
            slot.phase = CompletionPhase::Queued;
            true
        };
        if enqueue {
            self.enqueue(identity);
        }
    }

    fn enqueue(&self, identity: TaskIdentity) {
        if let Err(error) = self.ready.try_send(identity) {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            Self::fail_locked(
                &mut state,
                format!("task {identity} completion ready queue rejected its exact slot: {error}"),
            );
            if let Some(slot) = state.slots.get_mut(&identity) {
                slot.phase = CompletionPhase::Unresolved;
            }
            self.changed.notify_waiters();
        }
    }

    fn start_ready(&self, identity: TaskIdentity) -> Option<(Arc<Mutex<CompletionWork>>, bool)> {
        let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
        let slot = state.slots.get_mut(&identity)?;
        if slot.phase != CompletionPhase::Queued {
            return None;
        }
        slot.phase = CompletionPhase::Running;
        Some((Arc::clone(&slot.work), slot.withdrawn))
    }

    fn finish(&self, identity: TaskIdentity, exited: bool) {
        let (removed, custody) = {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            if exited {
                let removed = state.slots.remove(&identity);
                let custody = state
                    .slots
                    .is_empty()
                    .then(|| state.custody.take())
                    .flatten();
                (removed, custody)
            } else {
                if let Some(slot) = state.slots.get_mut(&identity) {
                    slot.phase = CompletionPhase::Unresolved;
                }
                Self::fail_locked(
                    &mut state,
                    format!("task {identity} completion work did not exit successfully"),
                );
                (None, None)
            }
        };
        // Exited work has no remaining foreign holders. Even metadata removal
        // and custody release occur outside the supervisor state lock.
        drop(removed);
        drop(custody);
        self.changed.notify_waiters();
    }

    fn owner_exited(&self) {
        let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
        if state.accepting
            || state
                .slots
                .values()
                .any(|slot| slot.phase != CompletionPhase::Unresolved)
        {
            Self::fail_locked(
                &mut state,
                "task completion runtime exited before its accepted work drained",
            );
        }
        state.accepting = false;
        self.changed.notify_waiters();
    }
}

/// A publisher carries identity and no ownership of completion work.
#[derive(Clone)]
pub struct TaskCompletionSignal {
    ticket: Arc<()>,
    identity: TaskIdentity,
    shared: std::sync::Weak<CompletionShared>,
}
impl TaskCompletionSignal {
    pub fn publish(&self, fact: FragmentTerminalFact) {
        if let Some(shared) = self.shared.upgrade() {
            shared.publish(self.identity, &self.ticket, fact);
        }
    }
}

/// Creation owns this reservation until commit or explicit rollback disposal.
/// It cannot be cloned; dropping an uncommitted creation invokes no action and
/// fabricates no stopped fact.
pub struct TaskCompletionReservation {
    ticket: Arc<()>,
    identity: TaskIdentity,
    shared: Arc<CompletionShared>,
    committed: std::sync::atomic::AtomicBool,
}
impl TaskCompletionReservation {
    /// The task first reserves its exact original slot, then attaches the
    /// action's captures before creation can commit or a terminal fact arrive.
    /// Rejection returns the original callback without running its destructor.
    pub fn bind_action(
        &mut self,
        action: TaskCompletionAction,
    ) -> Result<(), (CompletionReservationError, TaskCompletionAction)> {
        let mut state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
        let Some(slot) = state.slots.get_mut(&self.identity) else {
            return Err((
                CompletionReservationError::Unavailable(
                    novarocks_execution_contract::failure::BoundedDetail::truncating(
                        "original completion slot retired before action binding",
                    ),
                ),
                action,
            ));
        };
        if slot.withdrawn {
            return Err((CompletionReservationError::ShuttingDown, action));
        }
        if !Arc::ptr_eq(&slot.ticket, &self.ticket)
            || slot.creation_committed
            || slot.fact_received
            || slot.phase != CompletionPhase::Waiting
        {
            return Err((CompletionReservationError::Duplicate(self.identity), action));
        }
        let mut work = slot.work.lock().expect(COMPLETION_STATE_LOCK);
        if work.action.is_some() {
            return Err((CompletionReservationError::Duplicate(self.identity), action));
        }
        if let Some(exit) = work.exit.as_mut() {
            exit.bind();
        }
        work.action = Some(action);
        Ok(())
    }
    pub fn signal(&self) -> TaskCompletionSignal {
        TaskCompletionSignal {
            ticket: Arc::clone(&self.ticket),
            identity: self.identity,
            shared: Arc::downgrade(&self.shared),
        }
    }
    pub fn publish(&self, fact: FragmentTerminalFact) {
        self.signal().publish(fact);
    }
    pub fn commit_creation(&self) -> TaskCompletionSignal {
        self.shared.commit_creation(self.identity, &self.ticket);
        self.committed
            .store(true, std::sync::atomic::Ordering::Release);
        self.signal()
    }
}
impl Drop for TaskCompletionReservation {
    fn drop(&mut self) {
        if !self.committed.load(std::sync::atomic::Ordering::Acquire) {
            self.shared.withdraw_creation(self.identity, &self.ticket);
        }
    }
}

struct CompletionOwnerExit(Arc<CompletionShared>);
impl Drop for CompletionOwnerExit {
    fn drop(&mut self) {
        self.0.owner_exited();
    }
}

/// The exact failed slot admission returns these original artifacts to its
/// caller. No action has attached, and no unused authority is replaced.
pub struct TaskCompletionSlotRefusal {
    error: CompletionReservationError,
    reporter: Option<crate::TaskStatusReporter>,
    exit: Option<crate::task_lifecycle::CompletionExitNotice>,
    standalone_action: Option<TaskCompletionAction>,
}
impl std::fmt::Debug for TaskCompletionSlotRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskCompletionSlotRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl TaskCompletionSlotRefusal {
    pub fn into_task_parts(
        self,
    ) -> (
        CompletionReservationError,
        crate::TaskStatusReporter,
        crate::task_lifecycle::CompletionExitNotice,
    ) {
        (
            self.error,
            self.reporter.expect("Task slot received its reporter"),
            self.exit
                .expect("Task slot received its original exit notice"),
        )
    }
}

/// One async owner, with at most one existing-runtime blocking job per exact
/// slot. Running, destruction and unresolved work all occupy that same bound.
pub struct TaskCompletionSupervisor {
    runtime: Handle,
    capacity: usize,
    shared: Arc<CompletionShared>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl fmt::Debug for TaskCompletionSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
        formatter
            .debug_struct("TaskCompletionSupervisor")
            .field("capacity", &self.capacity)
            .field("slots", &state.slots.len())
            .field("failed", &state.failure.is_some())
            .finish()
    }
}
impl TaskCompletionSupervisor {
    pub fn start(runtime: Handle, capacity: usize) -> Arc<Self> {
        assert!(capacity > 0, "task completion capacity must be positive");
        let (ready, mut receiver) = mpsc::channel(capacity);
        let shared = Arc::new(CompletionShared {
            state: Mutex::new(CompletionState {
                accepting: true,
                slots: HashMap::new(),
                failure: None,
                custody: None,
                owner_panic: None,
            }),
            ready,
            changed: Notify::new(),
        });
        let owner_shared = Arc::clone(&shared);
        let work_runtime = runtime.clone();
        let join = runtime.spawn(async move {
            let _exit = CompletionOwnerExit(Arc::clone(&owner_shared));
            let mut jobs = FuturesUnordered::<
                futures::future::BoxFuture<'static, (TaskIdentity, Result<bool, tokio::task::JoinError>)>,
            >::new();
            loop {
                let changed = owner_shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = owner_shared.state.lock().expect(COMPLETION_STATE_LOCK);
                    if !state.accepting && state.slots.values().all(|slot| slot.phase == CompletionPhase::Unresolved) {
                        return;
                    }
                }
                tokio::select! {
                    Some((identity, result)) = jobs.next(), if !jobs.is_empty() => {
                        match result {
                            Ok(exited) => owner_shared.finish(identity, exited),
                            Err(error) => {
                                if error.is_panic() {
                                    let payload = std::mem::ManuallyDrop::new(error.into_panic());
                                    let mut state = owner_shared.state.lock().expect(COMPLETION_STATE_LOCK);
                                    state.slots.get_mut(&identity).expect("a failed join retains its exact slot").dispatch_panic = Some(payload);
                                }
                                owner_shared.finish(identity, false);
                            }
                        }
                    }
                    ready = receiver.recv() => {
                        let Some(identity) = ready else { return; };
                        let Some((work, withdrawn)) = owner_shared.start_ready(identity) else { continue; };
                        // The job never owns the sole foreign holder. A runtime
                        // refusal/cancellation leaves it in the charged slot.
                        let spawned = catch_unwind(AssertUnwindSafe(|| {
                            work_runtime.spawn_blocking(move || work.lock().expect(COMPLETION_STATE_LOCK).run(withdrawn))
                        }));
                        match spawned {
                            Ok(job) => jobs.push(async move { (identity, job.await) }.boxed()),
                            Err(payload) => {
                                // Spawn can panic after enqueueing the job. No
                                // JoinHandle proves its exit; retain the opaque
                                // dispatch payload in this exact unresolved slot.
                                let payload = std::mem::ManuallyDrop::new(payload);
                                let mut state = owner_shared.state.lock().expect(COMPLETION_STATE_LOCK);
                                let slot = state.slots.get_mut(&identity).expect("a dispatch retains its exact slot");
                                slot.dispatch_panic = Some(payload);
                                slot.phase = CompletionPhase::Unresolved;
                                CompletionShared::fail_locked(&mut state, format!("task {identity} completion dispatch panicked"));
                                owner_shared.changed.notify_waiters();
                            }
                        }
                    }
                    _ = &mut changed => {}
                }
            }
        });
        Arc::new(Self {
            runtime,
            capacity,
            shared,
            join: Mutex::new(Some(join)),
        })
    }

    /// A bounded standalone completion with no Task lifecycle publication.
    /// Native Task execution uses reserve_task_slot with its original notice.
    pub fn reserve(
        &self,
        identity: TaskIdentity,
        expected_fragment: novarocks_types::UniqueId,
        action: TaskCompletionAction,
    ) -> Result<TaskCompletionReservation, CompletionReservationError> {
        match self.reserve_empty(identity, expected_fragment, None, None, Some(action)) {
            Ok(reservation) => Ok(reservation),
            Err(refusal) => {
                // The rejected caller still owns this original action. The
                // admission lock has already exited before its real Drop.
                drop(refusal.standalone_action);
                Err(refusal.error)
            }
        }
    }

    /// Reserve before constructing the terminal action. A normal refusal
    /// returns both original metadata holders and the unused exit authority;
    /// no foreign action captures exist yet and no callback is silently lost.
    #[expect(
        clippy::result_large_err,
        reason = "Refusal returns the exact original finite holders without allocating another unadmitted custody carrier."
    )]
    pub fn reserve_task_slot(
        &self,
        identity: TaskIdentity,
        expected_fragment: novarocks_types::UniqueId,
        reporter: crate::TaskStatusReporter,
        exit: crate::task_lifecycle::CompletionExitNotice,
    ) -> Result<TaskCompletionReservation, TaskCompletionSlotRefusal> {
        self.reserve_empty(
            identity,
            expected_fragment,
            Some(reporter),
            Some(exit),
            None,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Refusal returns the exact original finite holders without allocating another unadmitted custody carrier."
    )]
    fn reserve_empty(
        &self,
        identity: TaskIdentity,
        expected_fragment: novarocks_types::UniqueId,
        reporter: Option<crate::TaskStatusReporter>,
        exit: Option<crate::task_lifecycle::CompletionExitNotice>,
        standalone_action: Option<TaskCompletionAction>,
    ) -> Result<TaskCompletionReservation, TaskCompletionSlotRefusal> {
        macro_rules! rejected {
            ($error:expr) => {
                TaskCompletionSlotRefusal {
                    error: $error,
                    reporter,
                    exit,
                    standalone_action,
                }
            };
        }
        if let Some(reporter) = reporter.as_ref()
            && reporter.identity() != identity
        {
            let actual = reporter.identity();
            return Err(rejected!(CompletionReservationError::StatusIdentity {
                reserved: identity,
                reporter: actual
            }));
        }
        if let Some(exit) = exit.as_ref()
            && exit.identity() != identity
        {
            let actual = exit.identity();
            return Err(rejected!(CompletionReservationError::StatusIdentity {
                reserved: identity,
                reporter: actual
            }));
        }
        let mut state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
        if state.accepting && self.shared.ready.is_closed() {
            CompletionShared::fail_locked(
                &mut state,
                "task completion runtime is no longer available",
            );
            state.accepting = false;
        }
        if let Some(detail) = &state.failure {
            return Err(rejected!(CompletionReservationError::Unavailable(
                novarocks_execution_contract::failure::BoundedDetail::truncating(detail),
            )));
        }
        if !state.accepting {
            return Err(rejected!(CompletionReservationError::ShuttingDown));
        }
        if state.slots.contains_key(&identity) {
            return Err(rejected!(CompletionReservationError::Duplicate(identity)));
        }
        if state.slots.len() >= self.capacity {
            return Err(rejected!(CompletionReservationError::SlotCapacity {
                committed: state.slots.len(),
                limit: self.capacity
            }));
        }
        let ticket = Arc::new(());
        state.slots.insert(
            identity,
            CompletionSlot {
                ticket: Arc::clone(&ticket),
                expected_fragment,
                work: Arc::new(Mutex::new(CompletionWork {
                    action: standalone_action,
                    fact: None,
                    reporter,
                    exit,
                    unresolved_payloads: std::array::from_fn(|_| None),
                })),
                creation_committed: false,
                fact_received: false,
                withdrawn: false,
                phase: CompletionPhase::Waiting,
                dispatch_panic: None,
            },
        );
        if state.custody.is_none() {
            state.custody = Some(Arc::clone(&self.shared));
        }
        Ok(TaskCompletionReservation {
            ticket,
            identity,
            shared: Arc::clone(&self.shared),
            committed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn poll_failure(&self) -> Option<String> {
        self.shared
            .state
            .lock()
            .expect(COMPLETION_STATE_LOCK)
            .failure
            .clone()
    }

    pub fn shutdown(&self) -> Result<(), String> {
        if Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread
        }) {
            return Err(
                "task completion shutdown must be awaited inside a current-thread runtime"
                    .to_string(),
            );
        }
        self.block_on(self.shutdown_async())
    }

    pub async fn shutdown_async(&self) -> Result<(), String> {
        self.shared
            .state
            .lock()
            .expect(COMPLETION_STATE_LOCK)
            .accepting = false;
        self.shared.changed.notify_waiters();
        let shared = Arc::clone(&self.shared);
        let result = async move {
            tokio::time::timeout(COMPLETION_SHUTDOWN_TIMEOUT, async {
                loop {
                    let changed = shared.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    {
                        let state = shared.state.lock().expect(COMPLETION_STATE_LOCK);
                        // An earlier failure never skips an accepted sibling.
                        if state
                            .slots
                            .values()
                            .all(|slot| slot.phase == CompletionPhase::Unresolved)
                        {
                            return state.failure.clone().map_or(Ok(()), Err);
                        }
                    }
                    changed.await;
                }
            })
            .await
            .map_err(|_| {
                "task completion owner did not drain its exact slots within 5000 ms".to_string()
            })?
        }
        .await;
        // A timeout is an observation, not permission to cancel accepted work.
        if result.is_ok()
            || self
                .shared
                .state
                .lock()
                .expect(COMPLETION_STATE_LOCK)
                .slots
                .values()
                .all(|slot| slot.phase == CompletionPhase::Unresolved)
        {
            let join = self.join.lock().expect(COMPLETION_STATE_LOCK).take();
            if let Some(join) = join
                && let Err(error) = join.await
            {
                if error.is_panic() {
                    let payload = std::mem::ManuallyDrop::new(error.into_panic());
                    let mut state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
                    state.owner_panic = Some(payload);
                    if state.custody.is_none() {
                        state.custody = Some(Arc::clone(&self.shared));
                    }
                    CompletionShared::fail_locked(
                        &mut state,
                        "task completion owner panicked before a trustworthy drain",
                    );
                }
                return Err("task completion owner stopped unexpectedly".to_string());
            }
        }
        result
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }
}
impl Drop for TaskCompletionSupervisor {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .expect(COMPLETION_STATE_LOCK)
            .accepting = false;
        self.shared.changed.notify_waiters();
        // Dropping JoinHandle detaches the existing owner. Never abort work or
        // implicitly destroy its holders because application shutdown timed out.
    }
}

#[cfg(test)]
mod admitted_exit_tests {
    use super::*;
    use crate::task_lifecycle::{CompletionExitCapability, CompletionExitNotice};
    use novarocks_execution::runtime::fragment::{
        ExecutionFailure, FragmentExecutionError, FragmentExecutionErrorKind, FragmentOutcome,
    };
    use novarocks_execution_contract::failure::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };
    use novarocks_types::{
        UniqueId,
        identity::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(107001, 107002), AttemptId::new(1).unwrap())
                .unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    struct Capture {
        mask: Arc<AtomicUsize>,
        bit: usize,
    }
    impl std::fmt::Debug for Capture {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original completion fact capture")
        }
    }
    impl std::fmt::Display for Capture {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original completion fact capture")
        }
    }
    impl std::error::Error for Capture {}
    impl FailureProjection for Capture {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(60002).unwrap()),
            )
        }
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            self.mask.fetch_or(self.bit, Ordering::AcqRel);
        }
    }
    struct Exit {
        mask: Arc<AtomicUsize>,
        outcome: Arc<AtomicUsize>,
        expected_mask: usize,
    }
    impl Exit {
        fn observe(&self, outcome: usize) {
            assert_eq!(
                self.mask.load(Ordering::Acquire),
                self.expected_mask,
                "exit handoff follows each actual original capture destructor"
            );
            assert_eq!(
                self.outcome.swap(outcome, Ordering::AcqRel),
                0,
                "one original finite authority is consumed once"
            );
        }
    }
    impl CompletionExitCapability for Exit {
        fn exited(self: Box<Self>) {
            self.observe(1);
        }
        fn withdrawn(self: Box<Self>) {
            self.observe(2);
        }
        fn revoke_unused(self: Box<Self>) {
            self.observe(3);
        }
        fn unresolved(self: Box<Self>) {
            self.observe(4);
        }
    }
    fn notice(
        mask: &Arc<AtomicUsize>,
        outcome: &Arc<AtomicUsize>,
        expected_mask: usize,
    ) -> CompletionExitNotice {
        CompletionExitNotice::from_admitted_role(
            identity(),
            Box::new(Exit {
                mask: mask.clone(),
                outcome: outcome.clone(),
                expected_mask,
            }),
        )
    }
    fn work(
        panic_body: bool,
        mask: &Arc<AtomicUsize>,
        outcome: &Arc<AtomicUsize>,
    ) -> CompletionWork {
        let capture = Capture {
            mask: mask.clone(),
            bit: 1,
        };
        let failure = ExecutionFailure::from_domain(Capture {
            mask: mask.clone(),
            bit: 2,
        });
        let mut exit = notice(mask, outcome, 3);
        exit.bind();
        CompletionWork {
            action: Some(Box::new(move |_| {
                let _ = &capture;
                assert!(!panic_body, "original completion action panic");
            })),
            fact: Some(FragmentTerminalFact::new(
                QueryId::new(107001, 107002),
                UniqueId::new(107003, 107004),
                FragmentOutcome::Failed(FragmentExecutionError::from_failure(
                    FragmentExecutionErrorKind::Pipeline,
                    failure,
                )),
                None,
            )),
            reporter: None,
            exit: Some(exit),
            unresolved_payloads: std::array::from_fn(|_| None),
        }
    }
    #[test]
    fn independent_exit_follows_actual_action_and_original_fact_destruction() {
        let mask = Arc::new(AtomicUsize::new(0));
        let outcome = Arc::new(AtomicUsize::new(0));
        let mut work = work(false, &mask, &outcome);
        assert!(work.run(false));
        assert_eq!(mask.load(Ordering::Acquire), 3);
        assert_eq!(outcome.load(Ordering::Acquire), 1);
        assert!(work.action.is_none() && work.fact.is_none() && work.exit.is_none());
    }
    #[test]
    fn a_live_original_action_destructor_blocks_its_independent_exit_notice() {
        struct GatedAction {
            inner: Option<TaskCompletionAction>,
            entered: std::sync::mpsc::Sender<()>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl GatedAction {
            fn run(&mut self, fact: &FragmentTerminalFact) {
                self.inner.as_mut().unwrap()(fact);
            }
        }
        impl Drop for GatedAction {
            fn drop(&mut self) {
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
                drop(self.inner.take());
            }
        }
        let mask = Arc::new(AtomicUsize::new(0));
        let outcome = Arc::new(AtomicUsize::new(0));
        let mut work = work(false, &mask, &outcome);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut action = GatedAction {
            inner: work.action.take(),
            entered: entered_tx,
            release: release_rx,
        };
        work.action = Some(Box::new(move |fact| action.run(fact)));
        let join = std::thread::spawn(move || work.run(false));
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let before_exit = outcome.load(Ordering::Acquire);
        let before_drop = mask.load(Ordering::Acquire);
        release_tx.send(()).unwrap();
        let joined = join.join().unwrap();
        assert!(entered.is_ok());
        assert_eq!(
            before_exit, 0,
            "a live action destructor cannot publish CompletionExited"
        );
        assert_eq!(
            before_drop, 0,
            "both the action capture and original fact are still live"
        );
        assert!(joined);
        assert_eq!(mask.load(Ordering::Acquire), 3);
        assert_eq!(outcome.load(Ordering::Acquire), 1);
    }
    #[test]
    fn a_panicked_original_action_never_emits_the_positive_completion_exit() {
        let mask = Arc::new(AtomicUsize::new(0));
        let outcome = Arc::new(AtomicUsize::new(0));
        let mut work = work(true, &mask, &outcome);
        assert!(!work.run(false));
        assert_eq!(mask.load(Ordering::Acquire), 3);
        assert_eq!(outcome.load(Ordering::Acquire), 4);
    }
    #[test]
    fn withdrawn_action_disposes_its_captures_without_running_or_claiming_runtime_exit() {
        let mask = Arc::new(AtomicUsize::new(0));
        let outcome = Arc::new(AtomicUsize::new(0));
        let mut work = work(true, &mask, &outcome);
        assert!(work.run(true));
        assert_eq!(mask.load(Ordering::Acquire), 3);
        assert_eq!(outcome.load(Ordering::Acquire), 2);
    }
    #[test]
    fn withdrawal_before_action_binding_returns_the_original_undropped_callback() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()
            .unwrap();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let mask = Arc::new(AtomicUsize::new(0));
        let outcome = Arc::new(AtomicUsize::new(0));
        let exit = notice(&mask, &outcome, 0);
        let mut reservation = supervisor
            .reserve_empty(
                exit.identity(),
                UniqueId::new(107003, 107004),
                None,
                Some(exit),
                None,
            )
            .unwrap();
        supervisor
            .shared
            .withdraw_creation(reservation.identity, &reservation.ticket);
        supervisor.shutdown().unwrap();
        assert_eq!(
            outcome.load(Ordering::Acquire),
            3,
            "the never-bound original role can be revoked"
        );
        let capture = Capture {
            mask: mask.clone(),
            bit: 1,
        };
        let action: TaskCompletionAction = Box::new(move |_| {
            let _ = &capture;
            panic!("the rejected action cannot run");
        });
        let (refusal, action) = match reservation.bind_action(action) {
            Ok(()) => panic!("a drained slot cannot bind an action"),
            Err(refusal) => refusal,
        };
        assert!(matches!(
            refusal,
            CompletionReservationError::Unavailable(_)
        ));
        assert_eq!(
            mask.load(Ordering::Acquire),
            0,
            "rejection transfers the original callback without running its destructor"
        );
        drop(action);
        assert_eq!(mask.load(Ordering::Acquire), 1);
        assert_eq!(
            outcome.load(Ordering::Acquire),
            3,
            "an unattached action cannot invent runtime exit"
        );
    }
    #[test]
    fn an_unattached_original_exit_role_is_revoked_without_a_stopped_fact() {
        let mask = Arc::new(AtomicUsize::new(0));
        let outcome = Arc::new(AtomicUsize::new(0));
        drop(notice(&mask, &outcome, 0));
        assert_eq!(outcome.load(Ordering::Acquire), 3);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_execution::runtime::fragment::{FragmentOutcome, FragmentTerminalFact};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use super::TaskCompletionSupervisor;
    use novarocks_execution_contract::TaskConclusionPort;

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()
            .expect("completion test runtime")
    }

    fn identity(query: i64, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(query, query + 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn terminal(identity: TaskIdentity, fragment: UniqueId) -> FragmentTerminalFact {
        FragmentTerminalFact::new(
            identity.query_execution_id().query_id(),
            fragment,
            FragmentOutcome::Succeeded,
            None,
        )
    }

    fn wait_for_count(count: &AtomicUsize, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while count.load(Ordering::Acquire) != expected && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(count.load(Ordering::Acquire), expected);
    }

    fn injected_owner_failure() {
        panic!("injected completion owner panic");
    }

    #[test]
    fn completion_before_creation_commit_is_delivered_once_after_commit() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 2);
        let identity = identity(91_001, 1);
        let fragment = UniqueId::new(91_003, 91_004);
        let completions = Arc::new(AtomicUsize::new(0));
        let action_completions = Arc::clone(&completions);
        let signal = supervisor
            .reserve(
                identity,
                fragment,
                Box::new(move |_| {
                    action_completions.fetch_add(1, Ordering::Release);
                }),
            )
            .expect("reserve exact slot");

        signal.publish(terminal(identity, fragment));
        signal.publish(terminal(identity, fragment));
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(completions.load(Ordering::Acquire), 0);

        signal.commit_creation();
        signal.commit_creation();
        wait_for_count(&completions, 1);
        signal.publish(terminal(identity, fragment));
        assert_eq!(completions.load(Ordering::Acquire), 1);
        supervisor.shutdown().expect("completion owner drains");
    }

    #[test]
    fn exact_slot_bound_rejects_excess_work_before_start() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let first = identity(92_001, 1);
        let second = identity(92_011, 1);
        let first_fragment = UniqueId::new(92_003, 92_004);
        let first_reservation = supervisor
            .reserve(first, first_fragment, Box::new(|_| {}))
            .expect("first slot fits");
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, ResourceDimension,
        };
        let duplicate = supervisor
            .reserve(first, first_fragment, Box::new(|_| {}))
            .err()
            .expect("duplicate must refuse before capacity");
        assert!(
            matches!(duplicate, super::CompletionReservationError::Duplicate(actual) if actual == first)
        );
        assert_eq!(duplicate.failure_fact().class(), FailureClass::Invariant);
        let refused = supervisor
            .reserve(second, UniqueId::new(92_013, 92_014), Box::new(|_| {}))
            .err()
            .expect("second exact slot exceeds the bound");
        assert_eq!(refused.failure_fact().class(), FailureClass::Resource);
        let resource = refused.failure_fact().resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Positions);
        assert_eq!(
            (
                resource.requested,
                resource.required_growth,
                resource.committed,
                resource.limit
            ),
            (Some(1), Some(1), Some(1), Some(1))
        );
        assert_eq!(resource.live, None);
        assert_eq!(supervisor.shared.state.lock().unwrap().slots.len(), 1);
        // Uncommitted creation withdraws through the same actual disposal owner.
        drop(first_reservation);
        supervisor
            .shutdown()
            .expect("withdrawn creation actually disposes");
    }

    #[test]
    fn owner_panic_is_supervision_failure_and_does_not_run_later_steps() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let identity = identity(93_001, 1);
        let fragment = UniqueId::new(93_003, 93_004);
        let actual_stop = Arc::new(AtomicUsize::new(0));
        let converged = Arc::new(AtomicUsize::new(0));
        let action_actual_stop = Arc::clone(&actual_stop);
        let action_converged = Arc::clone(&converged);
        let signal = supervisor
            .reserve(
                identity,
                fragment,
                Box::new(move |_| {
                    action_actual_stop.fetch_add(1, Ordering::Release);
                    injected_owner_failure();
                    action_converged.fetch_add(1, Ordering::Release);
                }),
            )
            .expect("reserve exact slot");
        signal.commit_creation();
        signal.publish(terminal(identity, fragment));

        let deadline = Instant::now() + Duration::from_secs(1);
        while supervisor.poll_failure().is_none() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(supervisor.poll_failure().is_some());
        assert_eq!(actual_stop.load(Ordering::Acquire), 1);
        assert_eq!(converged.load(Ordering::Acquire), 0);
        assert!(supervisor.shutdown().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_when_called_from_the_runtime() {
        let supervisor = TaskCompletionSupervisor::start(tokio::runtime::Handle::current(), 1);
        let identity = identity(94_001, 1);
        let fragment = UniqueId::new(94_003, 94_004);
        let completions = Arc::new(AtomicUsize::new(0));
        let action_completions = Arc::clone(&completions);
        let signal = supervisor
            .reserve(
                identity,
                fragment,
                Box::new(move |_| {
                    action_completions.fetch_add(1, Ordering::Release);
                }),
            )
            .expect("reserve exact slot");

        signal.commit_creation();
        signal.publish(terminal(identity, fragment));

        supervisor
            .shutdown()
            .expect("completion owner drains from the runtime");
        assert_eq!(completions.load(Ordering::Acquire), 1);
        let refused = supervisor
            .reserve(
                self::identity(94_011, 1),
                UniqueId::new(94_013, 94_014),
                Box::new(|_| {}),
            )
            .err()
            .unwrap();
        assert_eq!(refused, super::CompletionReservationError::ShuttingDown);
        assert!(
            supervisor.poll_failure().is_none(),
            "normal shutdown is not a runtime failure"
        );
    }

    #[derive(Default)]
    struct CompletionExitGate {
        state: std::sync::Mutex<(bool, bool)>,
        changed: std::sync::Condvar,
    }
    impl CompletionExitGate {
        fn enter_and_wait(&self) {
            let mut state = self.state.lock().expect("completion exit gate lock");
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self.changed.wait(state).expect("completion exit gate wait");
            }
        }
        fn wait_entered(&self) -> bool {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut state = self.state.lock().expect("completion exit gate lock");
            while !state.0 {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return false;
                };
                let (next, timeout) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .expect("completion exit gate observation");
                state = next;
                if timeout.timed_out() && !state.0 {
                    return false;
                }
            }
            true
        }
        fn release(&self) {
            self.state.lock().expect("completion exit gate lock").1 = true;
            self.changed.notify_all();
        }
    }
    struct ReleaseCompletionExitGate(Arc<CompletionExitGate>);
    impl ReleaseCompletionExitGate {
        fn release(&self) {
            self.0.release();
        }
    }
    impl Drop for ReleaseCompletionExitGate {
        fn drop(&mut self) {
            // A failed positive observation must not strand the actual owner
            // inside the action or its capture destructor during runtime Drop.
            self.0.release();
        }
    }
    struct CompletionCaptureExit {
        gate: Option<Arc<CompletionExitGate>>,
        body_entries: Arc<AtomicUsize>,
        exited: Arc<AtomicUsize>,
    }
    impl CompletionCaptureExit {
        fn observe_body(&self) {
            // Borrow the whole captured owner. Its Drop stays part of the real
            // FnOnce invocation's exit, rather than a separate fake callback.
            self.body_entries.fetch_add(1, Ordering::Release);
        }
    }
    impl Drop for CompletionCaptureExit {
        fn drop(&mut self) {
            if let Some(gate) = self.gate.as_ref() {
                gate.enter_and_wait();
            }
            self.exited.fetch_add(1, Ordering::Release);
        }
    }

    #[test]
    fn running_action_and_capture_drop_retain_the_exact_slot_until_actual_exit() {
        let runtime = test_runtime();
        let mut observations = Vec::new();
        // The same one-position contract applies both while the real action
        // runs and while its actual closure capture is being destroyed.
        for capture_drop in [false, true] {
            let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
            let gate = Arc::new(CompletionExitGate::default());
            let release = ReleaseCompletionExitGate(Arc::clone(&gate));
            let body_entries = Arc::new(AtomicUsize::new(0));
            let capture_exits = Arc::new(AtomicUsize::new(0));
            let captured = CompletionCaptureExit {
                gate: capture_drop.then(|| Arc::clone(&gate)),
                body_entries: Arc::clone(&body_entries),
                exited: Arc::clone(&capture_exits),
            };
            let action_gate = Arc::clone(&gate);
            let task = identity(95_001 + i64::from(capture_drop) * 100, 1);
            let fragment = UniqueId::new(95_003, 95_004);
            let signal = supervisor
                .reserve(
                    task,
                    fragment,
                    Box::new(move |_| {
                        captured.observe_body();
                        if !capture_drop {
                            action_gate.enter_and_wait();
                        }
                    }),
                )
                .expect("reserve actual action slot");
            signal.commit_creation();
            signal.publish(terminal(task, fragment));
            let entered = gate.wait_entered();

            // Probe the real admission port while the selected owner is
            // blocked. If the current implementation wrongly accepts, make
            // that real admitted completion drainable before releasing A.
            let probe_task = identity(95_011 + i64::from(capture_drop) * 100, 1);
            let probe_fragment = UniqueId::new(95_013, 95_014);
            let probe = supervisor.reserve(probe_task, probe_fragment, Box::new(|_| {}));
            let reservation = match probe {
                Ok(probe) => {
                    probe.commit_creation();
                    probe.publish(terminal(probe_task, probe_fragment));
                    Ok(())
                }
                Err(refusal) => Err(refusal),
            };
            release.release();
            // shutdown joins the existing owner. No negative assertion runs
            // with either real action/destructor still behind its gate.
            let joined = supervisor.shutdown();
            observations.push((
                capture_drop,
                entered,
                reservation,
                body_entries.load(Ordering::Acquire),
                capture_exits.load(Ordering::Acquire),
                joined,
            ));
        }
        for (capture_drop, entered, reservation, entries, exits, joined) in observations {
            assert!(
                joined.is_ok(),
                "the actual completion owner must join: {joined:?}"
            );
            assert!(
                entered,
                "the real exit gate must be reached (capture_drop={capture_drop})"
            );
            assert_eq!(entries, 1);
            assert_eq!(exits, 1, "the original closure capture must really exit");
            assert!(
                matches!(
                    reservation,
                    Err(super::CompletionReservationError::SlotCapacity {
                        committed: 1,
                        limit: 1
                    })
                ),
                "the running action/capture still owns its sole position (capture_drop={capture_drop}); observed {reservation:?}"
            );
        }
    }

    #[test]
    fn one_panicked_completion_keeps_its_unresolved_slot_and_drains_an_accepted_sibling() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 2);
        let gate = Arc::new(CompletionExitGate::default());
        let release = ReleaseCompletionExitGate(Arc::clone(&gate));
        let action_gate = Arc::clone(&gate);
        let failed = identity(96_001, 1);
        let failed_fragment = UniqueId::new(96_003, 96_004);
        let failed_signal = supervisor
            .reserve(
                failed,
                failed_fragment,
                Box::new(move |_| {
                    action_gate.enter_and_wait();
                    injected_owner_failure();
                }),
            )
            .expect("reserve the actual failing action");
        failed_signal.commit_creation();
        failed_signal.publish(terminal(failed, failed_fragment));
        let entered = gate.wait_entered();

        let sibling = identity(96_011, 1);
        let sibling_fragment = UniqueId::new(96_013, 96_014);
        let sibling_exits = Arc::new(AtomicUsize::new(0));
        let completed_sibling = Arc::clone(&sibling_exits);
        let sibling_signal = supervisor
            .reserve(
                sibling,
                sibling_fragment,
                Box::new(move |_| {
                    completed_sibling.fetch_add(1, Ordering::Release);
                }),
            )
            .expect("the sibling is accepted before the first action can panic");
        sibling_signal.commit_creation();
        sibling_signal.publish(terminal(sibling, sibling_fragment));

        release.release();
        // The real action panic is caught by the real supervisor. Its shutdown
        // joins that owner before either negative ownership assertion below.
        let joined = supervisor.shutdown();
        let failure = supervisor.poll_failure();
        let (failed_slot_retained, sibling_slot_retained) = {
            let state = supervisor
                .shared
                .state
                .lock()
                .expect("actual completion slots");
            (
                state.slots.contains_key(&failed),
                state.slots.contains_key(&sibling),
            )
        };
        let sibling_exits = sibling_exits.load(Ordering::Acquire);
        assert!(
            entered,
            "the actual action must have started before sibling admission"
        );
        assert!(
            joined.is_err(),
            "unknown work is not a successful drain receipt"
        );
        assert!(
            failure.is_some(),
            "the actual captured owner panic remains observable"
        );
        assert!(
            failed_slot_retained && !sibling_slot_retained && sibling_exits == 1,
            "an unresolved action retains responsibility while the accepted sibling really exits: failed_slot_retained={failed_slot_retained}, sibling_slot_retained={sibling_slot_retained}, sibling_exits={sibling_exits}"
        );
    }

    #[test]
    fn uncommitted_creation_disposes_its_capture_without_invoking_the_action() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let gate = Arc::new(CompletionExitGate::default());
        let release = ReleaseCompletionExitGate(Arc::clone(&gate));
        let entries = Arc::new(AtomicUsize::new(0));
        let exits = Arc::new(AtomicUsize::new(0));
        let captured = CompletionCaptureExit {
            gate: Some(Arc::clone(&gate)),
            body_entries: Arc::clone(&entries),
            exited: Arc::clone(&exits),
        };
        let task = identity(97_001, 1);
        let reserved = supervisor
            .reserve(
                task,
                UniqueId::new(97_003, 97_004),
                Box::new(move |_| captured.observe_body()),
            )
            .expect("reserve a real uncommitted creation");
        drop(reserved);
        let disposing = gate.wait_entered();
        let refused = supervisor
            .reserve(
                identity(97_011, 1),
                UniqueId::new(97_013, 97_014),
                Box::new(|_| {}),
            )
            .err();
        release.release();
        let joined = supervisor.shutdown();
        assert!(
            disposing,
            "withdrawal must reach the actual capture destructor"
        );
        assert!(joined.is_ok(), "actual withdrawal must drain: {joined:?}");
        assert_eq!(
            entries.load(Ordering::Acquire),
            0,
            "withdrawal never invokes completion"
        );
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert!(matches!(
            refused,
            Some(super::CompletionReservationError::SlotCapacity {
                committed: 1,
                limit: 1
            })
        ));
    }

    #[test]
    fn blocking_completion_does_not_stall_an_accepted_sibling_on_one_async_worker() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 2);
        let gate = Arc::new(CompletionExitGate::default());
        let release = ReleaseCompletionExitGate(Arc::clone(&gate));
        let action_gate = Arc::clone(&gate);
        let task = identity(98_001, 1);
        let fragment = UniqueId::new(98_003, 98_004);
        let first = supervisor
            .reserve(
                task,
                fragment,
                Box::new(move |_| action_gate.enter_and_wait()),
            )
            .unwrap();
        first.commit_creation();
        first.publish(terminal(task, fragment));
        let entered = gate.wait_entered();
        let sibling = identity(98_011, 1);
        let sibling_fragment = UniqueId::new(98_013, 98_014);
        let exits = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&exits);
        let second = supervisor
            .reserve(
                sibling,
                sibling_fragment,
                Box::new(move |_| {
                    observed.fetch_add(1, Ordering::Release);
                }),
            )
            .unwrap();
        second.commit_creation();
        second.publish(terminal(sibling, sibling_fragment));
        let deadline = Instant::now() + Duration::from_secs(5);
        while exits.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let sibling_exited_while_blocked = exits.load(Ordering::Acquire);
        release.release();
        let joined = supervisor.shutdown();
        assert!(entered);
        assert!(joined.is_ok(), "both real actions must join: {joined:?}");
        assert_eq!(
            sibling_exited_while_blocked, 1,
            "a blocked cleanup cannot hold the sole async worker"
        );
    }

    #[test]
    fn old_publisher_cannot_fill_a_reused_exact_task_reservation() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let task = identity(99_001, 1);
        let fragment = UniqueId::new(99_003, 99_004);
        let old = supervisor
            .reserve(
                task,
                fragment,
                Box::new(|_| panic!("withdrawn action must never run")),
            )
            .unwrap();
        let old_signal = old.signal();
        drop(old);
        // Wait for actual disposal and join, rather than merely dropping the
        // reservation and presuming its capacity has already returned.
        let deadline = Instant::now() + Duration::from_secs(5);
        let next = loop {
            match supervisor.reserve(task, fragment, Box::new(|_| {})) {
                Ok(next) => break next,
                Err(super::CompletionReservationError::Duplicate(_))
                    if Instant::now() < deadline =>
                {
                    std::thread::yield_now()
                }
                Err(error) => panic!("actual withdrawal did not release its position: {error}"),
            }
        };
        next.commit_creation();
        old_signal.publish(terminal(task, fragment));
        let old_filled_new_slot = supervisor
            .shared
            .state
            .lock()
            .unwrap()
            .slots
            .get(&task)
            .unwrap()
            .fact_received;
        next.publish(terminal(task, fragment));
        let joined = supervisor.shutdown();
        assert!(
            joined.is_ok(),
            "the current exact reservation drains: {joined:?}"
        );
        assert!(
            !old_filled_new_slot,
            "a withdrawn publisher cannot target a later reservation of the same identity"
        );
    }

    #[test]
    fn dropping_the_application_handle_does_not_abort_accepted_completion() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 2);
        let gate = Arc::new(CompletionExitGate::default());
        let release = ReleaseCompletionExitGate(Arc::clone(&gate));
        let action_gate = Arc::clone(&gate);
        let task = identity(100_001, 1);
        let fragment = UniqueId::new(100_003, 100_004);
        let first = supervisor
            .reserve(
                task,
                fragment,
                Box::new(move |_| action_gate.enter_and_wait()),
            )
            .unwrap();
        let signal = first.commit_creation();
        drop(first);
        signal.publish(terminal(task, fragment));
        let entered = gate.wait_entered();
        let sibling = identity(100_011, 1);
        let sibling_fragment = UniqueId::new(100_013, 100_014);
        let exits = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&exits);
        let second = supervisor
            .reserve(
                sibling,
                sibling_fragment,
                Box::new(move |_| {
                    observed.fetch_add(1, Ordering::Release);
                }),
            )
            .unwrap();
        let sibling_signal = second.commit_creation();
        drop(second);
        sibling_signal.publish(terminal(sibling, sibling_fragment));
        let shared = Arc::downgrade(&supervisor.shared);
        drop(supervisor);
        release.release();
        let deadline = Instant::now() + Duration::from_secs(5);
        while shared.upgrade().is_some() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(entered);
        assert!(
            shared.upgrade().is_none(),
            "the existing owner must finish real work and release custody"
        );
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    fn status_owner(task: TaskIdentity) -> Arc<crate::TaskStatusOwner> {
        Arc::new(crate::TaskStatusOwner::new(
            task,
            Arc::new(crate::TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            Duration::from_millis(250),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        ))
    }

    #[test]
    fn native_exit_evidence_follows_the_actual_action_capture_destruction() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let task = identity(101_001, 1);
        let fragment = UniqueId::new(101_003, 101_004);
        let owner = status_owner(task);
        let reporter = crate::TaskStatusReporter::new(Arc::clone(&owner));
        reporter.canceling(novarocks_execution_contract::task_execution::status::CancelReason::UpstreamNoLongerNeeded);
        let accepted = owner.accepted().unwrap();
        let gate = Arc::new(CompletionExitGate::default());
        let release = ReleaseCompletionExitGate(Arc::clone(&gate));
        let exits = Arc::new(AtomicUsize::new(0));
        let captured = CompletionCaptureExit {
            gate: Some(Arc::clone(&gate)),
            body_entries: Arc::new(AtomicUsize::new(0)),
            exited: Arc::clone(&exits),
        };
        let reservation = supervisor
            .reserve_with_status(
                task,
                fragment,
                Box::new(move |_| captured.observe_body()),
                Some(reporter),
            )
            .unwrap();
        reservation.commit_creation();
        reservation.publish(terminal(task, fragment));
        let disposing = gate.wait_entered();
        let before = owner.convergence();
        release.release();
        let joined = supervisor.shutdown();
        assert!(
            joined.is_ok(),
            "the original work must really join: {joined:?}"
        );
        assert!(disposing);
        assert!(
            !before.actual_stopped() && !before.resources_converged(),
            "logical control cannot cover a live capture destructor"
        );
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert!(owner.convergence().actual_stopped() && owner.convergence().resources_converged());
        assert!(
            Arc::ptr_eq(&accepted, &owner.accepted().unwrap()),
            "exit publication never accepts another Task conclusion"
        );
    }

    #[test]
    fn an_exit_reporter_for_another_task_is_refused_before_admission() {
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        let reserved = identity(102_001, 1);
        let foreign = identity(102_011, 1);
        let owner = status_owner(foreign);
        let reporter = crate::TaskStatusReporter::new(Arc::clone(&owner));
        let refusal = supervisor
            .reserve_with_status(
                reserved,
                UniqueId::new(102_003, 102_004),
                Box::new(|_| panic!("refused work cannot run")),
                Some(reporter),
            )
            .err()
            .unwrap();
        assert!(
            matches!(refusal, super::CompletionReservationError::StatusIdentity { reserved: actual, reporter: other } if actual == reserved && other == foreign)
        );
        assert_eq!(refusal.failure_fact().class(), FailureClass::Invariant);
        assert!(supervisor.shared.state.lock().unwrap().slots.is_empty());
        assert!(!owner.convergence().actual_stopped());
        supervisor.shutdown().unwrap();
    }

    #[test]
    fn a_runtime_that_never_polled_its_owner_cannot_admit_a_task() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 1);
        drop(runtime);
        let refusal = supervisor
            .reserve(
                identity(103_001, 1),
                UniqueId::new(103_003, 103_004),
                Box::new(|_| panic!("unavailable runtime cannot run work")),
            )
            .err()
            .unwrap();
        assert!(matches!(
            refusal,
            super::CompletionReservationError::Unavailable(_)
        ));
        assert!(supervisor.poll_failure().is_some());
        assert!(supervisor.shared.state.lock().unwrap().slots.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_callers_await_the_same_actual_drain() {
        let supervisor = TaskCompletionSupervisor::start(tokio::runtime::Handle::current(), 1);
        let task = identity(104_001, 1);
        let fragment = UniqueId::new(104_003, 104_004);
        let exits = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&exits);
        let reserved = supervisor
            .reserve(
                task,
                fragment,
                Box::new(move |_| {
                    observed.fetch_add(1, Ordering::Release);
                }),
            )
            .unwrap();
        reserved.commit_creation();
        reserved.publish(terminal(task, fragment));
        assert!(
            supervisor
                .shutdown()
                .unwrap_err()
                .contains("must be awaited")
        );
        supervisor
            .shutdown_async()
            .await
            .expect("the original owner and action actually join");
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    struct CompletionOpaquePayload {
        gate: Arc<CompletionExitGate>,
        exits: Arc<AtomicUsize>,
        secondary_exits: Arc<AtomicUsize>,
    }
    struct CompletionSecondaryPayload(Arc<AtomicUsize>);
    impl Drop for CompletionSecondaryPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }
    impl Drop for CompletionOpaquePayload {
        fn drop(&mut self) {
            self.gate.enter_and_wait();
            self.exits.fetch_add(1, Ordering::Release);
            std::panic::panic_any(CompletionSecondaryPayload(Arc::clone(
                &self.secondary_exits,
            )));
        }
    }

    #[test]
    fn opaque_panic_disposal_keeps_custody_and_cannot_replace_the_accepted_task() {
        let runtime = test_runtime();
        let supervisor = TaskCompletionSupervisor::start(runtime.handle().clone(), 2);
        let task = identity(105_001, 1);
        let fragment = UniqueId::new(105_003, 105_004);
        let owner = status_owner(task);
        let reporter = crate::TaskStatusReporter::new(Arc::clone(&owner));
        reporter.canceling(novarocks_execution_contract::task_execution::status::CancelReason::UpstreamNoLongerNeeded);
        let accepted = owner.accepted().unwrap();
        let gate = Arc::new(CompletionExitGate::default());
        let release = ReleaseCompletionExitGate(Arc::clone(&gate));
        let exits = Arc::new(AtomicUsize::new(0));
        let secondary_exits = Arc::new(AtomicUsize::new(0));
        let mut payload = Some(CompletionOpaquePayload {
            gate: Arc::clone(&gate),
            exits: Arc::clone(&exits),
            secondary_exits: Arc::clone(&secondary_exits),
        });
        let action = supervisor
            .reserve_with_status(
                task,
                fragment,
                Box::new(move |_| {
                    std::panic::panic_any(payload.take().expect("one original opaque payload"));
                }),
                Some(reporter),
            )
            .unwrap();
        action.commit_creation();
        action.publish(terminal(task, fragment));
        let disposing = gate.wait_entered();
        let during = owner.convergence();
        // A is inside the original payload destructor, not an async readiness
        // callback. An independently accepted sibling must still really exit.
        let sibling = identity(105_011, 1);
        let sibling_fragment = UniqueId::new(105_013, 105_014);
        let sibling_exits = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&sibling_exits);
        let sibling_work = supervisor
            .reserve(
                sibling,
                sibling_fragment,
                Box::new(move |_| {
                    observed.fetch_add(1, Ordering::Release);
                }),
            )
            .unwrap();
        sibling_work.commit_creation();
        sibling_work.publish(terminal(sibling, sibling_fragment));
        let deadline = Instant::now() + Duration::from_secs(5);
        while sibling_exits.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let sibling_exited_before_release = sibling_exits.load(Ordering::Acquire);
        release.release();
        let joined = supervisor.shutdown();
        assert!(disposing);
        assert_eq!(sibling_exited_before_release, 1);
        assert!(
            joined.is_err(),
            "an opaque secondary destruction failure cannot prove drain"
        );
        assert_eq!(
            exits.load(Ordering::Acquire),
            1,
            "never retry a partially destroyed payload"
        );
        assert_eq!(
            secondary_exits.load(Ordering::Acquire),
            0,
            "the secondary opaque holder stays in its original bounded custody"
        );
        assert!(!during.actual_stopped() && !during.resources_converged());
        assert!(
            !owner.convergence().actual_stopped() && !owner.convergence().resources_converged()
        );
        assert!(
            Arc::ptr_eq(&accepted, &owner.accepted().unwrap()),
            "cleanup does not accept or refine a Task conclusion"
        );
        assert!(supervisor.poll_failure().is_some());
    }
}
