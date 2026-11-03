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
//! Event-driven scheduler for blocked drivers.
//!
//! Responsibilities:
//! - Associates dependency events with blocked driver keys and wake-up queues.
//! - Coalesces notifications and returns blocked tasks to worker threads.
//! - Filters stale observable identity/generation events without polling operators.
//! - Holds drivers in `PendingFinish` until a finish watch fires or a requested
//!   recheck is due.
//!
//! Key exported interfaces:
//! - Types: `DriverKey`, `EventScheduler`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

#[cfg(test)]
use crate::exec::pipeline::operator::OperatorLifecycle;
#[cfg(test)]
use crate::runtime::fragment::ExecutionResult;

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;
use std::time::Instant;

use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::global_driver_executor::{DriverTask, ExecutorShared};
use crate::exec::pipeline::operator::{BlockedReason, DriverBlockDeadline};
use crate::exec::pipeline::schedule::observer::{Observable, PipelineObserver};
use crate::runtime::dispatch_metrics::{
    DispatchTransition, StaleDispatchEvent, observe_dispatch, observe_stale_dispatch,
};
use tracing::debug;

const EVENT_SCHEDULER_LOG_EVERY: u64 = 1024;
/// Deadline token of a pending-finish recheck. Operator block deadlines carry
/// their operator's token; this one belongs to the driver.
const PENDING_FINISH_RECHECK_TOKEN: u64 = u64::MAX;
static EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT: AtomicU64 = AtomicU64::new(0);
static EVENT_SCHEDULER_BLOCKED_ADD_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_sample_log(counter: &AtomicU64) -> bool {
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(EVENT_SCHEDULER_LOG_EVERY)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Stable key that identifies a driver in event-scheduler maps and queues.
pub(crate) struct DriverKey {
    finst: Option<(i64, i64)>,
    driver_id: i32,
}

struct BlockedTask {
    task: DriverTask,
    observable: Option<(Weak<Observable>, u64)>,
    /// Terminal operator's early-finish observable and the generation sampled
    /// before the driver last checked whether it was finished. A newer
    /// generation wakes the driver whatever it is otherwise waiting for.
    terminal: Option<(Weak<Observable>, u64)>,
    block_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScheduledDeadline {
    at: Instant,
    operator_token: u64,
    block_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeadlineIndexEntry {
    at: Instant,
    key: DriverKey,
    block_epoch: u64,
    operator_token: u64,
}

impl DeadlineIndexEntry {
    fn new(key: DriverKey, deadline: ScheduledDeadline) -> Self {
        Self {
            at: deadline.at,
            key,
            block_epoch: deadline.block_epoch,
            operator_token: deadline.operator_token,
        }
    }

    fn deadline(self) -> ScheduledDeadline {
        ScheduledDeadline {
            at: self.at,
            operator_token: self.operator_token,
            block_epoch: self.block_epoch,
        }
    }
}

struct RescheduleState {
    queue: VecDeque<DriverKey>,
    /// Keys queued for rescheduling, with when their earliest coalesced
    /// wake-up became effective.
    pending: HashMap<DriverKey, WakeOrigin>,
    deadlines: HashMap<DriverKey, ScheduledDeadline>,
    deadline_index: BTreeSet<DeadlineIndexEntry>,
}

/// When and why a queued key's wake-up became effective.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WakeOrigin {
    at: Instant,
    transition: DispatchTransition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchedulerWake {
    Key(DriverKey),
    Deadline(DriverKey, u64, u64),
}

impl RescheduleState {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pending: HashMap::new(),
            deadlines: HashMap::new(),
            deadline_index: BTreeSet::new(),
        }
    }

    fn enqueue(&mut self, key: DriverKey) -> bool {
        self.enqueue_from(
            key,
            WakeOrigin {
                at: Instant::now(),
                transition: DispatchTransition::WakeToEnqueue,
            },
        )
    }

    /// Queues `key` unless it is queued already; a coalesced wake-up keeps
    /// the earlier origin.
    fn enqueue_from(&mut self, key: DriverKey, origin: WakeOrigin) -> bool {
        if self.pending.contains_key(&key) {
            return false;
        }
        self.pending.insert(key, origin);
        self.queue.push_back(key);
        true
    }

    fn install_deadline(&mut self, key: DriverKey, deadline: ScheduledDeadline) {
        if let Some(previous) = self.deadlines.insert(key, deadline) {
            let removed = self
                .deadline_index
                .remove(&DeadlineIndexEntry::new(key, previous));
            debug_assert!(removed, "deadline map/index drift on replacement");
        }
        let inserted = self
            .deadline_index
            .insert(DeadlineIndexEntry::new(key, deadline));
        debug_assert!(inserted, "duplicate deadline index entry");
    }

    fn remove_deadline(&mut self, key: DriverKey) -> Option<ScheduledDeadline> {
        let deadline = self.deadlines.remove(&key)?;
        let removed = self
            .deadline_index
            .remove(&DeadlineIndexEntry::new(key, deadline));
        debug_assert!(removed, "deadline map/index drift on removal");
        Some(deadline)
    }

    fn clear_deadlines(&mut self) {
        self.deadlines.clear();
        self.deadline_index.clear();
    }

    fn earliest_deadline(&self) -> Option<(DriverKey, ScheduledDeadline)> {
        self.deadline_index
            .first()
            .copied()
            .map(|entry| (entry.key, entry.deadline()))
    }

    fn pop_wake(&mut self, now: Instant) -> Option<SchedulerWake> {
        if let Some((key, deadline)) = self.earliest_deadline()
            && deadline.at <= now
        {
            return Some(SchedulerWake::Deadline(
                key,
                deadline.block_epoch,
                deadline.operator_token,
            ));
        }
        self.queue.pop_front().map(SchedulerWake::Key)
    }
}

impl DriverKey {
    pub(crate) fn new(finst: Option<(i64, i64)>, driver_id: i32) -> Self {
        Self { finst, driver_id }
    }
}

#[derive(Clone)]
struct DispatcherRegistration {
    id: u64,
    state: Weak<EventDispatcherState>,
    scheduler: Weak<EventScheduler>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DispatcherDeadlineEntry {
    at: Instant,
    scheduler_id: u64,
    generation: u64,
}

struct DispatcherDeadline {
    entry: DispatcherDeadlineEntry,
    scheduler: Weak<EventScheduler>,
}

struct DispatcherReady {
    scheduler_id: u64,
    scheduler: Weak<EventScheduler>,
}

struct EventDispatcherQueues {
    registered: HashMap<u64, Weak<EventScheduler>>,
    ready: VecDeque<DispatcherReady>,
    ready_pending: HashSet<u64>,
    deadlines: HashMap<u64, DispatcherDeadline>,
    deadline_index: BTreeSet<DispatcherDeadlineEntry>,
}

impl EventDispatcherQueues {
    fn new() -> Self {
        Self {
            registered: HashMap::new(),
            ready: VecDeque::new(),
            ready_pending: HashSet::new(),
            deadlines: HashMap::new(),
            deadline_index: BTreeSet::new(),
        }
    }

    fn enqueue(&mut self, scheduler_id: u64, scheduler: Weak<EventScheduler>) -> bool {
        if !self.registered.contains_key(&scheduler_id) || !self.ready_pending.insert(scheduler_id)
        {
            return false;
        }
        self.ready.push_back(DispatcherReady {
            scheduler_id,
            scheduler,
        });
        true
    }

    fn remove_deadline(&mut self, scheduler_id: u64) {
        if let Some(previous) = self.deadlines.remove(&scheduler_id) {
            let removed = self.deadline_index.remove(&previous.entry);
            debug_assert!(removed, "dispatcher deadline map/index drift on removal");
        }
    }

    fn update_deadline(
        &mut self,
        scheduler_id: u64,
        scheduler: Weak<EventScheduler>,
        generation: u64,
        deadline: Option<Instant>,
    ) {
        if !self.registered.contains_key(&scheduler_id) {
            return;
        }
        if self
            .deadlines
            .get(&scheduler_id)
            .is_some_and(|current| current.entry.generation > generation)
        {
            return;
        }
        self.remove_deadline(scheduler_id);
        let Some(at) = deadline else {
            return;
        };
        let entry = DispatcherDeadlineEntry {
            at,
            scheduler_id,
            generation,
        };
        let inserted = self.deadline_index.insert(entry);
        debug_assert!(inserted, "duplicate dispatcher deadline entry");
        self.deadlines
            .insert(scheduler_id, DispatcherDeadline { entry, scheduler });
    }

    fn unregister(&mut self, scheduler_id: u64) {
        self.registered.remove(&scheduler_id);
        self.ready_pending.remove(&scheduler_id);
        self.remove_deadline(scheduler_id);
    }

    fn pop_dispatchable(&mut self, now: Instant) -> Option<DispatcherReady> {
        if let Some(deadline) = self.deadline_index.first().copied()
            && deadline.at <= now
        {
            let scheduled = self
                .deadlines
                .remove(&deadline.scheduler_id)
                .expect("deadline index must have a scheduler");
            let removed = self.deadline_index.remove(&deadline);
            debug_assert!(removed, "dispatcher deadline index drift on dispatch");
            return Some(DispatcherReady {
                scheduler_id: deadline.scheduler_id,
                scheduler: scheduled.scheduler,
            });
        }
        let ready = self.ready.pop_front()?;
        self.ready_pending.remove(&ready.scheduler_id);
        self.remove_deadline(ready.scheduler_id);
        Some(ready)
    }
}

struct EventDispatcherState {
    queues: Mutex<EventDispatcherQueues>,
    wake_cv: Condvar,
    shutdown: AtomicBool,
    next_scheduler_id: AtomicU64,
    #[cfg(test)]
    thread_exited: Arc<AtomicBool>,
}

impl EventDispatcherState {
    fn register(self: &Arc<Self>, scheduler: &Arc<EventScheduler>) -> DispatcherRegistration {
        let id = self
            .next_scheduler_id
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let mut queues = self.queues.lock().expect("event dispatcher queue lock");
        if !self.shutdown.load(Ordering::Acquire) {
            queues.registered.insert(id, Arc::downgrade(scheduler));
        }
        DispatcherRegistration {
            id,
            state: Arc::downgrade(self),
            scheduler: Arc::downgrade(scheduler),
        }
    }

    fn notify_scheduler(&self, scheduler_id: u64, scheduler: Weak<EventScheduler>) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let mut queues = self.queues.lock().expect("event dispatcher queue lock");
        if queues.enqueue(scheduler_id, scheduler) {
            self.wake_cv.notify_one();
        }
    }

    fn unregister(&self, scheduler_id: u64) {
        let mut queues = self.queues.lock().expect("event dispatcher queue lock");
        queues.unregister(scheduler_id);
        self.wake_cv.notify_one();
    }

    fn registered_schedulers(&self) -> Vec<Arc<EventScheduler>> {
        self.queues
            .lock()
            .expect("event dispatcher queue lock")
            .registered
            .values()
            .filter_map(Weak::upgrade)
            .collect()
    }

    fn close_and_drain(&self) -> Vec<DriverTask> {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return Vec::new();
        }
        let schedulers = {
            let mut queues = self.queues.lock().expect("event dispatcher queue lock");
            let schedulers = queues.registered.values().cloned().collect::<Vec<_>>();
            queues.registered.clear();
            queues.ready.clear();
            queues.ready_pending.clear();
            queues.deadlines.clear();
            queues.deadline_index.clear();
            schedulers
        };
        self.wake_cv.notify_all();
        schedulers
            .into_iter()
            .filter_map(|scheduler| scheduler.upgrade())
            .flat_map(|scheduler| scheduler.shutdown_and_take_blocked())
            .collect()
    }
}

/// The dispatcher thread together with the duty to close it, handed to the
/// executor reaper so the dispatcher outlives every driver that may still
/// wait on it.
pub(crate) struct EventDispatcherCloser {
    state: Arc<EventDispatcherState>,
    thread: thread::JoinHandle<()>,
}

impl EventDispatcherCloser {
    /// Closes the dispatcher, joins its thread and returns drivers that were
    /// still parked on it.
    pub(crate) fn close_and_join(self) -> Vec<DriverTask> {
        let tasks = self.state.close_and_drain();
        let _ = self.thread.join();
        tasks
    }
}

/// One process-runtime scheduler thread for every fragment's blocked-driver
/// events. Fragment schedulers retain their own state but never own a thread.
pub(crate) struct EventDispatcher {
    state: Arc<EventDispatcherState>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl EventDispatcher {
    pub(crate) fn new() -> Self {
        let state = Arc::new(EventDispatcherState {
            queues: Mutex::new(EventDispatcherQueues::new()),
            wake_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            next_scheduler_id: AtomicU64::new(0),
            #[cfg(test)]
            thread_exited: Arc::new(AtomicBool::new(false)),
        });
        let run_state = Arc::clone(&state);
        let thread = thread::Builder::new()
            .name("event_dispatcher".to_string())
            .spawn(move || run_dispatcher(run_state))
            .expect("spawn shared event dispatcher thread");
        Self {
            state,
            thread: Mutex::new(Some(thread)),
        }
    }

    fn register(&self, scheduler: &Arc<EventScheduler>) -> DispatcherRegistration {
        self.state.register(scheduler)
    }

    #[cfg(test)]
    fn registered_scheduler_count(&self) -> usize {
        self.state
            .queues
            .lock()
            .expect("event dispatcher queue lock")
            .registered
            .len()
    }

    #[cfg(test)]
    pub(crate) fn close_and_drain(&self) -> Vec<DriverTask> {
        self.state.close_and_drain()
    }

    /// Wakes every parked driver of every attached fragment once, so each
    /// observes a state change on a worker.
    pub(crate) fn wake_all_blocked(&self) {
        for scheduler in self.state.registered_schedulers() {
            scheduler.enqueue_all_blocked();
        }
    }

    /// Hands the dispatcher thread and its closing to the caller; `None` once
    /// they were handed out.
    pub(crate) fn take_closer(&self) -> Option<EventDispatcherCloser> {
        let thread = self
            .thread
            .lock()
            .expect("event dispatcher thread lock")
            .take()?;
        Some(EventDispatcherCloser {
            state: Arc::clone(&self.state),
            thread,
        })
    }

    /// Closes and joins the dispatcher unless its closer was handed out, in
    /// which case closing belongs to whoever took it.
    pub(crate) fn shutdown_and_join(&self) {
        if let Some(closer) = self.take_closer() {
            let _ = closer.close_and_join();
        }
    }

    #[cfg(test)]
    pub(crate) fn exit_probe(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.state.thread_exited)
    }
}

impl Drop for EventDispatcher {
    fn drop(&mut self) {
        self.shutdown_and_join();
    }
}

fn run_dispatcher(state: Arc<EventDispatcherState>) {
    #[cfg(test)]
    let _exit_guard = ThreadExitGuard(&state.thread_exited);
    loop {
        let ready = {
            let mut queues = state.queues.lock().expect("event dispatcher queue lock");
            loop {
                if state.shutdown.load(Ordering::Acquire) {
                    return;
                }
                if let Some(ready) = queues.pop_dispatchable(Instant::now()) {
                    break ready;
                }
                let Some(deadline) = queues.deadline_index.first().copied() else {
                    queues = state
                        .wake_cv
                        .wait(queues)
                        .unwrap_or_else(|error| error.into_inner());
                    continue;
                };
                let (next, _) = state
                    .wake_cv
                    .wait_timeout(
                        queues,
                        deadline.at.saturating_duration_since(Instant::now()),
                    )
                    .unwrap_or_else(|error| error.into_inner());
                queues = next;
            }
        };

        let Some(scheduler) = ready.scheduler.upgrade() else {
            state
                .queues
                .lock()
                .expect("event dispatcher queue lock")
                .unregister(ready.scheduler_id);
            continue;
        };
        let progressed = scheduler.dispatch_one();
        let (deadline_generation, deadline) = scheduler.deadline_snapshot();
        let mut queues = state.queues.lock().expect("event dispatcher queue lock");
        queues.update_deadline(
            ready.scheduler_id,
            Arc::downgrade(&scheduler),
            deadline_generation,
            deadline,
        );
        if progressed {
            queues.enqueue(ready.scheduler_id, Arc::downgrade(&scheduler));
            state.wake_cv.notify_one();
        }
    }
}

#[cfg(test)]
struct ThreadExitGuard<'a>(&'a AtomicBool);

#[cfg(test)]
impl Drop for ThreadExitGuard<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Event scheduler that returns notified blocked drivers to executor workers.
pub(crate) struct EventScheduler {
    shared: OnceLock<Arc<ExecutorShared>>,
    dispatcher: OnceLock<DispatcherRegistration>,
    blocked: Mutex<HashMap<DriverKey, BlockedTask>>,
    reschedule_queue: Mutex<RescheduleState>,
    shutdown: AtomicBool,
    next_block_epoch: AtomicU64,
    deadline_generation: AtomicU64,
    #[cfg(test)]
    dispatch_invocations: AtomicU64,
}

impl EventScheduler {
    pub(crate) fn new() -> Self {
        Self {
            shared: OnceLock::new(),
            dispatcher: OnceLock::new(),
            blocked: Mutex::new(HashMap::new()),
            reschedule_queue: Mutex::new(RescheduleState::new()),
            shutdown: AtomicBool::new(false),
            next_block_epoch: AtomicU64::new(0),
            deadline_generation: AtomicU64::new(0),
            #[cfg(test)]
            dispatch_invocations: AtomicU64::new(0),
        }
    }

    pub(crate) fn attach_executor(
        self: &Arc<Self>,
        shared: Arc<ExecutorShared>,
        dispatcher: &EventDispatcher,
    ) {
        let _ = self.shared.set(shared);
        if self.dispatcher.get().is_none() {
            let registration = dispatcher.register(self);
            if let Err(registration) = self.dispatcher.set(registration)
                && let Some(dispatcher) = registration.state.upgrade()
            {
                dispatcher.unregister(registration.id);
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.shutdown_and_take_blocked();
    }

    pub(crate) fn shutdown_and_take_blocked(&self) -> Vec<DriverTask> {
        let mut blocked_guard = self.blocked.lock().expect("event scheduler blocked lock");
        let mut queue_guard = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return Vec::new();
        }
        queue_guard.clear_deadlines();
        self.deadline_generation.fetch_add(1, Ordering::AcqRel);
        queue_guard.queue.clear();
        queue_guard.pending.clear();
        drop(queue_guard);
        let tasks = blocked_guard
            .drain()
            .map(|(_, blocked)| blocked.task)
            .collect();
        drop(blocked_guard);
        if let Some(registration) = self.dispatcher.get()
            && let Some(dispatcher) = registration.state.upgrade()
        {
            dispatcher.unregister(registration.id);
        }
        tasks
    }

    pub(crate) fn enqueue(&self, key: DriverKey) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let mut state = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        if state.enqueue(key) {
            drop(state);
            self.notify_dispatcher();
        }
    }

    pub(crate) fn enqueue_observable(
        &self,
        key: DriverKey,
        observable: &Weak<Observable>,
        generation: u64,
    ) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        // Always acquire blocked before reschedule. The identity/generation
        // check and queue admission are one atomic scheduler decision, so a
        // delayed callback cannot wake a later block of the same driver key.
        let blocked = self.blocked.lock().expect("event scheduler blocked lock");
        let Some(entry) = blocked.get(&key) else {
            return;
        };
        let newer_than = |recorded: &Option<(Weak<Observable>, u64)>| {
            recorded
                .as_ref()
                .is_some_and(|(current, blocked_generation)| {
                    Weak::ptr_eq(current, observable) && generation > *blocked_generation
                })
        };
        if !newer_than(&entry.observable) && !newer_than(&entry.terminal) {
            // A delayed callback for a transition the parked driver already
            // observed before it parked.
            observe_stale_dispatch(StaleDispatchEvent::Notification);
            return;
        }
        let mut state = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        if state.enqueue(key) {
            drop(state);
            self.notify_dispatcher();
        }
    }

    pub(crate) fn enqueue_all_blocked(&self) {
        let blocked = self.blocked.lock().expect("event scheduler blocked lock");
        if blocked.is_empty() {
            return;
        }
        let mut state = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        for key in blocked.keys().copied() {
            state.enqueue(key);
        }
        drop(state);
        self.notify_dispatcher();
    }

    pub(crate) fn add_blocked(
        self: &Arc<Self>,
        task: DriverTask,
        reason: BlockedReason,
    ) -> Result<(), Box<DriverTask>> {
        match reason {
            BlockedReason::InputEmpty | BlockedReason::OutputFull => {
                let Some((observable, generation, deadline)) = task.blocked_observable_snapshot()
                else {
                    if should_sample_log(&EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT) {
                        debug!(
                            "EventScheduler add_blocked: missing frozen observable; reason={:?} finst={:?} driver_id={}",
                            reason,
                            task.fragment_instance_id(),
                            task.driver_id()
                        );
                    }
                    return Err(Box::new(task));
                };
                if should_sample_log(&EVENT_SCHEDULER_BLOCKED_ADD_LOG_COUNT) {
                    debug!(
                        "EventScheduler add_blocked: reason={:?} finst={:?} driver_id={} generation={} observers_before={}",
                        reason,
                        task.fragment_instance_id(),
                        task.driver_id(),
                        generation,
                        observable.num_observers()
                    );
                }
                self.park_blocked(task, reason, observable, generation, deadline)
            }
            BlockedReason::Dependency(dep) => self.park_blocked_with_dependency(task, dep),
        }
    }

    /// Whether the executor stopped admitting drivers. Read under the blocked
    /// lock by every parking path: the executor closes admission before it
    /// wakes all parked drivers, so a driver parking concurrently either sees
    /// the closure here or is woken by that pass.
    fn executor_closing(&self) -> bool {
        self.shared
            .get()
            .is_some_and(|shared| shared.admission_closed.load(Ordering::Acquire))
    }

    /// Parks a driver whose asynchronous owners still hold finish work until
    /// one of their finish watches fires or a requested recheck is due.
    ///
    /// The pending question is asked here, on the worker, before any
    /// scheduler lock is taken. A driver whose work ended meanwhile goes
    /// straight back to a worker to complete.
    pub(crate) fn add_pending_finish(
        self: &Arc<Self>,
        slot: &mut Option<DriverTask>,
    ) -> Result<bool, crate::runtime::observable::ObservableCapacityError> {
        let task = slot.as_mut().expect("original pending-finish slot");
        let Some(wait) = task.refresh_pending_finish_wait()? else {
            // No foreign observation follows this check before the pure move.
            let Some(shared) = self.shared.get() else {
                return Ok(false);
            };
            let mut queue = shared.queue.lock().expect("global executor queue lock");
            let mut task = slot.take().expect("one ready handoff");
            task.set_ready();
            task.mark_queued();
            queue.push_back(task);
            shared.cv.notify_one();
            return Ok(true);
        };
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let block_epoch = self
            .next_block_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        self.register_finish_observer(task, Arc::clone(&wait.observable))?;
        let terminal = if task.operator_terminal_signal_delivered() {
            None
        } else {
            task.blocked_terminal_snapshot()
                .filter(|(terminal, _)| !Arc::ptr_eq(terminal, &wait.observable))
        };
        if let Some((terminal, _)) = terminal.as_ref() {
            self.register_terminal_observer(task, Arc::clone(terminal));
        }
        let wake_now = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            if self.shutdown.load(Ordering::Acquire) || blocked.contains_key(&key) {
                return Ok(false);
            }
            // Every preceding call only borrowed the original slot. The map
            // becomes its unique owner at this one pure move.
            let task = slot.take().expect("one blocked handoff");
            blocked.insert(
                key,
                BlockedTask {
                    task,
                    observable: Some((Arc::downgrade(&wait.observable), wait.generation)),
                    terminal: terminal
                        .as_ref()
                        .map(|(terminal, generation)| (Arc::downgrade(terminal), *generation)),
                    block_epoch,
                },
            );
            let entry = blocked.get(&key).expect("blocked task was just inserted");
            entry.task.set_in_blocked(true);
            // A driver whose operators have not heard of an abort yet needs a
            // turn to deliver it. One that already did only waits for its
            // owners: another abort turn would change nothing.
            let wake_now = (entry.task.should_abort_immediately() || self.executor_closing())
                && !entry.task.operator_terminal_signal_delivered();
            if let Some(at) = wait.recheck_at {
                let mut state = self
                    .reschedule_queue
                    .lock()
                    .expect("event scheduler queue lock");
                state.install_deadline(
                    key,
                    ScheduledDeadline {
                        at,
                        operator_token: PENDING_FINISH_RECHECK_TOKEN,
                        block_epoch,
                    },
                );
                self.deadline_generation.fetch_add(1, Ordering::AcqRel);
                drop(state);
                self.notify_dispatcher();
            }
            wake_now
        };
        if wake_now {
            self.enqueue(key);
        } else {
            // A finish published between the question and parking.
            self.enqueue_observable(
                key,
                &Arc::downgrade(&wait.observable),
                wait.observable.generation(),
            );
            // Both watches can fire before the task enters the blocked map.
            // Rechecking their generations closes both registration races.
            if let Some((terminal, _)) = terminal.as_ref() {
                self.enqueue_observable(key, &Arc::downgrade(terminal), terminal.generation());
            }
        }
        Ok(true)
    }

    fn park_blocked(
        self: &Arc<Self>,
        task: DriverTask,
        reason: BlockedReason,
        observable: Arc<Observable>,
        generation: u64,
        deadline: Option<DriverBlockDeadline>,
    ) -> Result<(), Box<DriverTask>> {
        #[cfg(novarocks_dev_controls)]
        let test_observer = task.exchange_test_observer();
        #[cfg(novarocks_dev_controls)]
        let emit_exchange_marker = test_observer
            .as_ref()
            .is_some_and(|observer| observer.is_active())
            && matches!(reason, BlockedReason::InputEmpty)
            && task.source_name().contains("EXCHANGE");
        #[cfg(novarocks_dev_controls)]
        let exchange_source_name = emit_exchange_marker.then(|| task.source_name().to_owned());
        #[cfg(novarocks_dev_controls)]
        let writer_diagnostic = task.writer_diagnostic();
        #[cfg(novarocks_dev_controls)]
        let mark_probe_dependency = task.mark_probe_dependency();
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let block_epoch = self
            .next_block_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        self.register_observer(&task, &reason, Arc::clone(&observable));
        let terminal = task
            .blocked_terminal_snapshot()
            .filter(|(terminal, _)| !Arc::ptr_eq(terminal, &observable));
        if let Some((terminal, _)) = terminal.as_ref() {
            self.register_terminal_observer(&task, Arc::clone(terminal));
        }
        let aborted = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            if self.shutdown.load(Ordering::Acquire) {
                return Err(Box::new(task));
            }
            blocked.insert(
                key,
                BlockedTask {
                    task,
                    observable: Some((Arc::downgrade(&observable), generation)),
                    terminal: terminal
                        .as_ref()
                        .map(|(terminal, generation)| (Arc::downgrade(terminal), *generation)),
                    block_epoch,
                },
            );
            let entry = blocked.get(&key).expect("blocked task was just inserted");
            entry.task.set_in_blocked(true);
            let aborted = entry.task.should_abort_immediately() || self.executor_closing();
            if let Some(deadline) = deadline {
                let mut state = self
                    .reschedule_queue
                    .lock()
                    .expect("event scheduler queue lock");
                state.install_deadline(
                    key,
                    ScheduledDeadline {
                        at: deadline.at(),
                        operator_token: deadline.token(),
                        block_epoch,
                    },
                );
                self.deadline_generation.fetch_add(1, Ordering::AcqRel);
                drop(state);
                self.notify_dispatcher();
            }
            aborted
        };

        #[cfg(novarocks_dev_controls)]
        if emit_exchange_marker {
            crate::runtime::exchange::emit_exchange_snapshot_marker(
                test_observer.as_deref(),
                || {
                    format!(
                        "event=driver_park finst={:?} driver_id={} reason={:?} frozen_generation={} current_generation={} block_epoch={} source_name={:?}",
                        key.finst,
                        key.driver_id,
                        reason,
                        generation,
                        observable.generation(),
                        block_epoch,
                        exchange_source_name
                            .as_deref()
                            .expect("active Exchange observer"),
                    )
                },
            );
        }

        #[cfg(novarocks_dev_controls)]
        if let Some((scope, source, sink)) = writer_diagnostic {
            scope.emit("driver_park", || format!(
                "finst={:?} driver_id={} source={source:?} sink={sink:?} reason={reason:?} mark_probe_dependency={mark_probe_dependency} frozen_generation={generation} current_generation={} block_epoch={block_epoch}",
                key.finst, key.driver_id, observable.generation()
            ));
        }

        // Registration can race with a transition that already made the task
        // runnable. The generation comparison closes that window without
        // evaluating operator readiness on the scheduler thread.
        if aborted {
            self.enqueue(key);
        } else {
            let current_generation = observable.generation();
            self.enqueue_observable(key, &Arc::downgrade(&observable), current_generation);
            if let Some((terminal, _)) = terminal.as_ref() {
                self.enqueue_observable(key, &Arc::downgrade(terminal), terminal.generation());
            }
        }
        Ok(())
    }

    fn park_blocked_with_dependency(
        self: &Arc<Self>,
        task: DriverTask,
        dep: DependencyHandle,
    ) -> Result<(), Box<DriverTask>> {
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let block_epoch = self
            .next_block_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let terminal = task.blocked_terminal_snapshot();
        if let Some((terminal, _)) = terminal.as_ref() {
            self.register_terminal_observer(&task, Arc::clone(terminal));
        }
        let aborted = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            if self.shutdown.load(Ordering::Acquire) {
                return Err(Box::new(task));
            }
            blocked.insert(
                key,
                BlockedTask {
                    task,
                    observable: None,
                    terminal: terminal
                        .as_ref()
                        .map(|(terminal, generation)| (Arc::downgrade(terminal), *generation)),
                    block_epoch,
                },
            );
            let entry = blocked.get(&key).expect("blocked task was just inserted");
            entry.task.set_in_blocked(true);
            entry.task.should_abort_immediately() || self.executor_closing()
        };
        let scheduler = Arc::downgrade(self);
        let dep_name = dep.name().to_string();
        let dep_id = dep.id();
        dep.add_waiter(Arc::new(move || {
            if dep_name.starts_with("nljoin_build:") {
                debug!(
                    "Dependency waiter fired: dep_id={} name={} finst={:?} driver_id={}",
                    dep_id, dep_name, key.finst, key.driver_id
                );
            }
            if let Some(scheduler) = scheduler.upgrade() {
                scheduler.enqueue(key);
            }
        }));
        if aborted {
            self.enqueue(key);
        } else if let Some((terminal, _)) = terminal.as_ref() {
            self.enqueue_observable(key, &Arc::downgrade(terminal), terminal.generation());
        }
        Ok(())
    }

    /// Watches a driver's finish observable while it waits in `PendingFinish`.
    /// Registration is once per driver and observable identity.
    fn register_finish_observer(
        self: &Arc<Self>,
        task: &DriverTask,
        observable: Arc<Observable>,
    ) -> Result<(), crate::runtime::observable::ObservableCapacityError> {
        if !task.try_mark_finish_observer_registered(&observable) {
            return Ok(());
        }
        let observer = Arc::new(PipelineObserver::new(
            Arc::downgrade(self),
            Arc::downgrade(&observable),
            DriverKey::new(task.fragment_instance_id(), task.driver_id()),
            task.driver_id(),
            task.fragment_instance_id(),
        ));
        self.try_add_observer(observable, observer, ObserverKind::Finish)
    }

    /// Watches a terminal operator's early-finish observable for a parked
    /// driver. Registration is once per driver and observable identity.
    fn register_terminal_observer(
        self: &Arc<Self>,
        task: &DriverTask,
        observable: Arc<Observable>,
    ) {
        if !task.try_mark_sink_observer_registered(&observable) {
            return;
        }
        let observer = Arc::new(PipelineObserver::new(
            Arc::downgrade(self),
            Arc::downgrade(&observable),
            DriverKey::new(task.fragment_instance_id(), task.driver_id()),
            task.driver_id(),
            task.fragment_instance_id(),
        ));
        self.add_observer(observable, observer, ObserverKind::Sink);
    }

    fn register_observer(
        self: &Arc<Self>,
        task: &DriverTask,
        reason: &BlockedReason,
        observable: Arc<Observable>,
    ) {
        let newly_registered = match reason {
            BlockedReason::InputEmpty => task.try_mark_source_observer_registered(&observable),
            BlockedReason::OutputFull => task.try_mark_sink_observer_registered(&observable),
            BlockedReason::Dependency(_) => false,
        };
        if !newly_registered {
            return;
        }
        let observer = Arc::new(PipelineObserver::new(
            Arc::downgrade(self),
            Arc::downgrade(&observable),
            DriverKey::new(task.fragment_instance_id(), task.driver_id()),
            task.driver_id(),
            task.fragment_instance_id(),
        ));
        let kind = match reason {
            BlockedReason::InputEmpty => ObserverKind::Source,
            BlockedReason::OutputFull => ObserverKind::Sink,
            BlockedReason::Dependency(_) => return,
        };
        self.add_observer(observable, observer, kind);
    }

    fn add_observer(
        &self,
        observable: Arc<Observable>,
        observer: Arc<PipelineObserver>,
        kind: ObserverKind,
    ) {
        self.try_add_observer(observable, observer, kind)
            .expect("observable registration capacity");
    }
    fn try_add_observer(
        &self,
        observable: Arc<Observable>,
        observer: Arc<PipelineObserver>,
        kind: ObserverKind,
    ) -> Result<(), crate::runtime::observable::ObservableCapacityError> {
        let callback: Arc<dyn Fn() + Send + Sync + 'static> = match kind {
            ObserverKind::Source => {
                let observer = Arc::clone(&observer);
                Arc::new(move || observer.source_trigger())
            }
            ObserverKind::Sink => {
                let observer = Arc::clone(&observer);
                Arc::new(move || observer.sink_trigger())
            }
            ObserverKind::Finish => {
                let observer = Arc::clone(&observer);
                Arc::new(move || observer.finish_trigger())
            }
        };
        observable.try_add_observer(callback)
    }

    fn notify_dispatcher(&self) {
        if let Some(registration) = self.dispatcher.get()
            && let Some(dispatcher) = registration.state.upgrade()
        {
            dispatcher.notify_scheduler(registration.id, registration.scheduler.clone());
        }
    }

    fn deadline_snapshot(&self) -> (u64, Option<Instant>) {
        let state = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        let generation = self.deadline_generation.load(Ordering::Acquire);
        (
            generation,
            state.earliest_deadline().map(|(_, deadline)| deadline.at),
        )
    }

    fn dispatch_one(self: &Arc<Self>) -> bool {
        #[cfg(test)]
        self.dispatch_invocations.fetch_add(1, Ordering::Relaxed);
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let wake = {
            let mut queue = self
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock");
            queue.pop_wake(Instant::now())
        };
        match wake {
            // Keep `pending` set until `try_schedule_key` removes the blocked
            // task. A callback in this gap is coalesced into the wake-up being
            // delivered.
            Some(SchedulerWake::Key(key)) => self.try_schedule_key(key),
            Some(SchedulerWake::Deadline(key, block_epoch, operator_token)) => {
                self.enqueue_due_deadline(key, block_epoch, operator_token)
            }
            None => return false,
        }
        true
    }

    fn enqueue_due_deadline(&self, key: DriverKey, block_epoch: u64, operator_token: u64) {
        let blocked = self.blocked.lock().expect("event scheduler blocked lock");
        let Some(entry) = blocked.get(&key) else {
            observe_stale_dispatch(StaleDispatchEvent::Deadline);
            return;
        };
        if entry.block_epoch != block_epoch {
            observe_stale_dispatch(StaleDispatchEvent::Deadline);
            return;
        }
        let mut state = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        let Some(deadline) = state.deadlines.get(&key).copied() else {
            observe_stale_dispatch(StaleDispatchEvent::Deadline);
            return;
        };
        if deadline.block_epoch != block_epoch
            || deadline.operator_token != operator_token
            || deadline.at > Instant::now()
        {
            observe_stale_dispatch(StaleDispatchEvent::Deadline);
            return;
        }
        state.remove_deadline(key);
        self.deadline_generation.fetch_add(1, Ordering::AcqRel);
        let origin = WakeOrigin {
            at: deadline.at,
            transition: DispatchTransition::DeadlineToEnqueue,
        };
        if state.enqueue_from(key, origin) {
            drop(state);
            self.notify_dispatcher();
        }
    }

    fn try_schedule_key(self: &Arc<Self>, key: DriverKey) {
        #[cfg(novarocks_dev_controls)]
        let mut exchange_marker = None;
        #[cfg(novarocks_dev_controls)]
        let mut writer_marker = None;
        let task = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            let entry = blocked.remove(&key);
            #[cfg(novarocks_dev_controls)]
            if let Some(entry) = entry.as_ref()
                && let Some(scope) = entry.task.writer_diagnostic()
            {
                let generations = entry
                    .observable
                    .as_ref()
                    .and_then(|(observable, generation)| {
                        observable
                            .upgrade()
                            .map(|observable| (*generation, observable.generation()))
                    });
                writer_marker = Some((scope, generations, entry.block_epoch));
            }
            #[cfg(novarocks_dev_controls)]
            if let Some(entry) = entry.as_ref()
                && entry.task.source_name().contains("EXCHANGE")
                && let Some(observer) = entry
                    .task
                    .exchange_test_observer()
                    .filter(|observer| observer.is_active())
            {
                let (blocked_generation, current_generation) = entry
                    .observable
                    .as_ref()
                    .and_then(|(observable, blocked_generation)| {
                        observable
                            .upgrade()
                            .map(|observable| (*blocked_generation, observable.generation()))
                    })
                    .unwrap_or((0, 0));
                exchange_marker = Some((
                    observer,
                    blocked_generation,
                    current_generation,
                    entry.block_epoch,
                ));
            }
            let mut state = self
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock");
            let origin = state.pending.remove(&key);
            if state.remove_deadline(key).is_some() {
                self.deadline_generation.fetch_add(1, Ordering::AcqRel);
            }
            entry.map(|entry| (entry.task, origin))
        };
        #[cfg(novarocks_dev_controls)]
        if let Some((observer, blocked_generation, current_generation, block_epoch)) =
            exchange_marker
        {
            crate::runtime::exchange::emit_exchange_snapshot_marker(
                Some(observer.as_ref()),
                || {
                    format!(
                        "event=driver_wake finst={:?} driver_id={} blocked_generation={} current_generation={} block_epoch={}",
                        key.finst,
                        key.driver_id,
                        blocked_generation,
                        current_generation,
                        block_epoch,
                    )
                },
            );
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(((scope, source, sink), generations, epoch)) = writer_marker {
            scope.emit("driver_wake", || format!(
                "finst={:?} driver_id={} source={source:?} sink={sink:?} generations={generations:?} block_epoch={epoch}",
                key.finst, key.driver_id
            ));
        }
        let Some((mut task, origin)) = task else {
            return;
        };
        task.set_ready();
        task.set_in_blocked(false);
        self.enqueue_ready(task);
        if let Some(origin) = origin {
            observe_dispatch(origin.transition, origin.at.elapsed());
        }
    }

    fn enqueue_ready(&self, mut task: DriverTask) {
        let Some(shared) = self.shared.get() else {
            debug!("EventScheduler enqueue_ready: executor not attached");
            return;
        };
        task.mark_queued();
        let mut queue = shared.queue.lock().expect("global executor queue lock");
        queue.push_back(task);
        shared.cv.notify_one();
    }
}

enum ObserverKind {
    Source,
    Sink,
    Finish,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::FragmentCompletion;
    use crate::exec::pipeline::input::{PreparedInput, RawInput};
    use crate::exec::pipeline::operator::{FinishWatch, InputEncodingPolicy, Operator};
    use crate::exec::pipeline::shape::{
        SinkKernel, SinkStep, Source, SourceKernel, Transform, TransformStart,
    };
    use crate::exec::pipeline::step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason};
    use crate::exec::pipeline::step_context::StepContext;
    use crate::runtime::runtime_state::RuntimeState;
    use std::sync::atomic::AtomicUsize;

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn exchange_park_and_wake_observe_the_same_actual_nonzero_generation_epoch() {
        #[derive(Debug, Default)]
        struct Events(Mutex<Vec<String>>);
        impl crate::runtime::exchange::TestEventObserver for Events {
            fn is_active(&self) -> bool {
                true
            }
            fn observe(&self, event: &str) {
                self.0.lock().unwrap().push(event.to_owned());
            }
        }
        struct ExchangeSource(Arc<Observable>);
        impl OperatorLifecycle for ExchangeSource {
            fn name(&self) -> &str {
                "EXCHANGE_TEST_SOURCE"
            }
        }
        impl SourceKernel for ExchangeSource {
            fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
                Ok(StepResult::new(
                    None,
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Input,
                        observable: Arc::clone(&self.0),
                        observed_generation: self.0.generation(),
                        deadline: None,
                    }),
                    0,
                ))
            }
        }
        let events = Arc::new(Events::default());
        let observable = Arc::new(Observable::new());
        observable.notify_observers();
        let runtime =
            Arc::new(RuntimeState::default().with_exchange_test_observer(Some(events.clone())));
        let finst = Some((67_900, 65_590));
        let mut driver = PipelineDriver::new(
            9,
            vec![
                Box::new(Source::new(ExchangeSource(Arc::clone(&observable)))),
                Box::new(EarlyFinishSink {
                    finished: Arc::new(AtomicBool::new(false)),
                    early_finish: Arc::new(Observable::new()),
                }),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime),
            finst,
        );
        assert!(matches!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        let (driver_event, first_generation, _) = driver.blocked_observable_snapshot().unwrap();
        assert!(
            !Arc::ptr_eq(&driver_event, &observable),
            "the scheduler parks the driver's frontier endpoint, not its raw source event"
        );
        // The first turn installed the real source-to-frontier subscription.
        // Only a subsequent source notification can advance that endpoint;
        // notifying before construction does not give the driver a generation.
        observable.notify_observers();
        assert!(driver_event.generation() > first_generation);
        assert!(matches!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        let (reparked_event, generation, _) = driver.blocked_observable_snapshot().unwrap();
        assert!(Arc::ptr_eq(&driver_event, &reparked_event));
        assert!(generation > 0);
        assert_eq!(generation, driver_event.generation());
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());
        let task = DriverTask::new(
            driver,
            FragmentCompletion::new(1, runtime.error_state()),
            Arc::new(FragmentContext::new(None, runtime, finst, None, None, None)),
            Duration::from_millis(10),
        );
        assert!(
            scheduler
                .add_blocked(task, BlockedReason::InputEmpty)
                .is_ok()
        );
        let key = DriverKey::new(finst, 9);
        let epoch = scheduler.blocked.lock().unwrap()[&key].block_epoch;
        let parked = events.0.lock().unwrap().clone();
        assert_eq!(parked.len(), 1);
        assert!(parked[0].contains("event=driver_park"));
        assert!(parked[0].contains(&format!("frozen_generation={generation}")));
        assert!(parked[0].contains(&format!("block_epoch={epoch}")));
        observable.notify_observers();
        assert!(driver_event.generation() > generation);
        // Drive the actual queued key through the normal scheduling author.
        let queued = scheduler
            .reschedule_queue
            .lock()
            .unwrap()
            .queue
            .pop_front()
            .unwrap();
        assert_eq!(queued, key);
        scheduler.try_schedule_key(queued);
        assert_eq!(executor.queue.lock().unwrap().len(), 1);
        let completed = events.0.lock().unwrap();
        assert_eq!(completed.len(), 2);
        assert!(completed[1].contains("event=driver_wake"));
        assert!(completed[1].contains(&format!("blocked_generation={generation}")));
        assert!(
            completed[1].contains(&format!("current_generation={}", driver_event.generation()))
        );
        assert!(completed[1].contains(&format!("block_epoch={epoch}")));
    }

    #[test]
    fn deadline_index_replace_remove_and_clear_leave_no_stale_entries() {
        let mut state = RescheduleState::new();
        let first_key = DriverKey::new(Some((1, 2)), 3);
        let second_key = DriverKey::new(Some((4, 5)), 6);
        let first = ScheduledDeadline {
            at: Instant::now() + Duration::from_secs(10),
            operator_token: 1,
            block_epoch: 11,
        };
        let replacement = ScheduledDeadline {
            at: Instant::now() + Duration::from_secs(20),
            operator_token: 2,
            block_epoch: 12,
        };
        let second = ScheduledDeadline {
            at: Instant::now() + Duration::from_secs(5),
            operator_token: 3,
            block_epoch: 13,
        };

        state.install_deadline(first_key, first);
        state.install_deadline(first_key, replacement);
        assert_eq!(state.deadlines.len(), 1);
        assert_eq!(state.deadline_index.len(), 1);
        assert!(
            !state
                .deadline_index
                .contains(&DeadlineIndexEntry::new(first_key, first))
        );
        assert!(
            state
                .deadline_index
                .contains(&DeadlineIndexEntry::new(first_key, replacement))
        );

        state.install_deadline(second_key, second);
        assert_eq!(state.earliest_deadline(), Some((second_key, second)));
        assert_eq!(state.remove_deadline(second_key), Some(second));
        assert_eq!(state.deadlines.len(), 1);
        assert_eq!(state.deadline_index.len(), 1);
        assert_eq!(state.earliest_deadline(), Some((first_key, replacement)));

        state.clear_deadlines();
        assert!(state.deadlines.is_empty());
        assert!(state.deadline_index.is_empty());
        assert!(state.earliest_deadline().is_none());
    }

    // Native scheduling remains manually driven; neither a dispatcher thread nor
    // the FragmentContext's explicit fanout participates in the wake oracle.
    #[derive(Clone, Copy, Debug)]
    enum OwnerStopPark {
        Input,
        Dependency,
        PendingFinish,
    }

    struct OwnerStopSource {
        event: Arc<Observable>,
        reason: WaitReason,
    }
    impl OperatorLifecycle for OwnerStopSource {
        fn name(&self) -> &str {
            "OWNER_STOP_SOURCE"
        }
    }
    impl SourceKernel for OwnerStopSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: self.reason,
                    observable: self.event.clone(),
                    observed_generation: self.event.generation(),
                    deadline: None,
                }),
                0,
            ))
        }
    }
    struct OwnerStopSink {
        pending: Arc<AtomicBool>,
        event: Arc<Observable>,
        terminal_calls: Arc<AtomicUsize>,
    }
    impl OperatorLifecycle for OwnerStopSink {
        fn name(&self) -> &str {
            "OWNER_STOP_SINK"
        }
        fn cancel(&mut self) {
            self.terminal_calls.fetch_add(1, Ordering::AcqRel);
        }
        fn observe_exit(&mut self) -> crate::exec::pipeline::operator::ExitObservation {
            if self.pending.load(Ordering::Acquire) {
                crate::exec::pipeline::operator::ExitObservation::pending(FinishWatch::Notify(
                    self.event.clone(),
                ))
            } else {
                crate::exec::pipeline::operator::ExitObservation::exited()
            }
        }
    }
    impl InputEncodingPolicy for OwnerStopSink {}
    impl SinkKernel for OwnerStopSink {
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<SinkStep> {
            panic!("a stop-only wake fixture must not receive data")
        }
    }

    fn assert_owner_stop_only_wakes_parked_driver(
        owner: Arc<dyn crate::runtime::task_control::TaskExecutionPort>,
        advance: impl FnOnce(),
        deferred: bool,
        park: OwnerStopPark,
        stop_before_parking: bool,
    ) {
        use novarocks_execution_contract::{AbortCause, TaskConclusionProposal, TaskFailurePhase};
        let state = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::for_task(
                Arc::new(
                    crate::runtime::task_control::TaskExecutionControl::new(
                        owner.identity(),
                        owner.clone(),
                        TaskFailurePhase::Execution,
                    )
                    .unwrap(),
                ),
            )),
        ));
        let data_event = Arc::new(Observable::new());
        let finish_event = Arc::new(Observable::new());
        let pending = Arc::new(AtomicBool::new(matches!(
            park,
            OwnerStopPark::PendingFinish
        )));
        let terminal_calls = Arc::new(AtomicUsize::new(0));
        let source: Box<dyn Operator> = if matches!(park, OwnerStopPark::PendingFinish) {
            Box::new(Source::new(DrainedSource))
        } else {
            Box::new(Source::new(OwnerStopSource {
                event: data_event.clone(),
                reason: if matches!(park, OwnerStopPark::Dependency) {
                    WaitReason::Dependency
                } else {
                    WaitReason::Input
                },
            }))
        };
        let driver = PipelineDriver::new(
            82,
            vec![
                source,
                Box::new(Transform::hydrating(OwnerStopSink {
                    pending: pending.clone(),
                    event: finish_event.clone(),
                    terminal_calls: terminal_calls.clone(),
                })),
            ],
            None,
            Vec::new(),
            state.clone(),
            Some((91_082, 91_083)),
        );
        let completion = FragmentCompletion::new(1, state.error_state());
        let context = Arc::new(FragmentContext::new(
            None,
            state,
            Some((91_082, 91_083)),
            None,
            None,
            None,
        ));
        let mut task = DriverTask::new(
            driver,
            completion.clone(),
            context,
            Duration::from_millis(10),
        );
        let blocked = match task.process_for_test(Duration::from_secs(1)) {
            DriverState::PendingFinish if matches!(park, OwnerStopPark::PendingFinish) => None,
            DriverState::Blocked(reason) if !matches!(park, OwnerStopPark::PendingFinish) => {
                Some(reason)
            }
            state => panic!("the driver must park before the owner stops: {state:?}"),
        };
        assert!(!task.operator_terminal_signal_delivered());
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(executor.clone()).is_ok());
        let wakes = || scheduler.reschedule_queue.lock().unwrap().queue.len();
        let park_task = |task: DriverTask| {
            if let Some(reason) = blocked {
                assert!(scheduler.add_blocked(task, reason).is_ok());
            } else {
                let mut slot = Some(task);
                assert!(scheduler.add_pending_finish(&mut slot).unwrap());
                assert!(slot.is_none());
            }
        };
        // Before-parking checks the generation/initial-state race; after-parking
        // proves an already parked task receives a later stop publication.
        if stop_before_parking {
            owner.submit(TaskConclusionProposal::Aborted(AbortCause::QueryFailed));
            if deferred {
                assert!(!owner.is_stopped());
            }
            advance();
            park_task(task);
        } else {
            park_task(task);
            assert_eq!(wakes(), 0);
            owner.submit(TaskConclusionProposal::Aborted(AbortCause::QueryFailed));
            if deferred {
                assert!(!owner.is_stopped());
                assert_eq!(wakes(), 0, "handoff itself has no wake authority");
            }
            advance();
        }
        assert!(owner.is_stopped());
        assert_eq!(data_event.generation(), 0);
        assert_eq!(
            finish_event.generation(),
            0,
            "the actual async owner did not end"
        );
        assert_eq!(
            wakes(),
            1,
            "only the accepted stop made the driver runnable"
        );
        let key = scheduler
            .reschedule_queue
            .lock()
            .unwrap()
            .queue
            .pop_front()
            .unwrap();
        scheduler.try_schedule_key(key);
        let mut task = executor.queue.lock().unwrap().pop_front().unwrap();
        let state = task.process_for_test(Duration::from_secs(1));
        assert!(task.operator_terminal_signal_delivered());
        assert_eq!(terminal_calls.load(Ordering::Acquire), 1);
        if matches!(park, OwnerStopPark::PendingFinish) {
            assert!(matches!(state, DriverState::PendingFinish));
            assert!(completion.stopped_fact().is_none());
            let mut slot = Some(task);
            assert!(scheduler.add_pending_finish(&mut slot).unwrap());
            assert!(slot.is_none());
            assert_eq!(wakes(), 0, "stop is not a proof of the async owner's exit");
            pending.store(false, Ordering::Release);
            finish_event.notify_observers();
            assert_eq!(wakes(), 1);
            let key = scheduler
                .reschedule_queue
                .lock()
                .unwrap()
                .queue
                .pop_front()
                .unwrap();
            scheduler.try_schedule_key(key);
            task = executor.queue.lock().unwrap().pop_front().unwrap();
        } else {
            assert!(matches!(state, DriverState::Failed(_)));
        }
        assert!(
            task.finish_due_to_abort().is_none(),
            "actual exit converges the original driver"
        );
        assert!(completion.stopped_fact().is_some());
        assert!(scheduler.blocked.lock().unwrap().is_empty());
        assert!(executor.queue.lock().unwrap().is_empty());
        assert_eq!(terminal_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn a_pending_finish_driver_wakes_for_a_later_stop() {
        let owner = crate::runtime::task_control::test_support::Owner::new();
        assert_owner_stop_only_wakes_parked_driver(
            owner,
            || {},
            false,
            OwnerStopPark::PendingFinish,
            false,
        );
    }

    #[test]
    fn a_pending_finish_driver_does_not_lose_a_stop_before_parking() {
        let owner = crate::runtime::task_control::test_support::Owner::new();
        assert_owner_stop_only_wakes_parked_driver(
            owner,
            || {},
            false,
            OwnerStopPark::PendingFinish,
            true,
        );
    }

    #[test]
    fn deferred_parked_drivers_wake_on_stop_notification_alone() {
        for park in [
            OwnerStopPark::Input,
            OwnerStopPark::Dependency,
            OwnerStopPark::PendingFinish,
        ] {
            let owner = crate::runtime::task_control::test_support::DeferredOwner::new();
            let pump = owner.clone();
            assert_owner_stop_only_wakes_parked_driver(
                owner,
                move || pump.advance_all(),
                true,
                park,
                false,
            );
        }
    }
    struct FinishTransitionSink {
        finished: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }

    struct ControlledDynamicSource {
        ready: Arc<AtomicBool>,
        poll_forbidden: Arc<AtomicBool>,
        use_second_observable: Arc<AtomicBool>,
        first_observable: Arc<Observable>,
        second_observable: Arc<Observable>,
    }

    impl OperatorLifecycle for ControlledDynamicSource {
        fn name(&self) -> &str {
            "CONTROLLED_DYNAMIC_SOURCE"
        }
    }

    impl SourceKernel for ControlledDynamicSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            assert!(
                !self.poll_forbidden.load(Ordering::Acquire),
                "scheduler thread must not poll source readiness"
            );
            let event = if self.use_second_observable.load(Ordering::Acquire) {
                self.second_observable.clone()
            } else {
                self.first_observable.clone()
            };
            let generation = event.generation();
            Ok(if self.ready.load(Ordering::Acquire) {
                StepResult::new(Some(one_row_chunk()), NextStep::Runnable, 1)
            } else {
                StepResult::new(
                    None,
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Input,
                        observable: event,
                        observed_generation: generation,
                        deadline: None,
                    }),
                    0,
                )
            })
        }
    }

    struct ControlledTerminalSink {
        ready: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }

    struct DeadlineSource {
        ready: Arc<AtomicBool>,
        observable: Arc<Observable>,
        deadline: Arc<Mutex<Option<DriverBlockDeadline>>>,
    }

    impl OperatorLifecycle for DeadlineSource {
        fn name(&self) -> &str {
            "DEADLINE_SOURCE"
        }
    }

    impl SourceKernel for DeadlineSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let generation = self.observable.generation();
            Ok(if self.ready.load(Ordering::Acquire) {
                StepResult::new(Some(one_row_chunk()), NextStep::Runnable, 1)
            } else {
                StepResult::new(
                    None,
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Input,
                        observable: self.observable.clone(),
                        observed_generation: generation,
                        deadline: *self.deadline.lock().expect("test deadline lock"),
                    }),
                    0,
                )
            })
        }
    }

    impl OperatorLifecycle for ControlledTerminalSink {
        fn name(&self) -> &str {
            "CONTROLLED_TERMINAL_SINK"
        }
    }

    impl InputEncodingPolicy for ControlledTerminalSink {}
    impl SinkKernel for ControlledTerminalSink {
        fn admission(
            &self,
            _: &mut StepContext<'_>,
            _: &RawInput,
        ) -> ExecutionResult<Option<AdmissionNext>> {
            let generation = self.observable.generation();
            Ok((!self.ready.load(Ordering::Acquire)).then(|| {
                AdmissionNext::Wait(WaitFact {
                    reason: WaitReason::Output,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                })
            }))
        }
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            Ok(SinkStep::new(NextStep::NeedInput, chunk.len() as u64))
        }
    }

    impl OperatorLifecycle for FinishTransitionSink {
        fn name(&self) -> &str {
            "FINISH_TRANSITION_SINK"
        }
    }

    impl InputEncodingPolicy for FinishTransitionSink {}
    impl SinkKernel for FinishTransitionSink {
        fn start(&self) -> TransformStart {
            TransformStart::Ready
        }
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<SinkStep> {
            panic!("finish transition fixture owns no input edge")
        }
        fn poll(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
            let generation = self.observable.generation();
            Ok(SinkStep::new(
                if self.finished.load(Ordering::Acquire) {
                    NextStep::Finished
                } else {
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Output,
                        observable: self.observable.clone(),
                        observed_generation: generation,
                        deadline: None,
                    })
                },
                0,
            ))
        }
    }

    #[test]
    fn output_full_task_that_finishes_before_parking_is_rechecked() {
        let finished = Arc::new(AtomicBool::new(false));
        let observable = Arc::new(Observable::new());
        let runtime_state = Arc::new(RuntimeState::default());
        let mut driver = PipelineDriver::new(
            3,
            vec![Box::new(Transform::hydrating(FinishTransitionSink {
                finished: Arc::clone(&finished),
                observable: Arc::clone(&observable),
            }))],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((67_100, 65_538)),
        );

        assert!(matches!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::OutputFull)
        ));
        finished.store(true, Ordering::Release);
        observable.notify_observers();

        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            runtime_state,
            Some((67_100, 65_538)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, fragment_ctx.runtime_state().error_state());
        let task = DriverTask::new(driver, completion, fragment_ctx, Duration::from_millis(10));
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());

        assert!(
            scheduler
                .add_blocked(task, BlockedReason::OutputFull)
                .is_ok(),
            "a sink that became ready must be rechecked instead of rejected"
        );

        let key = scheduler
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock")
            .queue
            .pop_front()
            .expect("generation change must close the pre-registration wake-up race");
        scheduler.try_schedule_key(key);
        assert_eq!(
            executor
                .queue
                .lock()
                .expect("global executor queue lock")
                .len(),
            1
        );
    }

    /// An independently signaled demand withdrawal. No data step is needed
    /// to learn it, and scheduler notification callbacks cannot poll it.
    struct EarlyFinishSink {
        finished: Arc<AtomicBool>,
        early_finish: Arc<Observable>,
    }
    impl OperatorLifecycle for EarlyFinishSink {
        fn name(&self) -> &str {
            "EARLY_FINISH_SINK"
        }
    }
    impl crate::exec::pipeline::shape::StepOperator for EarlyFinishSink {
        fn initial_step(&self) -> crate::exec::pipeline::step::NextStep {
            crate::exec::pipeline::step::NextStep::NeedInput
        }
        fn poll_control(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::shape::ControlStep> {
            Ok(if self.finished.load(Ordering::Acquire) {
                crate::exec::pipeline::shape::ControlStep::Withdrawn
            } else {
                crate::exec::pipeline::shape::ControlStep::Active
            })
        }
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.early_finish.clone())
        }
        fn try_push(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            _: crate::exec::pipeline::input::RawInput,
        ) -> crate::exec::pipeline::step::InputResult {
            panic!("control-only fixture must not receive input")
        }
        fn drive(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            panic!("control withdrawal must not need a data step")
        }
        fn finish(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<()> {
            Ok(())
        }
    }

    struct EarlyFinishHarness {
        scheduler: Arc<EventScheduler>,
        executor: Arc<ExecutorShared>,
        finished: Arc<AtomicBool>,
        source_observable: Arc<Observable>,
        early_finish: Arc<Observable>,
        driver: Option<PipelineDriver>,
        runtime_state: Arc<RuntimeState>,
    }

    fn input_empty_driver_with_early_finish_sink() -> EarlyFinishHarness {
        let finished = Arc::new(AtomicBool::new(false));
        let source_observable = Arc::new(Observable::new());
        let early_finish = Arc::new(Observable::new());
        let runtime_state = Arc::new(RuntimeState::default());
        let mut driver = PipelineDriver::new(
            5,
            vec![
                Box::new(Source::new(ControlledDynamicSource {
                    ready: Arc::new(AtomicBool::new(false)),
                    poll_forbidden: Arc::new(AtomicBool::new(false)),
                    use_second_observable: Arc::new(AtomicBool::new(false)),
                    first_observable: Arc::clone(&source_observable),
                    second_observable: Arc::new(Observable::new()),
                })),
                Box::new(EarlyFinishSink {
                    finished: Arc::clone(&finished),
                    early_finish: Arc::clone(&early_finish),
                }),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((67_300, 65_540)),
        );
        assert!(matches!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());
        EarlyFinishHarness {
            scheduler,
            executor,
            finished,
            source_observable,
            early_finish,
            driver: Some(driver),
            runtime_state,
        }
    }

    impl EarlyFinishHarness {
        fn park(&mut self) {
            let fragment_ctx = Arc::new(FragmentContext::new(
                None,
                Arc::clone(&self.runtime_state),
                Some((67_300, 65_540)),
                None,
                None,
                None,
            ));
            let task = DriverTask::new(
                self.driver.take().expect("driver"),
                FragmentCompletion::new(1, fragment_ctx.runtime_state().error_state()),
                fragment_ctx,
                Duration::from_millis(10),
            );
            assert!(
                self.scheduler
                    .add_blocked(task, BlockedReason::InputEmpty)
                    .is_ok()
            );
        }

        fn queued_wakes(&self) -> usize {
            self.scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .len()
        }

        fn deliver_wakes(&self) -> usize {
            loop {
                let key = self
                    .scheduler
                    .reschedule_queue
                    .lock()
                    .expect("event scheduler queue lock")
                    .queue
                    .pop_front();
                let Some(key) = key else {
                    break;
                };
                self.scheduler.try_schedule_key(key);
            }
            self.executor
                .queue
                .lock()
                .expect("global executor queue lock")
                .len()
        }
    }

    #[test]
    fn input_empty_driver_wakes_when_its_terminal_sink_finishes_early() {
        let mut harness = input_empty_driver_with_early_finish_sink();
        harness.park();
        assert_eq!(harness.queued_wakes(), 0, "nothing has happened yet");

        harness.finished.store(true, Ordering::Release);
        harness.early_finish.notify_observers();

        assert_eq!(
            harness.deliver_wakes(),
            1,
            "a parked driver must learn that its pipeline ended without waiting for its source"
        );
        assert_eq!(harness.source_observable.generation(), 0);
        let mut task = harness
            .executor
            .queue
            .lock()
            .expect("global executor queue lock")
            .pop_front()
            .expect("rescheduled driver");
        task.set_ready();
        assert!(matches!(
            task.process_for_test(Duration::from_millis(10)),
            DriverState::Finished
        ));
    }

    #[test]
    fn early_finish_between_the_turn_and_parking_is_not_lost() {
        let mut harness = input_empty_driver_with_early_finish_sink();
        // The turn sampled the early-finish generation before it parked; the
        // finish lands before the scheduler registers its observer.
        harness.finished.store(true, Ordering::Release);
        harness.early_finish.notify_observers();
        harness.park();

        assert_eq!(harness.deliver_wakes(), 1);
    }

    fn one_row_chunk() -> Chunk {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![7])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[novarocks_types::SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    struct DrainedSource;
    impl OperatorLifecycle for DrainedSource {
        fn name(&self) -> &str {
            "DRAINED_SOURCE"
        }
    }
    impl crate::exec::pipeline::shape::SourceKernel for DrainedSource {
        fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
            crate::exec::pipeline::shape::SourceStart::Finished
        }
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            unreachable!("a drained source has no work")
        }
    }
    struct OwingAfterFinishProcessor {
        pending: Arc<AtomicBool>,
        observable: Arc<Observable>,
        owed: Option<Chunk>,
    }
    impl OperatorLifecycle for OwingAfterFinishProcessor {
        fn name(&self) -> &str {
            "OWING_AFTER_FINISH"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for OwingAfterFinishProcessor {}
    impl crate::exec::pipeline::shape::TransformKernel for OwingAfterFinishProcessor {
        fn apply(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            _: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            unreachable!("this fixture starts with exhausted input")
        }
        fn drive(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
            let generation = self.observable.generation();
            if self.pending.load(Ordering::Acquire) {
                Ok(StepResult::new(
                    None,
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Finish,
                        observable: self.observable.clone(),
                        observed_generation: generation,
                        deadline: None,
                    }),
                    0,
                ))
            } else {
                Ok(StepResult::new(self.owed.take(), NextStep::Finished, 0))
            }
        }
        fn finish(
            &mut self,
            ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            self.drive(ctx)
        }
    }
    struct CountingSink {
        rows: Arc<AtomicUsize>,
        exit: Option<(Arc<AtomicBool>, Arc<Observable>)>,
    }
    impl OperatorLifecycle for CountingSink {
        fn name(&self) -> &str {
            "COUNTING_SINK"
        }
        fn observe_exit(&mut self) -> crate::exec::pipeline::operator::ExitObservation {
            match &self.exit {
                Some((pending, event)) if pending.load(Ordering::Acquire) => {
                    crate::exec::pipeline::operator::ExitObservation::pending(FinishWatch::Notify(
                        event.clone(),
                    ))
                }
                _ => crate::exec::pipeline::operator::ExitObservation::exited(),
            }
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for CountingSink {}
    impl crate::exec::pipeline::shape::SinkKernel for CountingSink {
        fn consume(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let rows = input.into_uncovered_chunk()?.len();
            self.rows.fetch_add(rows, Ordering::AcqRel);
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                crate::exec::pipeline::step::NextStep::NeedInput,
                rows as u64,
            ))
        }
    }

    #[test]
    fn owed_output_after_a_work_wait_reaches_the_sink_before_the_driver_ends() {
        let pending = Arc::new(AtomicBool::new(true));
        let owner = Arc::new(Observable::new());
        let rows = Arc::new(AtomicUsize::new(0));
        let runtime_state = Arc::new(RuntimeState::default());
        let mut driver = PipelineDriver::new(
            7,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(DrainedSource)),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    OwingAfterFinishProcessor {
                        pending: Arc::clone(&pending),
                        observable: Arc::clone(&owner),
                        owed: Some(one_row_chunk()),
                    },
                )),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CountingSink {
                        rows: Arc::clone(&rows),
                        exit: None,
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((67_500, 65_550)),
        );
        assert!(matches!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::OutputFull)
        ));

        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            Some((67_500, 65_550)),
            None,
            None,
            None,
        ));
        let task = DriverTask::new(
            driver,
            FragmentCompletion::new(1, fragment_ctx.runtime_state().error_state()),
            fragment_ctx,
            Duration::from_millis(10),
        );
        assert!(
            scheduler
                .add_blocked(task, BlockedReason::OutputFull)
                .is_ok()
        );
        let queued = |scheduler: &EventScheduler| {
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .len()
        };
        assert_eq!(queued(&scheduler), 0, "nothing ended yet");

        pending.store(false, Ordering::Release);
        owner.notify_observers();
        assert_eq!(queued(&scheduler), 1, "the owner's end wakes the driver");
        let key = scheduler
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock")
            .queue
            .pop_front()
            .expect("woken driver");
        scheduler.try_schedule_key(key);
        let mut task = executor
            .queue
            .lock()
            .expect("global executor queue lock")
            .pop_front()
            .expect("rescheduled driver");

        assert!(matches!(
            task.process_for_test(Duration::from_millis(10)),
            DriverState::Finished
        ));
        assert_eq!(
            rows.load(Ordering::Acquire),
            1,
            "the owed chunk is pushed before the driver completes"
        );
    }

    struct EndlessSource;

    impl OperatorLifecycle for EndlessSource {
        fn name(&self) -> &str {
            "ENDLESS_SOURCE"
        }
    }

    impl SourceKernel for EndlessSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(
                Some(one_row_chunk()),
                NextStep::Runnable,
                1,
            ))
        }
    }

    /// A sink that stops after `limit` rows and then flushes asynchronously.
    struct FlushingLimitSink {
        rows: Arc<AtomicUsize>,
        limit: usize,
        flushing: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }

    impl OperatorLifecycle for FlushingLimitSink {
        fn name(&self) -> &str {
            "FLUSHING_LIMIT_SINK"
        }
        fn observe_exit(&mut self) -> crate::exec::pipeline::operator::ExitObservation {
            if self.rows.load(Ordering::Acquire) >= self.limit
                && self.flushing.load(Ordering::Acquire)
            {
                crate::exec::pipeline::operator::ExitObservation::pending(FinishWatch::Notify(
                    self.observable.clone(),
                ))
            } else {
                crate::exec::pipeline::operator::ExitObservation::exited()
            }
        }
    }

    impl InputEncodingPolicy for FlushingLimitSink {}
    impl SinkKernel for FlushingLimitSink {
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            let rows = self.rows.fetch_add(chunk.len(), Ordering::AcqRel) + chunk.len();
            Ok(SinkStep::new(
                if rows >= self.limit {
                    NextStep::Finished
                } else {
                    NextStep::NeedInput
                },
                chunk.len() as u64,
            ))
        }
    }

    #[test]
    fn a_continuously_ready_driver_still_waits_for_its_sinks_pending_finish() {
        let rows = Arc::new(AtomicUsize::new(0));
        let flushing = Arc::new(AtomicBool::new(true));
        let mut driver = PipelineDriver::new(
            9,
            vec![
                Box::new(Source::new(EndlessSource)),
                Box::new(Transform::hydrating(FlushingLimitSink {
                    rows: Arc::clone(&rows),
                    limit: 3,
                    flushing: Arc::clone(&flushing),
                    observable: Arc::new(Observable::new()),
                })),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            Some((67_700, 65_570)),
        );

        // The source never runs dry, so nothing but the finished sink ends
        // the turn: it must end in PendingFinish, not Ready.
        assert!(matches!(
            driver.process(Duration::from_secs(5)),
            DriverState::PendingFinish
        ));
        assert_eq!(rows.load(Ordering::Acquire), 3);
        assert!(driver.pending_finish_wait_on_worker().unwrap().is_some());

        flushing.store(false, Ordering::Release);
        assert!(driver.pending_finish_wait_on_worker().unwrap().is_none());
        assert!(matches!(
            driver.process(Duration::from_secs(5)),
            DriverState::Finished
        ));
        assert_eq!(rows.load(Ordering::Acquire), 3, "no row after the limit");
    }

    #[test]
    fn pending_work_that_ended_before_parking_goes_straight_back_to_a_worker() {
        let pending = Arc::new(AtomicBool::new(true));
        let rows = Arc::new(AtomicUsize::new(0));
        let runtime_state = Arc::new(RuntimeState::default());
        let mut driver = PipelineDriver::new(
            8,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(DrainedSource)),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CountingSink {
                        rows,
                        exit: Some((pending.clone(), Arc::new(Observable::new()))),
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((67_600, 65_560)),
        );
        assert!(matches!(
            driver.process(Duration::from_millis(10)),
            DriverState::PendingFinish
        ));
        // The work ends between the turn and parking, without a notification
        // the scheduler could still observe.
        pending.store(false, Ordering::Release);

        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());
        let task = DriverTask::new(
            driver,
            FragmentCompletion::new(1, runtime_state.error_state()),
            Arc::new(FragmentContext::new(
                None,
                runtime_state,
                Some((67_600, 65_560)),
                None,
                None,
                None,
            )),
            Duration::from_millis(10),
        );
        let mut slot = Some(task);
        assert!(scheduler.add_pending_finish(&mut slot).unwrap());
        assert!(slot.is_none(), "the original scheduler took the task");
        assert!(scheduler.blocked.lock().expect("blocked lock").is_empty());
        let mut task = executor
            .queue
            .lock()
            .expect("global executor queue lock")
            .pop_front()
            .expect("driver handed back to a worker");
        assert!(matches!(
            task.process_for_test(Duration::from_millis(10)),
            DriverState::Finished
        ));
    }

    #[test]
    fn notification_delivery_is_deduplicated_state_blind_and_aba_safe() {
        let source_ready = Arc::new(AtomicBool::new(false));
        let poll_forbidden = Arc::new(AtomicBool::new(false));
        let use_second_observable = Arc::new(AtomicBool::new(false));
        let first_observable = Arc::new(Observable::try_bounded(1).unwrap());
        let second_observable = Arc::new(Observable::try_bounded(1).unwrap());
        let terminal_ready = Arc::new(AtomicBool::new(true));
        let terminal_observable = Arc::new(Observable::new());
        let runtime_state = Arc::new(RuntimeState::default());
        let driver = PipelineDriver::new(
            8,
            vec![
                Box::new(Source::new(ControlledDynamicSource {
                    ready: Arc::clone(&source_ready),
                    poll_forbidden: Arc::clone(&poll_forbidden),
                    use_second_observable: Arc::clone(&use_second_observable),
                    first_observable: Arc::clone(&first_observable),
                    second_observable: Arc::clone(&second_observable),
                })),
                Box::new(Transform::hydrating(ControlledTerminalSink {
                    ready: terminal_ready,
                    observable: terminal_observable,
                })),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((72_100, 72_101)),
        );
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            runtime_state,
            Some((72_100, 72_101)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, fragment_ctx.runtime_state().error_state());
        let mut task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(10),
        );
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());

        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected input-empty block, got {state:?}"),
        };
        assert!(
            scheduler.add_blocked(task, reason).is_ok(),
            "frozen source observable is registrable"
        );
        assert!(
            first_observable.try_subscribe(Arc::new(|| {})).is_err(),
            "the active source owns the bounded subscription slot"
        );
        assert!(
            second_observable.try_subscribe(Arc::new(|| {})).is_ok(),
            "an inactive source has no subscription"
        );
        assert!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .is_empty(),
            "a stable blocked transition must remain parked until notification"
        );

        poll_forbidden.store(true, Ordering::Release);
        source_ready.store(true, Ordering::Release);
        first_observable.notify_observers();
        first_observable.notify_observers();
        assert_eq!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .len(),
            1,
            "duplicate notifications must coalesce by driver key"
        );
        let key = scheduler
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock")
            .queue
            .pop_front()
            .expect("source notification must wake the blocked driver");
        first_observable.notify_observers();
        {
            let state = scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock");
            assert!(
                state.queue.is_empty(),
                "a notification in the pop-to-remove gap must join the pending wake-up"
            );
            assert!(state.pending.contains_key(&key));
        }
        scheduler.try_schedule_key(key);

        assert!(
            scheduler
                .blocked
                .lock()
                .expect("event scheduler blocked lock")
                .is_empty()
        );
        assert_eq!(
            executor
                .queue
                .lock()
                .expect("global executor queue lock")
                .len(),
            1
        );

        let mut task = executor
            .queue
            .lock()
            .expect("global executor queue lock")
            .pop_front()
            .expect("woken task returns to its worker queue");
        poll_forbidden.store(false, Ordering::Release);
        source_ready.store(false, Ordering::Release);
        use_second_observable.store(true, Ordering::Release);
        second_observable.notify_observers();
        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected second input-empty block, got {state:?}"),
        };
        let (driver_event, consumed_generation, _) = task.blocked_observable_snapshot().unwrap();
        assert!(scheduler.add_blocked(task, reason).is_ok());
        assert!(
            first_observable.try_subscribe(Arc::new(|| {})).is_ok(),
            "switching source releases the old bounded slot"
        );
        assert!(
            second_observable.try_subscribe(Arc::new(|| {})).is_err(),
            "the current source has exactly one scoped subscription"
        );

        scheduler.enqueue_observable(key, &Arc::downgrade(&driver_event), consumed_generation);
        assert!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .is_empty(),
            "the generation consumed by the worker's block decision must be discarded"
        );

        first_observable.notify_observers();
        assert!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .is_empty(),
            "a stale observable identity must not wake a later block of the same driver key"
        );

        second_observable.notify_observers();
        assert_eq!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .queue
                .len(),
            1,
            "the current observable identity must wake the reblocked driver"
        );
    }

    #[test]
    fn old_input_deadline_cannot_wake_a_later_output_full_block() {
        let source_ready = Arc::new(AtomicBool::new(false));
        let source_observable = Arc::new(Observable::new());
        let source_deadline = Arc::new(Mutex::new(Some(DriverBlockDeadline::new(
            Instant::now() + Duration::from_secs(30),
            1,
        ))));
        let terminal_ready = Arc::new(AtomicBool::new(false));
        let terminal_observable = Arc::new(Observable::new());
        let runtime_state = Arc::new(RuntimeState::default());
        let driver = PipelineDriver::new(
            19,
            vec![
                Box::new(Source::new(DeadlineSource {
                    ready: Arc::clone(&source_ready),
                    observable: Arc::clone(&source_observable),
                    deadline: source_deadline,
                })),
                Box::new(Transform::hydrating(ControlledTerminalSink {
                    ready: Arc::clone(&terminal_ready),
                    observable: terminal_observable,
                })),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((93_001, 93_002)),
        );
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            runtime_state,
            Some((93_001, 93_002)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, fragment_ctx.runtime_state().error_state());
        let mut task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(10),
        );
        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected input-empty block, got {state:?}"),
        };
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared::new());
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());
        assert!(scheduler.add_blocked(task, reason).is_ok());
        let key = DriverKey::new(Some((93_001, 93_002)), 19);
        let old_epoch = scheduler
            .blocked
            .lock()
            .expect("event scheduler blocked lock")
            .get(&key)
            .expect("blocked input task")
            .block_epoch;

        source_ready.store(true, Ordering::Release);
        source_observable.notify_observers();
        let queued = scheduler
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock")
            .queue
            .pop_front()
            .expect("source wake");
        scheduler.try_schedule_key(queued);
        let mut task = executor
            .queue
            .lock()
            .expect("global executor queue lock")
            .pop_front()
            .expect("worker task");
        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::OutputFull) => reason,
            state => panic!("expected output-full reblock, got {state:?}"),
        };
        assert!(scheduler.add_blocked(task, reason).is_ok());

        scheduler.enqueue_due_deadline(key, old_epoch, 1);
        let state = scheduler
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        assert!(state.queue.is_empty());
        assert!(!state.deadlines.contains_key(&key));
        assert!(state.deadline_index.is_empty());
    }

    #[test]
    fn due_deadlines_preempt_a_continuously_ready_scheduler() {
        let now = Instant::now();
        let ready_key = DriverKey::new(Some((10, 11)), 1);
        let deadline_key = DriverKey::new(Some((12, 13)), 2);
        let mut scheduler_queue = RescheduleState::new();
        assert!(scheduler_queue.enqueue(ready_key));
        scheduler_queue.install_deadline(
            deadline_key,
            ScheduledDeadline {
                at: now,
                operator_token: 7,
                block_epoch: 9,
            },
        );
        assert_eq!(
            scheduler_queue.pop_wake(now),
            Some(SchedulerWake::Deadline(deadline_key, 9, 7)),
            "a fragment-local ready backlog must not starve its due deadline"
        );

        let ready_scheduler = Arc::new(EventScheduler::new());
        let deadline_scheduler = Arc::new(EventScheduler::new());
        let mut dispatcher_queue = EventDispatcherQueues::new();
        dispatcher_queue
            .registered
            .insert(1, Arc::downgrade(&ready_scheduler));
        dispatcher_queue
            .registered
            .insert(2, Arc::downgrade(&deadline_scheduler));
        assert!(dispatcher_queue.enqueue(1, Arc::downgrade(&ready_scheduler)));
        dispatcher_queue.update_deadline(2, Arc::downgrade(&deadline_scheduler), 1, Some(now));
        let selected = dispatcher_queue
            .pop_dispatchable(now)
            .expect("due scheduler is dispatchable");
        assert_eq!(selected.scheduler_id, 2);
        assert_eq!(
            dispatcher_queue
                .ready
                .front()
                .expect("ready scheduler remains queued")
                .scheduler_id,
            1,
            "a process-wide ready backlog must yield to a due deadline"
        );
    }

    #[test]
    fn shutdown_interrupts_a_far_future_deadline_wait() {
        let source_ready = Arc::new(AtomicBool::new(false));
        let runtime_state = Arc::new(RuntimeState::default());
        let driver = PipelineDriver::new(
            20,
            vec![Box::new(Source::new(DeadlineSource {
                ready: source_ready,
                observable: Arc::new(Observable::new()),
                deadline: Arc::new(Mutex::new(Some(DriverBlockDeadline::new(
                    Instant::now() + Duration::from_secs(30),
                    1,
                )))),
            }))],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            Some((93_011, 93_012)),
        );
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            runtime_state,
            Some((93_011, 93_012)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, fragment_ctx.runtime_state().error_state());
        let mut task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(10),
        );
        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected input-empty block, got {state:?}"),
        };
        let scheduler = Arc::new(EventScheduler::new());
        let dispatcher = EventDispatcher::new();
        scheduler.attach_executor(Arc::new(ExecutorShared::new()), &dispatcher);
        assert!(scheduler.add_blocked(task, reason).is_ok());
        let scheduler_id = scheduler
            .dispatcher
            .get()
            .expect("scheduler registration")
            .id;
        let index_deadline = Instant::now() + Duration::from_secs(1);
        while !dispatcher
            .state
            .queues
            .lock()
            .expect("event dispatcher queue lock")
            .deadlines
            .contains_key(&scheduler_id)
            && Instant::now() < index_deadline
        {
            thread::yield_now();
        }
        let queues = dispatcher
            .state
            .queues
            .lock()
            .expect("event dispatcher queue lock");
        assert_eq!(queues.deadlines.len(), 1);
        assert_eq!(queues.deadline_index.len(), 1);
        drop(queues);

        let start = Instant::now();
        let drained = dispatcher.close_and_drain();
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(drained.len(), 1);
        for task in drained {
            task.reject_due_to_executor_shutdown();
        }
        assert!(completion.stopped_fact().is_some());
        assert!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .deadlines
                .is_empty()
        );
        assert!(
            scheduler
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock")
                .deadline_index
                .is_empty()
        );
        assert_eq!(dispatcher.registered_scheduler_count(), 0);
        dispatcher.shutdown_and_join();
    }

    #[test]
    fn dispatcher_visits_only_the_scheduler_that_was_marked_ready() {
        let dispatcher = EventDispatcher::new();
        let executor = Arc::new(ExecutorShared::new());
        let idle = (0..512)
            .map(|_| {
                let scheduler = Arc::new(EventScheduler::new());
                scheduler.attach_executor(Arc::clone(&executor), &dispatcher);
                scheduler
            })
            .collect::<Vec<_>>();
        let active = Arc::new(EventScheduler::new());
        active.attach_executor(executor, &dispatcher);

        active.enqueue(DriverKey::new(Some((9_001, 9_002)), 7));
        let deadline = Instant::now() + Duration::from_secs(1);
        while active.dispatch_invocations.load(Ordering::Acquire) == 0 && Instant::now() < deadline
        {
            thread::yield_now();
        }

        assert!(active.dispatch_invocations.load(Ordering::Acquire) > 0);
        assert!(
            idle.iter()
                .all(|scheduler| scheduler.dispatch_invocations.load(Ordering::Acquire) == 0),
            "idle schedulers must not be scanned when another scheduler wakes"
        );
        for scheduler in idle {
            scheduler.shutdown();
        }
        active.shutdown();
        dispatcher.shutdown_and_join();
    }
}
