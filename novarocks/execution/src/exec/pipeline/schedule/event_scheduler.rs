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
//!
//! Key exported interfaces:
//! - Types: `DriverKey`, `EventScheduler`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;
use std::time::Instant;

use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::global_driver_executor::{DriverTask, ExecutorShared};
use crate::exec::pipeline::operator::{BlockedReason, DriverBlockDeadline};
use crate::exec::pipeline::schedule::observer::{Observable, PipelineObserver};
use tracing::debug;

const EVENT_SCHEDULER_LOG_EVERY: u64 = 1024;
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
    pending: HashSet<DriverKey>,
    deadlines: HashMap<DriverKey, ScheduledDeadline>,
    deadline_index: BTreeSet<DeadlineIndexEntry>,
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
            pending: HashSet::new(),
            deadlines: HashMap::new(),
            deadline_index: BTreeSet::new(),
        }
    }

    fn enqueue(&mut self, key: DriverKey) -> bool {
        if !self.pending.insert(key) {
            return false;
        }
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

    pub(crate) fn close_and_drain(&self) -> Vec<DriverTask> {
        if self.state.shutdown.swap(true, Ordering::AcqRel) {
            return Vec::new();
        }
        let schedulers = {
            let mut queues = self
                .state
                .queues
                .lock()
                .expect("event dispatcher queue lock");
            let schedulers = queues.registered.values().cloned().collect::<Vec<_>>();
            queues.registered.clear();
            queues.ready.clear();
            queues.ready_pending.clear();
            queues.deadlines.clear();
            queues.deadline_index.clear();
            schedulers
        };
        self.state.wake_cv.notify_all();
        schedulers
            .into_iter()
            .filter_map(|scheduler| scheduler.upgrade())
            .flat_map(|scheduler| scheduler.shutdown_and_take_blocked())
            .collect()
    }

    pub(crate) fn take_thread(&self) -> Option<thread::JoinHandle<()>> {
        self.thread
            .lock()
            .expect("event dispatcher thread lock")
            .take()
    }

    pub(crate) fn shutdown_and_join(&self) {
        let _ = self.close_and_drain();
        if let Some(thread) = self.take_thread() {
            let _ = thread.join();
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
        let Some((current_observable, blocked_generation)) = &entry.observable else {
            return;
        };
        if !Weak::ptr_eq(current_observable, observable) || generation <= *blocked_generation {
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

    fn park_blocked(
        self: &Arc<Self>,
        task: DriverTask,
        reason: BlockedReason,
        observable: Arc<Observable>,
        generation: u64,
        deadline: Option<DriverBlockDeadline>,
    ) -> Result<(), Box<DriverTask>> {
        let emit_exchange_marker = crate::runtime::exchange::exchange_snapshot_markers_enabled()
            && matches!(reason, BlockedReason::InputEmpty)
            && task.source_name().contains("EXCHANGE")
            && generation == 0;
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let block_epoch = self
            .next_block_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        self.register_observer(&task, &reason, Arc::clone(&observable));
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
                    block_epoch,
                },
            );
            let entry = blocked.get(&key).expect("blocked task was just inserted");
            entry.task.set_in_blocked(true);
            let aborted = entry.task.should_abort_immediately();
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

        if emit_exchange_marker {
            crate::runtime::exchange::emit_exchange_snapshot_marker(|| {
                format!(
                    "event=driver_park finst={:?} driver_id={} reason={:?} frozen_generation={} current_generation={} block_epoch={}",
                    key.finst,
                    key.driver_id,
                    reason,
                    generation,
                    observable.generation(),
                    block_epoch,
                )
            });
        }

        // Registration can race with a transition that already made the task
        // runnable. The generation comparison closes that window without
        // evaluating operator readiness on the scheduler thread.
        if aborted {
            self.enqueue(key);
        } else {
            let current_generation = observable.generation();
            self.enqueue_observable(key, &Arc::downgrade(&observable), current_generation);
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
                    block_epoch,
                },
            );
            let entry = blocked.get(&key).expect("blocked task was just inserted");
            entry.task.set_in_blocked(true);
            entry.task.should_abort_immediately()
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
        }
        Ok(())
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
        let callback: Arc<dyn Fn() + Send + Sync + 'static> = match kind {
            ObserverKind::Source => {
                let observer = Arc::clone(&observer);
                Arc::new(move || observer.source_trigger())
            }
            ObserverKind::Sink => {
                let observer = Arc::clone(&observer);
                Arc::new(move || observer.sink_trigger())
            }
        };
        observable.add_observer(callback);
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
            return;
        };
        if entry.block_epoch != block_epoch {
            return;
        }
        let mut state = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        let Some(deadline) = state.deadlines.get(&key).copied() else {
            return;
        };
        if deadline.block_epoch != block_epoch
            || deadline.operator_token != operator_token
            || deadline.at > Instant::now()
        {
            return;
        }
        state.remove_deadline(key);
        self.deadline_generation.fetch_add(1, Ordering::AcqRel);
        if state.enqueue(key) {
            drop(state);
            self.notify_dispatcher();
        }
    }

    fn try_schedule_key(self: &Arc<Self>, key: DriverKey) {
        let (task, exchange_marker) = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            let entry = blocked.remove(&key);
            let exchange_marker = crate::runtime::exchange::exchange_snapshot_markers_enabled()
                .then_some(entry.as_ref())
                .flatten()
                .and_then(|entry| {
                    entry
                        .task
                        .source_name()
                        .contains("EXCHANGE")
                        .then(|| {
                            let (blocked_generation, current_generation) = entry
                                .observable
                                .as_ref()
                                .and_then(|(observable, blocked_generation)| {
                                    observable.upgrade().map(|observable| {
                                        (*blocked_generation, observable.generation())
                                    })
                                })
                                .map_or((0, 0), |generations| generations);
                            (blocked_generation, current_generation, entry.block_epoch)
                        })
                        .filter(|(blocked_generation, _, _)| *blocked_generation == 0)
                });
            let mut state = self
                .reschedule_queue
                .lock()
                .expect("event scheduler queue lock");
            state.pending.remove(&key);
            if state.remove_deadline(key).is_some() {
                self.deadline_generation.fetch_add(1, Ordering::AcqRel);
            }
            (entry.map(|entry| entry.task), exchange_marker)
        };
        if let Some((blocked_generation, current_generation, block_epoch)) = exchange_marker {
            crate::runtime::exchange::emit_exchange_snapshot_marker(|| {
                format!(
                    "event=driver_wake finst={:?} driver_id={} blocked_generation={} current_generation={} block_epoch={}",
                    key.finst, key.driver_id, blocked_generation, current_generation, block_epoch,
                )
            });
        }
        let Some(mut task) = task else {
            return;
        };
        task.set_ready();
        task.set_in_blocked(false);
        self.enqueue_ready(task);
    }

    fn enqueue_ready(&self, task: DriverTask) {
        let Some(shared) = self.shared.get() else {
            debug!("EventScheduler enqueue_ready: executor not attached");
            return;
        };
        let mut queue = shared.queue.lock().expect("global executor queue lock");
        queue.push_back(task);
        shared.cv.notify_one();
    }
}

enum ObserverKind {
    Source,
    Sink,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::FragmentCompletion;
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::runtime::runtime_state::RuntimeState;

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

    impl Operator for ControlledDynamicSource {
        fn name(&self) -> &str {
            "CONTROLLED_DYNAMIC_SOURCE"
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ControlledDynamicSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            assert!(
                !self.poll_forbidden.load(Ordering::Acquire),
                "scheduler thread must not poll source readiness"
            );
            self.ready.load(Ordering::Acquire)
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            if self.use_second_observable.load(Ordering::Acquire) {
                Some(Arc::clone(&self.second_observable))
            } else {
                Some(Arc::clone(&self.first_observable))
            }
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

    impl Operator for DeadlineSource {
        fn name(&self) -> &str {
            "DEADLINE_SOURCE"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for DeadlineSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }

        fn source_block_deadline(&self) -> Option<DriverBlockDeadline> {
            *self.deadline.lock().expect("test deadline lock")
        }
    }

    impl Operator for ControlledTerminalSink {
        fn name(&self) -> &str {
            "CONTROLLED_TERMINAL_SINK"
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ControlledTerminalSink {
        fn need_input(&self) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    impl Operator for FinishTransitionSink {
        fn name(&self) -> &str {
            "FINISH_TRANSITION_SINK"
        }

        fn is_finished(&self) -> bool {
            self.finished.load(Ordering::Acquire)
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for FinishTransitionSink {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            true
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            (!self.is_finished()).then(|| Arc::clone(&self.observable))
        }
    }

    #[test]
    fn output_full_task_that_finishes_before_parking_is_rechecked() {
        let finished = Arc::new(AtomicBool::new(false));
        let observable = Arc::new(Observable::new());
        let runtime_state = Arc::new(RuntimeState::default());
        let mut driver = PipelineDriver::new(
            3,
            vec![Box::new(FinishTransitionSink {
                finished: Arc::clone(&finished),
                observable: Arc::clone(&observable),
            })],
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
        let completion = FragmentCompletion::new(1);
        let task = DriverTask::new(driver, completion, fragment_ctx, Duration::from_millis(10));
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            admission_closed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
        });
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

    #[test]
    fn notification_delivery_is_deduplicated_state_blind_and_aba_safe() {
        let source_ready = Arc::new(AtomicBool::new(false));
        let poll_forbidden = Arc::new(AtomicBool::new(false));
        let use_second_observable = Arc::new(AtomicBool::new(false));
        let first_observable = Arc::new(Observable::new());
        let second_observable = Arc::new(Observable::new());
        let terminal_ready = Arc::new(AtomicBool::new(true));
        let terminal_observable = Arc::new(Observable::new());
        let runtime_state = Arc::new(RuntimeState::default());
        let driver = PipelineDriver::new(
            8,
            vec![
                Box::new(ControlledDynamicSource {
                    ready: Arc::clone(&source_ready),
                    poll_forbidden: Arc::clone(&poll_forbidden),
                    use_second_observable: Arc::clone(&use_second_observable),
                    first_observable: Arc::clone(&first_observable),
                    second_observable: Arc::clone(&second_observable),
                }),
                Box::new(ControlledTerminalSink {
                    ready: terminal_ready,
                    observable: terminal_observable,
                }),
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
        let completion = FragmentCompletion::new(1);
        let mut task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(10),
        );
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            admission_closed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
        });
        assert!(scheduler.shared.set(Arc::clone(&executor)).is_ok());

        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected input-empty block, got {state:?}"),
        };
        assert!(
            scheduler.add_blocked(task, reason).is_ok(),
            "frozen source observable is registrable"
        );
        assert_eq!(first_observable.num_observers(), 1);
        assert_eq!(second_observable.num_observers(), 0);
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
            assert!(state.pending.contains(&key));
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
        assert!(scheduler.add_blocked(task, reason).is_ok());
        assert_eq!(second_observable.num_observers(), 1);

        scheduler.enqueue_observable(
            key,
            &Arc::downgrade(&second_observable),
            second_observable.generation(),
        );
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
                Box::new(DeadlineSource {
                    ready: Arc::clone(&source_ready),
                    observable: Arc::clone(&source_observable),
                    deadline: source_deadline,
                }),
                Box::new(ControlledTerminalSink {
                    ready: Arc::clone(&terminal_ready),
                    observable: terminal_observable,
                }),
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
        let completion = FragmentCompletion::new(1);
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
        let executor = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            admission_closed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
        });
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
            vec![Box::new(DeadlineSource {
                ready: source_ready,
                observable: Arc::new(Observable::new()),
                deadline: Arc::new(Mutex::new(Some(DriverBlockDeadline::new(
                    Instant::now() + Duration::from_secs(30),
                    1,
                )))),
            })],
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
        let completion = FragmentCompletion::new(1);
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
        scheduler.attach_executor(
            Arc::new(ExecutorShared {
                queue: Mutex::new(VecDeque::new()),
                cv: Condvar::new(),
                admission_closed: AtomicBool::new(false),
                shutdown: AtomicBool::new(false),
                live_workers: AtomicUsize::new(0),
            }),
            &dispatcher,
        );
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
        let executor = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            admission_closed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
        });
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
