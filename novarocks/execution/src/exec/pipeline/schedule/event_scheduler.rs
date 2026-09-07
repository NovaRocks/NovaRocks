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
}

impl DriverKey {
    pub(crate) fn new(finst: Option<(i64, i64)>, driver_id: i32) -> Self {
        Self { finst, driver_id }
    }
}

/// Event scheduler that returns notified blocked drivers to executor workers.
pub(crate) struct EventScheduler {
    shared: OnceLock<Arc<ExecutorShared>>,
    blocked: Mutex<HashMap<DriverKey, BlockedTask>>,
    reschedule_queue: Mutex<RescheduleState>,
    reschedule_cv: Condvar,
    shutdown: AtomicBool,
    started: AtomicBool,
    next_block_epoch: AtomicU64,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl EventScheduler {
    pub(crate) fn new() -> Self {
        Self {
            shared: OnceLock::new(),
            blocked: Mutex::new(HashMap::new()),
            reschedule_queue: Mutex::new(RescheduleState::new()),
            reschedule_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            started: AtomicBool::new(false),
            next_block_epoch: AtomicU64::new(0),
            thread: Mutex::new(None),
        }
    }

    pub(crate) fn attach_executor(self: &Arc<Self>, shared: Arc<ExecutorShared>) {
        let _ = self.shared.set(shared);
        self.start_if_needed();
    }

    pub(crate) fn shutdown(&self) {
        // Guard the shutdown predicate with the same mutex used by the condvar wait loop,
        // otherwise the idle scheduler thread can miss the wake-up and keep join() blocked.
        let mut queue_guard = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        queue_guard.clear_deadlines();
        self.reschedule_cv.notify_all();
        drop(queue_guard);
        if let Some(handle) = self
            .thread
            .lock()
            .expect("event scheduler thread lock")
            .take()
        {
            if handle.thread().id() == thread::current().id() {
                debug!("EventScheduler shutdown called from scheduler thread; skip self-join");
            } else {
                let _ = handle.join();
            }
        }
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
            self.reschedule_cv.notify_one();
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
            self.reschedule_cv.notify_one();
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
        self.reschedule_cv.notify_all();
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
                self.park_blocked(task, reason, observable, generation, deadline);
                Ok(())
            }
            BlockedReason::Dependency(dep) => {
                self.park_blocked_with_dependency(task, dep);
                Ok(())
            }
        }
    }

    fn park_blocked(
        self: &Arc<Self>,
        task: DriverTask,
        reason: BlockedReason,
        observable: Arc<Observable>,
        generation: u64,
        deadline: Option<DriverBlockDeadline>,
    ) {
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
                self.reschedule_cv.notify_one();
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
    }

    fn park_blocked_with_dependency(self: &Arc<Self>, task: DriverTask, dep: DependencyHandle) {
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let block_epoch = self
            .next_block_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let aborted = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
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

    fn start_if_needed(self: &Arc<Self>) {
        if self.shared.get().is_none() {
            return;
        }
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let scheduler = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("event_scheduler".to_string())
            .spawn(move || scheduler.run())
            .expect("spawn event scheduler thread");
        *self.thread.lock().expect("event scheduler thread lock") = Some(handle);
    }

    fn run(self: Arc<Self>) {
        enum Wake {
            Key(DriverKey),
            Deadline(DriverKey, u64, u64),
            Shutdown,
        }

        loop {
            let wake = {
                let mut queue = self
                    .reschedule_queue
                    .lock()
                    .expect("event scheduler queue lock");
                loop {
                    if let Some(key) = queue.queue.pop_front() {
                        // Keep `pending` set until `try_schedule_key` removes
                        // the blocked task. A callback in this gap is coalesced
                        // into the wake-up already being delivered.
                        break Wake::Key(key);
                    }
                    if self.shutdown.load(Ordering::Acquire) {
                        break Wake::Shutdown;
                    }
                    let earliest = queue.earliest_deadline();
                    let Some((key, deadline)) = earliest else {
                        queue = self
                            .reschedule_cv
                            .wait(queue)
                            .expect("event scheduler condvar wait");
                        continue;
                    };
                    let now = Instant::now();
                    if deadline.at <= now {
                        break Wake::Deadline(key, deadline.block_epoch, deadline.operator_token);
                    }
                    let (guard, _) = self
                        .reschedule_cv
                        .wait_timeout(queue, deadline.at.saturating_duration_since(now))
                        .unwrap_or_else(|error| error.into_inner());
                    queue = guard;
                }
            };
            match wake {
                Wake::Key(key) => self.try_schedule_key(key),
                Wake::Deadline(key, block_epoch, operator_token) => {
                    self.enqueue_due_deadline(key, block_epoch, operator_token)
                }
                Wake::Shutdown => break,
            }
        }
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
        if state.enqueue(key) {
            self.reschedule_cv.notify_one();
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
            state.remove_deadline(key);
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
            shutdown: AtomicBool::new(false),
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
        let mut task = DriverTask::new(driver, completion, fragment_ctx, Duration::from_millis(10));
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
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
        let mut task = DriverTask::new(driver, completion, fragment_ctx, Duration::from_millis(10));
        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected input-empty block, got {state:?}"),
        };
        let scheduler = Arc::new(EventScheduler::new());
        let executor = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
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
        let mut task = DriverTask::new(driver, completion, fragment_ctx, Duration::from_millis(10));
        let reason = match task.process_for_test(Duration::from_millis(10)) {
            DriverState::Blocked(reason @ BlockedReason::InputEmpty) => reason,
            state => panic!("expected input-empty block, got {state:?}"),
        };
        let scheduler = Arc::new(EventScheduler::new());
        scheduler.attach_executor(Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
        }));
        assert!(scheduler.add_blocked(task, reason).is_ok());

        let start = Instant::now();
        scheduler.shutdown();
        assert!(start.elapsed() < Duration::from_secs(1));
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
    }
}
