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
//! - Reschedules ready drivers when observed dependencies become satisfiable.
//!
//! Key exported interfaces:
//! - Types: `DriverKey`, `EventScheduler`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;

use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::global_driver_executor::{DriverTask, ExecutorShared};
use crate::exec::pipeline::operator::BlockedReason;
use crate::exec::pipeline::schedule::observer::{Observable, PipelineObserver};
use crate::novarocks_logging::debug;

const EVENT_SCHEDULER_LOG_EVERY: u64 = 1024;
static EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT: AtomicU64 = AtomicU64::new(0);
static EVENT_SCHEDULER_BLOCKED_ADD_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_sample_log(counter: &AtomicU64) -> bool {
    counter.fetch_add(1, Ordering::Relaxed) % EVENT_SCHEDULER_LOG_EVERY == 0
}

fn is_parquet_scan(source_name: &str) -> bool {
    source_name.contains("HDFS_SCAN") || source_name.contains("PARQUET_SCAN")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
/// Stable key that identifies a driver in event-scheduler maps and queues.
pub(crate) struct DriverKey {
    finst: Option<(i64, i64)>,
    driver_id: i32,
}

impl DriverKey {
    pub(crate) fn new(finst: Option<(i64, i64)>, driver_id: i32) -> Self {
        Self { finst, driver_id }
    }
}

/// Event scheduler that wakes blocked drivers after observed dependency transitions.
pub(crate) struct EventScheduler {
    shared: OnceLock<Arc<ExecutorShared>>,
    blocked: Mutex<HashMap<DriverKey, DriverTask>>,
    reschedule_queue: Mutex<VecDeque<DriverKey>>,
    reschedule_cv: Condvar,
    shutdown: AtomicBool,
    started: AtomicBool,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl EventScheduler {
    pub(crate) fn new() -> Self {
        Self {
            shared: OnceLock::new(),
            blocked: Mutex::new(HashMap::new()),
            reschedule_queue: Mutex::new(VecDeque::new()),
            reschedule_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            started: AtomicBool::new(false),
            thread: Mutex::new(None),
        }
    }

    pub(crate) fn attach_executor(self: &Arc<Self>, shared: Arc<ExecutorShared>) {
        let _ = self.shared.set(shared);
        self.start_if_needed();
    }

    pub(crate) fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.reschedule_cv.notify_all();
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
        let mut queue = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        queue.push_back(key);
        self.reschedule_cv.notify_one();
    }

    pub(crate) fn enqueue_all_blocked(&self) {
        let keys = {
            let blocked = self.blocked.lock().expect("event scheduler blocked lock");
            if blocked.is_empty() {
                return;
            }
            blocked
                .values()
                .for_each(|task| task.set_need_check_reschedule(true));
            blocked.keys().copied().collect::<Vec<_>>()
        };
        let mut queue = self
            .reschedule_queue
            .lock()
            .expect("event scheduler queue lock");
        for key in keys {
            queue.push_back(key);
        }
        self.reschedule_cv.notify_all();
    }

    pub(crate) fn register_driver(self: &Arc<Self>, task: &DriverTask) {
        let mut observer: Option<Arc<PipelineObserver>> = None;
        let mut get_observer = || {
            observer
                .get_or_insert_with(|| {
                    Arc::new(PipelineObserver::new(
                        task.schedule_state(),
                        Arc::downgrade(self),
                        DriverKey::new(task.fragment_instance_id(), task.driver_id()),
                        task.driver_id(),
                        task.fragment_instance_id(),
                    ))
                })
                .clone()
        };
        if let Some(observable) = task.source_observable() {
            if task.try_mark_source_observer_registered() {
                let observer = get_observer();
                self.add_observer(observable, Arc::clone(&observer), ObserverKind::Source);
            }
        } else if should_sample_log(&EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT) {
            debug!(
                "EventScheduler register: missing source observable; finst={:?} driver_id={}",
                task.fragment_instance_id(),
                task.driver_id()
            );
        }
        if let Some(observable) = task.sink_observable() {
            if task.try_mark_sink_observer_registered() {
                let observer = get_observer();
                self.add_observer(observable, observer, ObserverKind::Sink);
            }
        } else if should_sample_log(&EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT) {
            debug!(
                "EventScheduler register: missing sink observable; finst={:?} driver_id={}",
                task.fragment_instance_id(),
                task.driver_id()
            );
        }
    }

    pub(crate) fn add_blocked(
        self: &Arc<Self>,
        task: DriverTask,
        reason: BlockedReason,
    ) -> Result<(), DriverTask> {
        self.register_driver(&task);
        match reason {
            BlockedReason::InputEmpty => {
                let Some(observable) = task.source_observable() else {
                    if should_sample_log(&EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT) {
                        debug!(
                            "EventScheduler add_blocked: InputEmpty missing source observable; finst={:?} driver_id={} source_ready={} sink_ready={}",
                            task.fragment_instance_id(),
                            task.driver_id(),
                            task.source_ready(),
                            task.sink_ready()
                        );
                    }
                    return Err(task);
                };
                let is_scan = is_parquet_scan(task.source_name());
                if is_scan || should_sample_log(&EVENT_SCHEDULER_BLOCKED_ADD_LOG_COUNT) {
                    debug!(
                        "EventScheduler add_blocked: reason=InputEmpty finst={:?} driver_id={} source={} sink={} source_ready={} sink_ready={} observers_before={}",
                        task.fragment_instance_id(),
                        task.driver_id(),
                        task.source_name(),
                        task.sink_name(),
                        task.source_ready(),
                        task.sink_ready(),
                        observable.num_observers()
                    );
                }
                self.park_blocked(task);
                Ok(())
            }
            BlockedReason::OutputFull => {
                let Some(observable) = task.sink_observable() else {
                    if should_sample_log(&EVENT_SCHEDULER_MISSING_OBS_LOG_COUNT) {
                        debug!(
                            "EventScheduler add_blocked: OutputFull missing sink observable; finst={:?} driver_id={} source_ready={} sink_ready={}",
                            task.fragment_instance_id(),
                            task.driver_id(),
                            task.source_ready(),
                            task.sink_ready()
                        );
                    }
                    return Err(task);
                };
                let is_scan = is_parquet_scan(task.source_name());
                if is_scan || should_sample_log(&EVENT_SCHEDULER_BLOCKED_ADD_LOG_COUNT) {
                    debug!(
                        "EventScheduler add_blocked: reason=OutputFull finst={:?} driver_id={} source={} sink={} source_ready={} sink_ready={} observers_before={}",
                        task.fragment_instance_id(),
                        task.driver_id(),
                        task.source_name(),
                        task.sink_name(),
                        task.source_ready(),
                        task.sink_ready(),
                        observable.num_observers()
                    );
                }
                self.park_blocked(task);
                Ok(())
            }
            BlockedReason::Dependency(dep) => {
                self.park_blocked_with_dependency(task, dep);
                Ok(())
            }
        }
    }

    fn park_blocked(self: &Arc<Self>, task: DriverTask) {
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let schedule_state = task.schedule_state();
        let _token = schedule_state.acquire_schedule_token();
        {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            blocked.insert(key, task);
        }
        schedule_state.set_in_blocked(true);
        self.enqueue(key);
    }

    fn park_blocked_with_dependency(self: &Arc<Self>, task: DriverTask, dep: DependencyHandle) {
        let key = DriverKey::new(task.fragment_instance_id(), task.driver_id());
        let schedule_state = task.schedule_state();
        let _token = schedule_state.acquire_schedule_token();
        {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            blocked.insert(key, task);
        }
        schedule_state.set_in_blocked(true);
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
        self.enqueue(key);
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
        loop {
            let key = {
                let mut queue = self
                    .reschedule_queue
                    .lock()
                    .expect("event scheduler queue lock");
                while queue.is_empty() && !self.shutdown.load(Ordering::Acquire) {
                    queue = self
                        .reschedule_cv
                        .wait(queue)
                        .expect("event scheduler condvar wait");
                }
                if queue.is_empty() && self.shutdown.load(Ordering::Acquire) {
                    None
                } else {
                    queue.pop_front()
                }
            };
            if key.is_none() && self.shutdown.load(Ordering::Acquire) {
                break;
            }
            if let Some(key) = key {
                self.try_schedule_key(key);
            }
        }
    }

    fn try_schedule_key(&self, key: DriverKey) {
        let mut task = {
            let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
            blocked.remove(&key)
        };
        let Some(mut task) = task.take() else {
            return;
        };

        if task.should_abort_immediately() {
            task.set_in_blocked(false);
            task.set_need_check_reschedule(false);
            task.finish_due_to_abort();
            return;
        }

        if task.check_is_ready() {
            task.set_ready();
            task.set_in_blocked(false);
            task.set_need_check_reschedule(false);
            self.enqueue_ready(task);
            return;
        }

        let mut blocked = self.blocked.lock().expect("event scheduler blocked lock");
        blocked.insert(key, task);
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
