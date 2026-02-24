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
//! Observable primitives for scheduling events.
//!
//! Responsibilities:
//! - Provides callback registration and deferred notification helpers for dependency changes.
//! - Used by event scheduler and dependencies to broadcast readiness transitions.
//!
//! Key exported interfaces:
//! - Types: `Observer`, `PipelineObserver`, `Observable`, `DeferNotify`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::exec::pipeline::driver::DriverScheduleState;
use crate::exec::pipeline::schedule::event_scheduler::{DriverKey, EventScheduler};
use crate::novarocks_logging::debug;

static NOTIFY_COUNT: AtomicU64 = AtomicU64::new(0);
static OBSERVER_NOT_BLOCKED_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

const OBSERVER_LOG_EVERY: u64 = 1024;

fn should_log_observer(counter: &AtomicU64) -> bool {
    counter.fetch_add(1, Ordering::Relaxed) % OBSERVER_LOG_EVERY == 0
}

/// Callback type invoked when observable scheduling events are triggered.
pub type Observer = Arc<dyn Fn() + Send + Sync + 'static>;

const SOURCE_CHANGE_EVENT: usize = 1;
const SINK_CHANGE_EVENT: usize = 1 << 1;
#[allow(dead_code)]
const CANCEL_EVENT: usize = 1 << 2;

/// Observer registry for dependency/event notifications inside pipeline scheduling.
pub(crate) struct PipelineObserver {
    schedule_state: Arc<DriverScheduleState>,
    scheduler: Weak<EventScheduler>,
    key: DriverKey,
    driver_id: i32,
    fragment_instance_id: Option<(i64, i64)>,
    pending_event_cnt: AtomicUsize,
    events: AtomicUsize,
}

impl PipelineObserver {
    pub(crate) fn new(
        schedule_state: Arc<DriverScheduleState>,
        scheduler: Weak<EventScheduler>,
        key: DriverKey,
        driver_id: i32,
        fragment_instance_id: Option<(i64, i64)>,
    ) -> Self {
        Self {
            schedule_state,
            scheduler,
            key,
            driver_id,
            fragment_instance_id,
            pending_event_cnt: AtomicUsize::new(0),
            events: AtomicUsize::new(0),
        }
    }

    pub(crate) fn source_trigger(&self) {
        self.trigger(SOURCE_CHANGE_EVENT);
    }

    pub(crate) fn sink_trigger(&self) {
        self.trigger(SINK_CHANGE_EVENT);
    }

    #[allow(dead_code)]
    pub(crate) fn cancel_trigger(&self) {
        self.trigger(CANCEL_EVENT);
    }

    #[allow(dead_code)]
    pub(crate) fn all_trigger(&self) {
        self.trigger(SOURCE_CHANGE_EVENT | SINK_CHANGE_EVENT);
    }

    fn trigger(&self, event: usize) {
        self.events.fetch_or(event, Ordering::AcqRel);
        self.update();
    }

    fn update(&self) {
        if self.pending_event_cnt.fetch_add(1, Ordering::AcqRel) != 0 {
            return;
        }
        loop {
            let event = self.events.swap(0, Ordering::AcqRel);
            self.do_update(event);
            if self.pending_event_cnt.fetch_sub(1, Ordering::AcqRel) == 1 {
                break;
            }
        }
    }

    fn do_update(&self, event: usize) {
        let token = self.schedule_state.acquire_schedule_token();
        if !token.acquired() {
            self.schedule_state.set_need_check_reschedule(true);
        }
        if self.schedule_state.is_in_blocked() {
            if let Some(scheduler) = self.scheduler.upgrade() {
                scheduler.enqueue(self.key);
            } else if should_log_observer(&OBSERVER_NOT_BLOCKED_LOG_COUNT) {
                debug!(
                    "Observer update dropped: scheduler already released; finst={:?} driver_id={} event={}",
                    self.fragment_instance_id, self.driver_id, event
                );
            }
        } else {
            self.schedule_state.set_need_check_reschedule(true);
            if should_log_observer(&OBSERVER_NOT_BLOCKED_LOG_COUNT) {
                debug!(
                    "Observer update while not blocked: finst={:?} driver_id={} event={}",
                    self.fragment_instance_id, self.driver_id, event
                );
            }
        }
    }
}

/// Observable helper that stores and notifies subscribed scheduler callbacks.
pub struct Observable {
    observers: Mutex<Vec<Observer>>,
}

impl Observable {
    pub fn new() -> Self {
        Self {
            observers: Mutex::new(Vec::new()),
        }
    }

    pub fn add_observer(&self, observer: Observer) {
        let mut guard = self.observers.lock().expect("observable lock");
        guard.push(observer);
    }

    // Create a deferred notifier that triggers on drop if armed.
    pub fn defer_notify(self: &Arc<Self>) -> DeferNotify {
        DeferNotify::new(Arc::clone(self))
    }

    pub(in crate::exec::pipeline::schedule) fn notify_observers(&self) {
        let observers = {
            let guard = self.observers.lock().expect("observable lock");
            guard.clone()
        };
        let notify_count = NOTIFY_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if notify_count % 1024 == 0 {
            debug!(
                "Observable notify: count={} observers={}",
                notify_count,
                observers.len()
            );
        }
        for observer in observers {
            observer();
        }
    }

    pub fn num_observers(&self) -> usize {
        let guard = self.observers.lock().expect("observable lock");
        guard.len()
    }
}

/// DeferNotify delays observer callbacks until drop to ensure notifications happen out of locks.
/// Call `arm()` after the state change is committed.
#[must_use]
/// RAII helper that defers observable notification until scope exit.
pub struct DeferNotify {
    observable: Arc<Observable>,
    armed: AtomicBool,
}

impl DeferNotify {
    pub fn new(observable: Arc<Observable>) -> Self {
        Self {
            observable,
            armed: AtomicBool::new(false),
        }
    }

    // Arm the notifier so drop will deliver the notification.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl Drop for DeferNotify {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            self.observable.notify_observers();
        }
    }
}
