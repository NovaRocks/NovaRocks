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

use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::exec::pipeline::schedule::event_scheduler::{DriverKey, EventScheduler};
pub use crate::runtime::observable::{DeferNotify, Observable, Observer};
use tracing::debug;

#[allow(
    dead_code,
    reason = "Retained for scheduler notification observability tests."
)]
static NOTIFY_COUNT: AtomicU64 = AtomicU64::new(0);
static OBSERVER_NOT_BLOCKED_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

const OBSERVER_LOG_EVERY: u64 = 1024;

fn should_log_observer(counter: &AtomicU64) -> bool {
    counter
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(OBSERVER_LOG_EVERY)
}

/// Observer registry for dependency/event notifications inside pipeline scheduling.
pub(crate) struct PipelineObserver {
    scheduler: Weak<EventScheduler>,
    observable: Weak<Observable>,
    key: DriverKey,
    driver_id: i32,
    fragment_instance_id: Option<(i64, i64)>,
}

impl PipelineObserver {
    pub(crate) fn new(
        scheduler: Weak<EventScheduler>,
        observable: Weak<Observable>,
        key: DriverKey,
        driver_id: i32,
        fragment_instance_id: Option<(i64, i64)>,
    ) -> Self {
        Self {
            scheduler,
            observable,
            key,
            driver_id,
            fragment_instance_id,
        }
    }

    pub(crate) fn source_trigger(&self) {
        self.trigger("source");
    }

    pub(crate) fn sink_trigger(&self) {
        self.trigger("sink");
    }

    fn trigger(&self, event: &'static str) {
        if let (Some(scheduler), Some(observable)) =
            (self.scheduler.upgrade(), self.observable.upgrade())
        {
            // Notification callbacks are deliberately state-blind. They never
            // inspect a driver or operator; the worker owns every readiness
            // decision after the task is requeued.
            scheduler.enqueue_observable(self.key, &self.observable, observable.generation());
        } else if should_log_observer(&OBSERVER_NOT_BLOCKED_LOG_COUNT) {
            debug!(
                "Observer update dropped: scheduler already released; finst={:?} driver_id={} event={}",
                self.fragment_instance_id, self.driver_id, event
            );
        }
    }
}
