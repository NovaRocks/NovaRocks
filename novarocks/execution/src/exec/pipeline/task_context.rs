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
//! Fragment-level pipeline runtime context.
//!
//! Responsibilities:
//! - Holds shared state required by all drivers in one fragment execution.
//! - Carries identifiers, memory/runtime handles, and cancellation/error state.
//!
//! Key exported interfaces:
//! - Types: `TaskContext`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::task::ExecutionFailure;

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::exec::pipeline::schedule::event_scheduler::EventScheduler;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::profile::Profiler;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::{NoopTaskEventSink, TaskEventSink};
use novarocks_types::QueryId;

/// Fragment-scoped runtime context shared across drivers and operator instances.
#[allow(
    dead_code,
    reason = "Legacy progress reporting remains represented while application hosts complete migration."
)]
pub struct TaskContext {
    next_driver_id: AtomicI32,
    profiler: Option<Profiler>,
    runtime_state: Arc<RuntimeState>,
    task_key: Option<(i64, i64)>,
    #[allow(dead_code)]
    query_id: Option<QueryId>,
    #[allow(dead_code)]
    fe_addr: Option<RuntimeEndpoint>,
    #[allow(dead_code)]
    backend_num: Option<i32>,
    #[allow(dead_code)]
    enable_profile: bool,
    #[allow(dead_code)]
    runtime_profile_report_interval_ns: Option<i64>,
    legacy_progress_reporting: bool,
    event_sink: Arc<dyn TaskEventSink>,
    event_scheduler: Arc<EventScheduler>,
}

#[allow(
    dead_code,
    reason = "Legacy progress inspection remains available while application hosts complete migration."
)]
impl TaskContext {
    pub(crate) fn new(
        profiler: Option<Profiler>,
        runtime_state: Arc<RuntimeState>,
        task_key: Option<(i64, i64)>,
        query_id: Option<QueryId>,
        fe_addr: Option<RuntimeEndpoint>,
        backend_num: Option<i32>,
    ) -> Self {
        let enable_profile = runtime_state
            .query_options()
            .map(|opts| opts.enable_profile)
            .unwrap_or(false);
        let runtime_profile_report_interval_ns = runtime_state.runtime_profile_report_interval_ns();
        Self {
            next_driver_id: AtomicI32::new(0),
            profiler,
            runtime_state,
            task_key,
            query_id,
            fe_addr,
            backend_num,
            enable_profile,
            runtime_profile_report_interval_ns,
            legacy_progress_reporting: true,
            event_sink: Arc::new(NoopTaskEventSink),
            event_scheduler: Arc::new(EventScheduler::new()),
        }
    }

    pub(crate) fn new_report_neutral(
        profiler: Option<Profiler>,
        runtime_state: Arc<RuntimeState>,
        task_key: Option<(i64, i64)>,
        event_sink: Arc<dyn TaskEventSink>,
    ) -> Self {
        Self {
            next_driver_id: AtomicI32::new(0),
            profiler,
            runtime_state,
            task_key,
            query_id: None,
            fe_addr: None,
            backend_num: None,
            enable_profile: false,
            runtime_profile_report_interval_ns: None,
            legacy_progress_reporting: false,
            event_sink,
            event_scheduler: Arc::new(EventScheduler::new()),
        }
    }

    pub(crate) fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }

    pub(crate) fn runtime_state(&self) -> &Arc<RuntimeState> {
        &self.runtime_state
    }

    pub(crate) fn task_key(&self) -> Option<(i64, i64)> {
        self.task_key
    }

    #[allow(dead_code)]
    pub(crate) fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    #[allow(dead_code)]
    pub fn fe_addr(&self) -> Option<&RuntimeEndpoint> {
        self.fe_addr.as_ref()
    }

    #[allow(dead_code)]
    pub(crate) fn backend_num(&self) -> Option<i32> {
        self.backend_num
    }

    #[allow(dead_code)]
    pub(crate) fn enable_profile(&self) -> bool {
        self.enable_profile
    }

    #[allow(dead_code)]
    pub(crate) fn runtime_profile_report_interval_ns(&self) -> Option<i64> {
        self.runtime_profile_report_interval_ns
    }

    pub(crate) const fn legacy_progress_reporting(&self) -> bool {
        self.legacy_progress_reporting
    }

    pub(crate) fn event_sink(&self) -> Arc<dyn TaskEventSink> {
        Arc::clone(&self.event_sink)
    }

    pub(crate) fn next_driver_id(&self) -> i32 {
        self.next_driver_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn event_scheduler(&self) -> Arc<EventScheduler> {
        Arc::clone(&self.event_scheduler)
    }

    pub(crate) fn set_final_status(&self, err: impl Into<ExecutionFailure>) {
        self.runtime_state.error_state().set_error(err.into());
        // Stop subscriptions own eventual wakeup, including PendingFinish.
        // This fanout is only a hint: adjudication may follow this call, or
        // the owner may have set stop while a publication fence delays notice.
        self.event_scheduler.enqueue_all_blocked();
    }
}
