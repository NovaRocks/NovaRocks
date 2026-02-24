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
//! Global driver executor and worker pool.
//!
//! Responsibilities:
//! - Schedules driver tasks across worker threads and tracks fragment completion status.
//! - Coordinates task queues, wake-up signaling, and terminal completion callbacks.
//!
//! Key exported interfaces:
//! - Types: `FragmentCompletion`, `DriverTask`, `ExecutorShared`, `GlobalDriverExecutor`.
//! - Functions: `global_driver_executor`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use crate::common::app_config;
use crate::runtime::exchange;
use crate::runtime::query_context::query_context_manager;
use crate::runtime::result_buffer;

use super::blocked_driver_poller::BlockedDriverPoller;
use super::driver::{DriverState, PipelineDriver};
use super::operator::BlockedReason;
use crate::exec::pipeline::schedule::observer::Observable;

/// Completion result payload reported when a fragment finishes execution.
pub struct FragmentCompletion {
    mu: Mutex<FragmentCompletionState>,
    cv: Condvar,
    fragment_ctx: Arc<crate::exec::pipeline::fragment_context::FragmentContext>,
}

#[derive(Debug)]
struct FragmentCompletionState {
    remaining: usize,
    aborting: bool,
    error: Option<String>,
}

impl FragmentCompletion {
    pub(crate) fn new(
        driver_count: usize,
        fragment_ctx: Arc<crate::exec::pipeline::fragment_context::FragmentContext>,
    ) -> Arc<Self> {
        Arc::new(Self {
            mu: Mutex::new(FragmentCompletionState {
                remaining: driver_count,
                aborting: false,
                error: None,
            }),
            cv: Condvar::new(),
            fragment_ctx,
        })
    }

    pub(crate) fn fragment_ctx(
        &self,
    ) -> &Arc<crate::exec::pipeline::fragment_context::FragmentContext> {
        &self.fragment_ctx
    }

    pub fn should_abort(&self) -> bool {
        self.mu.lock().expect("fragment completion lock").aborting
    }

    pub fn fail(&self, err: String) {
        self.fail_internal(err, true);
    }

    pub(crate) fn abort_from_query(&self, err: String) {
        self.fail_internal(err, false);
    }

    fn fail_internal(&self, err: String, notify_query: bool) {
        let first = self.fragment_ctx.set_final_status(err.clone());
        let mut st = self.mu.lock().expect("fragment completion lock");
        if st.error.is_none() {
            st.error = Some(err.clone());
        }
        st.aborting = true;
        self.cv.notify_all();
        drop(st);

        if notify_query && first {
            if let Some(query_id) = self.fragment_ctx.query_id() {
                let mgr = query_context_manager();
                let finsts = mgr.cancel_query(query_id, err.clone());
                for id in finsts {
                    result_buffer::close_error(id, err.clone());
                    exchange::cancel_fragment(id.hi, id.lo);
                }
            }
        }
    }

    pub fn driver_finished(&self) {
        let mut st = self.mu.lock().expect("fragment completion lock");
        if st.remaining == 0 {
            return;
        }
        st.remaining -= 1;
        if st.remaining == 0 {
            self.cv.notify_all();
        }
        let finished = st.remaining == 0;
        drop(st);
        if finished {
            self.fragment_ctx.event_scheduler().shutdown();
        }
    }

    pub fn wait(&self) -> Result<(), String> {
        let mut st = self.mu.lock().expect("fragment completion lock");
        while st.remaining > 0 {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }

        st.error.clone().map(Err).unwrap_or(Ok(()))
    }
}

/// Schedulable driver task containing execution context and completion hooks.
pub struct DriverTask {
    driver: PipelineDriver,
    completion: Arc<FragmentCompletion>,
    time_slice: Duration,
}

impl DriverTask {
    pub fn new(
        driver: PipelineDriver,
        completion: Arc<FragmentCompletion>,
        time_slice: Duration,
    ) -> Self {
        Self {
            driver,
            completion,
            time_slice,
        }
    }

    pub(crate) fn driver_id(&self) -> i32 {
        self.driver.driver_id()
    }

    pub(crate) fn fragment_instance_id(&self) -> Option<(i64, i64)> {
        self.driver.fragment_instance_id()
    }

    pub(crate) fn fragment_ctx(
        &self,
    ) -> &Arc<crate::exec::pipeline::fragment_context::FragmentContext> {
        self.completion.fragment_ctx()
    }

    pub(crate) fn should_abort(&self) -> bool {
        self.completion.should_abort()
    }

    pub(crate) fn has_pending_finish(&self) -> bool {
        self.driver.has_pending_finish()
    }

    pub(crate) fn should_abort_immediately(&self) -> bool {
        self.should_abort() && !self.has_pending_finish()
    }

    pub(crate) fn finish_due_to_abort(self) {
        let completion = Arc::clone(&self.completion);
        drop(self);
        completion.driver_finished();
    }

    pub(crate) fn source_observable(&self) -> Option<Arc<Observable>> {
        self.driver.source_observable()
    }

    pub(crate) fn sink_observable(&self) -> Option<Arc<Observable>> {
        self.driver.sink_observable()
    }

    pub(crate) fn source_name(&self) -> &str {
        self.driver.source_name()
    }

    pub(crate) fn sink_name(&self) -> &str {
        self.driver.sink_name()
    }

    pub(crate) fn source_ready(&self) -> bool {
        self.driver.source_ready()
    }

    pub(crate) fn sink_ready(&self) -> bool {
        self.driver.sink_ready()
    }

    pub(crate) fn schedule_state(&self) -> Arc<super::driver::DriverScheduleState> {
        self.driver.schedule_state()
    }

    pub(crate) fn try_mark_source_observer_registered(&self) -> bool {
        self.driver.try_mark_source_observer_registered()
    }

    pub(crate) fn try_mark_sink_observer_registered(&self) -> bool {
        self.driver.try_mark_sink_observer_registered()
    }

    pub(crate) fn set_in_blocked(&self, value: bool) {
        self.driver.set_in_blocked(value);
    }

    pub(crate) fn set_need_check_reschedule(&self, value: bool) {
        self.driver.set_need_check_reschedule(value);
    }

    pub(crate) fn check_is_ready(&self) -> bool {
        self.driver.check_is_ready()
    }

    pub(crate) fn set_ready(&mut self) {
        self.driver.set_ready();
    }
}

/// Shared executor internals used by global driver executor worker threads.
pub(crate) struct ExecutorShared {
    pub(crate) queue: Mutex<VecDeque<DriverTask>>,
    pub(crate) cv: Condvar,
    pub(crate) shutdown: AtomicBool,
}

impl ExecutorShared {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
        }
    }
}

/// Global executor that schedules and runs pipeline driver tasks across worker threads.
pub struct GlobalDriverExecutor {
    shared: Arc<ExecutorShared>,
    _poller: BlockedDriverPoller,
    _workers: Vec<thread::JoinHandle<()>>,
}

impl GlobalDriverExecutor {
    fn new(num_threads: usize) -> Self {
        let num_threads = num_threads.max(1);
        let shared = Arc::new(ExecutorShared::new());
        let poller = BlockedDriverPoller::new(Arc::clone(&shared));
        poller.start();

        let mut workers = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let shared_cloned = Arc::clone(&shared);
            let poller_cloned = poller.clone();
            workers.push(thread::spawn(move || {
                worker_loop(shared_cloned, poller_cloned)
            }));
        }

        Self {
            shared,
            _poller: poller,
            _workers: workers,
        }
    }

    pub fn submit(&self, tasks: Vec<DriverTask>) {
        if tasks.is_empty() {
            return;
        }

        let mut queue = self
            .shared
            .queue
            .lock()
            .expect("global executor queue lock");
        for task in tasks {
            let scheduler = task.fragment_ctx().event_scheduler();
            scheduler.attach_executor(Arc::clone(&self.shared));
            scheduler.register_driver(&task);
            task.driver.print_pipeline_structure();
            queue.push_back(task);
        }
        self.shared.cv.notify_all();
    }
}

static GLOBAL_DRIVER_EXECUTOR: OnceLock<GlobalDriverExecutor> = OnceLock::new();

/// Return the process-wide global driver executor singleton.
pub fn global_driver_executor() -> &'static GlobalDriverExecutor {
    GLOBAL_DRIVER_EXECUTOR.get_or_init(|| {
        let runtime_config = app_config::config()
            .map(|c| c.runtime.clone())
            .unwrap_or_default();
        GlobalDriverExecutor::new(runtime_config.actual_exec_threads())
    })
}

fn worker_loop(shared: Arc<ExecutorShared>, poller: BlockedDriverPoller) {
    loop {
        let mut task = {
            let mut queue = shared.queue.lock().expect("global executor queue lock");
            while queue.is_empty() && !shared.shutdown.load(Ordering::Acquire) {
                queue = shared
                    .cv
                    .wait(queue)
                    .expect("global executor queue condvar wait");
            }
            if shared.shutdown.load(Ordering::Acquire) {
                return;
            }
            queue.pop_front()
        };

        let Some(mut task) = task.take() else {
            continue;
        };

        if task.completion.should_abort() && !task.has_pending_finish() {
            drop(task.driver);
            task.completion.driver_finished();
            continue;
        }

        let state = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.driver.process(task.time_slice)
        }))
        .unwrap_or_else(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic payload".to_string()
            };
            DriverState::Failed(format!("panic in driver execution: {msg}"))
        });

        if matches!(
            state,
            DriverState::Ready
                | DriverState::Running
                | DriverState::Blocked(_)
                | DriverState::PendingFinish
        ) {
            task.driver.report_exec_state_if_necessary();
        }

        match state {
            DriverState::Ready | DriverState::Running => {
                if task.completion.should_abort() && !task.has_pending_finish() {
                    drop(task.driver);
                    task.completion.driver_finished();
                    continue;
                }
                let mut queue = shared.queue.lock().expect("global executor queue lock");
                queue.push_back(task);
                shared.cv.notify_one();
            }
            DriverState::Blocked(reason) => {
                if task.should_abort_immediately() {
                    task.finish_due_to_abort();
                    continue;
                }
                match reason {
                    BlockedReason::InputEmpty | BlockedReason::OutputFull => {
                        let scheduler = task.fragment_ctx().event_scheduler();
                        match scheduler.add_blocked(task, reason.clone()) {
                            Ok(()) => {}
                            Err(task) => {
                                let err = format!(
                                    "missing observable for blocked driver: reason={:?} finst={:?} driver_id={}",
                                    reason,
                                    task.fragment_instance_id(),
                                    task.driver_id()
                                );
                                task.completion.fail(err);
                                task.completion.driver_finished();
                            }
                        }
                    }
                    BlockedReason::Dependency(_) => {
                        let scheduler = task.fragment_ctx().event_scheduler();
                        match scheduler.add_blocked(task, reason.clone()) {
                            Ok(()) => {}
                            Err(task) => {
                                let err = format!(
                                    "event scheduler refused dependency-blocked driver: finst={:?} driver_id={}",
                                    task.fragment_instance_id(),
                                    task.driver_id()
                                );
                                task.completion.fail(err);
                                task.completion.driver_finished();
                            }
                        }
                    }
                }
            }
            DriverState::PendingFinish => {
                poller.add_pending_finish(task);
            }
            DriverState::Finished => {
                task.completion.driver_finished();
            }
            DriverState::Canceled => {
                task.completion.fail("pipeline driver canceled".to_string());
                task.completion.driver_finished();
            }
            DriverState::Failed(err) => {
                task.completion.fail(err);
                task.completion.driver_finished();
            }
        }
    }
}
