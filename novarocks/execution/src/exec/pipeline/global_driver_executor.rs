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
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::blocked_driver_poller::BlockedDriverPoller;
use super::driver::{DriverState, PipelineDriver};
use super::fragment_context::FragmentContext;
use super::operator::BlockedReason;
use crate::exec::pipeline::schedule::observer::Observable;

/// Completion result payload reported when a fragment finishes execution.
pub struct FragmentCompletion {
    mu: Mutex<FragmentCompletionState>,
    cv: Condvar,
}

#[derive(Debug)]
struct FragmentCompletionState {
    remaining: usize,
    aborting: bool,
    error: Option<String>,
}

impl FragmentCompletion {
    pub fn new(driver_count: usize) -> Arc<Self> {
        Arc::new(Self {
            mu: Mutex::new(FragmentCompletionState {
                remaining: driver_count,
                aborting: false,
                error: None,
            }),
            cv: Condvar::new(),
        })
    }

    pub fn should_abort(&self) -> bool {
        self.mu.lock().expect("fragment completion lock").aborting
    }

    pub fn fail(&self, err: String) -> bool {
        let mut st = self.mu.lock().expect("fragment completion lock");
        self.fail_locked(&mut st, err)
    }

    fn fail_locked(&self, st: &mut FragmentCompletionState, err: String) -> bool {
        if st.error.is_some() || st.remaining == 0 {
            return false;
        }
        st.error = Some(err);
        st.aborting = true;
        self.cv.notify_all();
        true
    }

    pub fn driver_finished(&self) -> bool {
        let mut st = self.mu.lock().expect("fragment completion lock");
        if st.remaining == 0 {
            return false;
        }
        st.remaining -= 1;
        let finished = st.remaining == 0;
        if finished {
            self.cv.notify_all();
        }
        finished
    }

    pub fn wait(&self) -> Result<(), String> {
        let mut st = self.mu.lock().expect("fragment completion lock");
        while st.remaining > 0 {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }

        st.error.clone().map(Err).unwrap_or(Ok(()))
    }

    pub fn wait_timeout(&self, timeout: Duration, err: String) -> Result<(), String> {
        self.wait_timeout_with_local_cancel(timeout, err, || {})
    }

    pub(crate) fn wait_timeout_with_local_cancel<F>(
        &self,
        timeout: Duration,
        err: String,
        on_timeout: F,
    ) -> Result<(), String>
    where
        F: FnOnce(),
    {
        let deadline = Instant::now() + timeout;
        let mut st = self.mu.lock().expect("fragment completion lock");
        let mut on_timeout = Some(on_timeout);
        while st.remaining > 0 {
            let now = Instant::now();
            if now >= deadline {
                let timeout_won = self.fail_locked(&mut st, err.clone());
                drop(st);
                if timeout_won {
                    on_timeout.take().expect("timeout callback is available")();
                }
                let mut st = self.mu.lock().expect("fragment completion lock");
                while st.remaining > 0 {
                    st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                return st.error.clone().map(Err).unwrap_or(Ok(()));
            }

            let remaining = deadline.saturating_duration_since(now);
            let (guard, result) = self
                .cv
                .wait_timeout(st, remaining)
                .unwrap_or_else(|e| e.into_inner());
            st = guard;
            if result.timed_out() && st.remaining > 0 {
                let timeout_won = self.fail_locked(&mut st, err.clone());
                drop(st);
                if timeout_won {
                    on_timeout.take().expect("timeout callback is available")();
                }
                let mut st = self.mu.lock().expect("fragment completion lock");
                while st.remaining > 0 {
                    st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                return st.error.clone().map(Err).unwrap_or(Ok(()));
            }
        }

        st.error.clone().map(Err).unwrap_or(Ok(()))
    }
}

/// Schedulable driver task containing execution context and completion hooks.
pub struct DriverTask {
    driver: PipelineDriver,
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    time_slice: Duration,
}

impl DriverTask {
    pub fn new(
        driver: PipelineDriver,
        completion: Arc<FragmentCompletion>,
        fragment_ctx: Arc<FragmentContext>,
        time_slice: Duration,
    ) -> Self {
        Self {
            driver,
            completion,
            fragment_ctx,
            time_slice,
        }
    }

    pub(crate) fn driver_id(&self) -> i32 {
        self.driver.driver_id()
    }

    pub(crate) fn fragment_instance_id(&self) -> Option<(i64, i64)> {
        self.driver.fragment_instance_id()
    }

    pub(crate) fn fragment_ctx(&self) -> &Arc<FragmentContext> {
        &self.fragment_ctx
    }

    pub(crate) fn should_abort(&self) -> bool {
        self.completion.should_abort()
    }

    pub(crate) fn has_pending_finish(&self) -> bool {
        self.driver.has_pending_finish()
    }

    pub(crate) fn should_abort_immediately(&self) -> bool {
        self.should_abort()
    }

    pub(crate) fn finish_due_to_abort(mut self) {
        self.driver.cancel_for_fragment_abort();
        self.driver_finished();
        drop(self);
    }

    pub(crate) fn fail(&self, err: String) {
        if self.completion.fail(err.clone()) {
            self.fragment_ctx.set_final_status(err);
        }
    }

    pub(crate) fn driver_finished(&self) {
        if self.completion.driver_finished() {
            self.fragment_ctx.event_scheduler().shutdown();
        }
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
    pub fn new(num_threads: usize) -> Self {
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

        if task.completion.should_abort() {
            task.finish_due_to_abort();
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

        if task.completion.should_abort() {
            task.finish_due_to_abort();
            continue;
        }

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
                if task.completion.should_abort() {
                    task.finish_due_to_abort();
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
                                task.fail(err);
                                task.driver_finished();
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
                                task.fail(err);
                                task.driver_finished();
                            }
                        }
                    }
                }
            }
            DriverState::PendingFinish => {
                poller.add_pending_finish(task);
            }
            DriverState::Finished => {
                if task.has_pending_finish() {
                    // A terminal driver state must not complete the fragment while an
                    // asynchronous operator is still publishing its final output.
                    poller.add_pending_finish(task);
                } else {
                    task.driver_finished();
                }
            }
            DriverState::Canceled => {
                task.fail("pipeline driver canceled".to_string());
                task.driver_finished();
            }
            DriverState::Failed(err) => {
                task.fail(err);
                task.driver_finished();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn fragment_completion_wait_timeout_drains_before_returning_timeout_error() {
        let completion = FragmentCompletion::new(1);
        let waiter = Arc::clone(&completion);
        let (result_tx, result_rx) = mpsc::sync_channel(1);

        let join = thread::spawn(move || {
            result_tx
                .send(waiter.wait_timeout(
                    Duration::from_millis(5),
                    "query timed out after 5 ms".to_string(),
                ))
                .expect("test receiver remains available");
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        while !completion.should_abort() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            completion.should_abort(),
            "timeout must initiate local cancellation"
        );
        assert!(
            result_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "timeout must not return before submitted drivers drain"
        );

        completion.driver_finished();
        let err = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter must return after the final driver drains")
            .expect_err("incomplete fragment should time out");
        join.join().expect("timeout waiter thread must not panic");

        assert_eq!(err, "query timed out after 5 ms");
        assert!(completion.should_abort());
    }
}
