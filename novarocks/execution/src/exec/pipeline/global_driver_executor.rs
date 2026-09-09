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
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::blocked_driver_poller::BlockedDriverPoller;
use super::driver::{DriverState, PipelineDriver};
use super::fragment_context::FragmentContext;
use super::operator::{BlockedReason, DriverBlockDeadline};
use super::schedule::event_scheduler::EventDispatcher;
use crate::exec::pipeline::schedule::observer::Observable;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Completion result payload reported when a fragment finishes execution.
pub struct FragmentCompletion {
    mu: Mutex<FragmentCompletionState>,
    cv: Condvar,
}

/// Immutable proof that every submitted driver has actually stopped.
///
/// A failed conclusion may be available before this fact. Observers must use
/// this fact when they release execution-owned resources or publish a stopped
/// lifecycle transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentStoppedFact {
    conclusion: Result<(), String>,
}

impl FragmentStoppedFact {
    pub fn conclusion(&self) -> Result<(), String> {
        self.conclusion.clone()
    }
}

type FragmentStoppedObserver = Box<dyn FnOnce(FragmentStoppedFact) + Send + 'static>;

struct FragmentCompletionState {
    remaining: usize,
    aborting: bool,
    conclusion: Option<Result<(), String>>,
    stopped: Option<FragmentStoppedFact>,
    stopped_observers: Vec<FragmentStoppedObserver>,
}

impl FragmentCompletion {
    pub fn new(driver_count: usize) -> Arc<Self> {
        let stopped = (driver_count == 0).then_some(FragmentStoppedFact { conclusion: Ok(()) });
        Arc::new(Self {
            mu: Mutex::new(FragmentCompletionState {
                remaining: driver_count,
                aborting: false,
                conclusion: stopped.as_ref().map(FragmentStoppedFact::conclusion),
                stopped,
                stopped_observers: Vec::new(),
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
        if st.conclusion.is_some() || st.stopped.is_some() {
            return false;
        }
        st.conclusion = Some(Err(err));
        st.aborting = true;
        self.cv.notify_all();
        true
    }

    pub fn driver_finished(&self) -> bool {
        let (stopped, observers) = {
            let mut st = self.mu.lock().expect("fragment completion lock");
            if st.remaining == 0 {
                return false;
            }
            st.remaining -= 1;
            if st.remaining != 0 {
                return false;
            }

            let conclusion = st.conclusion.clone().unwrap_or(Ok(()));
            st.conclusion = Some(conclusion.clone());
            let stopped = FragmentStoppedFact { conclusion };
            st.stopped = Some(stopped.clone());
            let observers = std::mem::take(&mut st.stopped_observers);
            self.cv.notify_all();
            (stopped, observers)
        };
        for observer in observers {
            observer(stopped.clone());
        }
        true
    }

    /// Returns the execution conclusion as soon as it is known.
    ///
    /// Failure is known when cancellation or a driver error wins. Success is
    /// known only when every driver has actually stopped.
    pub fn conclusion(&self) -> Option<Result<(), String>> {
        self.mu
            .lock()
            .expect("fragment completion lock")
            .conclusion
            .clone()
    }

    pub fn stopped_fact(&self) -> Option<FragmentStoppedFact> {
        self.mu
            .lock()
            .expect("fragment completion lock")
            .stopped
            .clone()
    }

    /// Registers a one-shot stopped observer without a check/register race.
    ///
    /// The callback runs inline after the completion lock is released. When
    /// registration follows the final driver transition, it runs immediately
    /// with the retained stopped fact.
    pub fn subscribe_stopped(&self, observer: FragmentStoppedObserver) {
        let mut observer = Some(observer);
        let stopped = {
            let mut st = self.mu.lock().expect("fragment completion lock");
            match st.stopped.clone() {
                Some(stopped) => Some(stopped),
                None => {
                    st.stopped_observers
                        .push(observer.take().expect("stopped observer is available"));
                    None
                }
            }
        };
        if let Some(stopped) = stopped {
            observer
                .take()
                .expect("stopped observer was not registered")(stopped);
        }
    }

    pub fn wait(&self) -> Result<(), String> {
        let mut st = self.mu.lock().expect("fragment completion lock");
        while st.remaining > 0 {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }

        st.stopped
            .as_ref()
            .expect("remaining drivers reached zero without a stopped fact")
            .conclusion()
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
                return st
                    .stopped
                    .as_ref()
                    .expect("remaining drivers reached zero without a stopped fact")
                    .conclusion();
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
                return st
                    .stopped
                    .as_ref()
                    .expect("remaining drivers reached zero without a stopped fact")
                    .conclusion();
            }
        }

        st.stopped
            .as_ref()
            .expect("remaining drivers reached zero without a stopped fact")
            .conclusion()
    }

    #[cfg(test)]
    fn remaining_drivers(&self) -> usize {
        self.mu.lock().expect("fragment completion lock").remaining
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

    pub(crate) fn source_name(&self) -> &str {
        self.driver.source_name()
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

    pub(crate) fn finish_due_to_abort(mut self) -> Option<Self> {
        if self.driver.cancel_for_fragment_abort() == DriverState::PendingFinish {
            return Some(self);
        }
        self.driver_finished();
        None
    }

    pub(crate) fn reject_due_to_executor_shutdown(mut self) {
        self.fail("driver executor is shutting down".to_string());
        if self.driver.cancel_for_fragment_abort() == DriverState::PendingFinish {
            // This driver was never admitted, so the stopped fact must remain
            // unavailable while an asynchronous operator still owns cleanup.
            // Dropping the rejected task releases its local handles without
            // forging the completion counter.
            return;
        }
        self.driver_finished();
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

    pub(crate) fn blocked_observable_snapshot(
        &self,
    ) -> Option<(Arc<Observable>, u64, Option<DriverBlockDeadline>)> {
        self.driver.blocked_observable_snapshot()
    }

    pub(crate) fn try_mark_source_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        self.driver.try_mark_source_observer_registered(observable)
    }

    pub(crate) fn try_mark_sink_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        self.driver.try_mark_sink_observer_registered(observable)
    }

    pub(crate) fn set_in_blocked(&self, value: bool) {
        self.driver.set_in_blocked(value);
    }

    pub(crate) fn pending_finish_complete(&self) -> bool {
        self.driver.pending_finish_complete()
    }

    pub(crate) fn set_ready(&mut self) {
        self.driver.set_ready();
    }

    #[cfg(test)]
    pub(crate) fn process_for_test(&mut self, time_slice: Duration) -> DriverState {
        self.driver.process(time_slice)
    }
}

/// Shared executor internals used by global driver executor worker threads.
pub(crate) struct ExecutorShared {
    pub(crate) queue: Mutex<VecDeque<DriverTask>>,
    pub(crate) cv: Condvar,
    pub(crate) admission_closed: AtomicBool,
    pub(crate) shutdown: AtomicBool,
    #[cfg(test)]
    pub(crate) live_workers: AtomicUsize,
}

impl ExecutorShared {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            admission_closed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            #[cfg(test)]
            live_workers: AtomicUsize::new(0),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutorShutdownPhase {
    Running,
    Joining,
    Joined,
}

struct ExecutorLifecycle {
    phase: ExecutorShutdownPhase,
    workers: Vec<thread::JoinHandle<()>>,
}

struct ExecutorLifecycleOwner {
    state: Mutex<ExecutorLifecycle>,
    cv: Condvar,
}

/// Global executor that schedules and runs pipeline driver tasks across worker threads.
pub struct GlobalDriverExecutor {
    shared: Arc<ExecutorShared>,
    event_dispatcher: EventDispatcher,
    poller: BlockedDriverPoller,
    lifecycle: Arc<ExecutorLifecycleOwner>,
}

#[cfg(test)]
pub(crate) struct DriverExecutorExitProbes {
    shared: Arc<ExecutorShared>,
    event_exited: Arc<AtomicBool>,
    poller_exited: Arc<AtomicBool>,
}

#[cfg(test)]
impl DriverExecutorExitProbes {
    pub(crate) fn all_exited(&self) -> bool {
        self.shared.live_workers.load(Ordering::Acquire) == 0
            && self.event_exited.load(Ordering::Acquire)
            && self.poller_exited.load(Ordering::Acquire)
    }
}

impl GlobalDriverExecutor {
    pub fn new(num_threads: usize) -> Self {
        let num_threads = num_threads.max(1);
        let shared = Arc::new(ExecutorShared::new());
        let event_dispatcher = EventDispatcher::new();
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
            event_dispatcher,
            poller,
            lifecycle: Arc::new(ExecutorLifecycleOwner {
                state: Mutex::new(ExecutorLifecycle {
                    phase: ExecutorShutdownPhase::Running,
                    workers,
                }),
                cv: Condvar::new(),
            }),
        }
    }

    /// Attempts to admit a batch of drivers.
    ///
    /// Returns `false` after shutdown has closed admission. Rejected drivers
    /// receive cooperative cancellation, but a driver with live asynchronous
    /// finish work does not publish a forged stopped fact.
    pub fn submit(&self, tasks: Vec<DriverTask>) -> bool {
        if tasks.is_empty() {
            return true;
        }

        // Scheduler attachment may start its notification thread, so keep it
        // outside the global worker queue critical section. Observable
        // registration belongs to the worker's exact blocked transition.
        for task in &tasks {
            let scheduler = task.fragment_ctx().event_scheduler();
            scheduler.attach_executor(Arc::clone(&self.shared), &self.event_dispatcher);
            task.driver.print_pipeline_structure();
        }
        let mut queue = self
            .shared
            .queue
            .lock()
            .expect("global executor queue lock");
        if self.shared.admission_closed.load(Ordering::Acquire) {
            drop(queue);
            abort_tasks(tasks);
            return false;
        }
        queue.extend(tasks);
        self.shared.cv.notify_all();
        true
    }

    /// Close task admission and start abort/drain for every admitted driver.
    ///
    /// The call waits up to the configured bound for every runtime-owned
    /// scheduler thread to join. Cleanup remains owned by the reaper after a
    /// timeout, and repeated calls observe the same lifecycle transition.
    pub fn shutdown(&self) -> Result<(), String> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    pub(crate) fn shutdown_with_timeout(&self, timeout: Duration) -> Result<(), String> {
        self.start_shutdown_reaper();
        let deadline = Instant::now() + timeout;
        let mut lifecycle = self
            .lifecycle
            .state
            .lock()
            .expect("executor lifecycle lock");
        while lifecycle.phase != ExecutorShutdownPhase::Joined {
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "driver executor did not stop within {} ms",
                    timeout.as_millis()
                ));
            }
            let (next, wait) = self
                .lifecycle
                .cv
                .wait_timeout(lifecycle, deadline.saturating_duration_since(now))
                .unwrap_or_else(|error| error.into_inner());
            lifecycle = next;
            if wait.timed_out() && lifecycle.phase != ExecutorShutdownPhase::Joined {
                return Err(format!(
                    "driver executor did not stop within {} ms",
                    timeout.as_millis()
                ));
            }
        }
        Ok(())
    }

    fn start_shutdown_reaper(&self) {
        self.begin_shutdown();
        let workers = {
            let mut lifecycle = self
                .lifecycle
                .state
                .lock()
                .expect("executor lifecycle lock");
            if lifecycle.phase != ExecutorShutdownPhase::Running {
                return;
            }
            lifecycle.phase = ExecutorShutdownPhase::Joining;
            std::mem::take(&mut lifecycle.workers)
        };
        let event_thread = self.event_dispatcher.take_thread();
        let poller_thread = self.poller.take_thread();
        let poller = self.poller.clone();
        let lifecycle = Arc::clone(&self.lifecycle);
        thread::Builder::new()
            .name("driver_executor_reaper".to_string())
            .spawn(move || {
                if let Some(event_thread) = event_thread {
                    let _ = event_thread.join();
                }
                for worker in workers {
                    let _ = worker.join();
                }
                // No worker can add another pending-finish task after this
                // point. The poller keeps cooperative abort ownership until
                // every asynchronous operator reports actual completion.
                poller.finish_producers();
                if let Some(poller_thread) = poller_thread {
                    let _ = poller_thread.join();
                }
                let mut state = lifecycle.state.lock().expect("executor lifecycle lock");
                state.phase = ExecutorShutdownPhase::Joined;
                lifecycle.cv.notify_all();
            })
            .expect("spawn driver executor reaper");
    }

    fn begin_shutdown(&self) {
        self.shared.admission_closed.store(true, Ordering::Release);
        let blocked = self.event_dispatcher.close_and_drain();
        if !blocked.is_empty() {
            for task in &blocked {
                task.fail("driver executor is shutting down".to_string());
            }
            self.shared
                .queue
                .lock()
                .expect("global executor queue lock")
                .extend(blocked);
        }
        self.poller.signal_shutdown();
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.cv.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn exit_probes(&self) -> DriverExecutorExitProbes {
        DriverExecutorExitProbes {
            shared: Arc::clone(&self.shared),
            event_exited: self.event_dispatcher.exit_probe(),
            poller_exited: self.poller.exit_probe(),
        }
    }
}

impl Drop for GlobalDriverExecutor {
    fn drop(&mut self) {
        // Drop can run on one of the executor's own workers through the last
        // RuntimeState owner. Transfer join ownership to the process reaper so
        // destruction never joins itself or waits for an uncooperative driver.
        self.start_shutdown_reaper();
    }
}

fn abort_tasks(tasks: Vec<DriverTask>) {
    for task in tasks {
        task.reject_due_to_executor_shutdown();
    }
}

fn worker_loop(shared: Arc<ExecutorShared>, poller: BlockedDriverPoller) {
    #[cfg(test)]
    let _worker_guard = WorkerCountGuard::new(&shared.live_workers);
    loop {
        let mut task = {
            let mut queue = shared.queue.lock().expect("global executor queue lock");
            while queue.is_empty() && !shared.shutdown.load(Ordering::Acquire) {
                queue = shared
                    .cv
                    .wait(queue)
                    .expect("global executor queue condvar wait");
            }
            if queue.is_empty() && shared.shutdown.load(Ordering::Acquire) {
                return;
            }
            queue.pop_front()
        };

        let Some(mut task) = task.take() else {
            continue;
        };

        if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort() {
            if shared.admission_closed.load(Ordering::Acquire) {
                task.fail("driver executor is shutting down".to_string());
            }
            if let Some(task) = task.finish_due_to_abort() {
                poller.add_pending_finish(task);
            }
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

        if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort() {
            if shared.admission_closed.load(Ordering::Acquire) {
                task.fail("driver executor is shutting down".to_string());
            }
            if let Some(task) = task.finish_due_to_abort() {
                poller.add_pending_finish(task);
            }
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
                if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort()
                {
                    if shared.admission_closed.load(Ordering::Acquire) {
                        task.fail("driver executor is shutting down".to_string());
                    }
                    if let Some(task) = task.finish_due_to_abort() {
                        poller.add_pending_finish(task);
                    }
                    continue;
                }
                let mut queue = shared.queue.lock().expect("global executor queue lock");
                queue.push_back(task);
                shared.cv.notify_one();
            }
            DriverState::Blocked(reason) => {
                if task.should_abort_immediately() {
                    if let Some(task) = task.finish_due_to_abort() {
                        poller.add_pending_finish(task);
                    }
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
                                let task = *task;
                                task.fail(err);
                                if let Some(task) = task.finish_due_to_abort() {
                                    poller.add_pending_finish(task);
                                }
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
                                let task = *task;
                                task.fail(err);
                                if let Some(task) = task.finish_due_to_abort() {
                                    poller.add_pending_finish(task);
                                }
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
struct WorkerCountGuard<'a>(&'a AtomicUsize);

#[cfg(test)]
impl<'a> WorkerCountGuard<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count)
    }
}

#[cfg(test)]
impl Drop for WorkerCountGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::mpsc;
    use std::thread;

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::runtime::runtime_state::RuntimeState;

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::yield_now();
        }
        predicate()
    }

    struct PendingAbortOperator {
        pending: Arc<AtomicBool>,
        observed: Arc<AtomicBool>,
        cancel_requested: Arc<AtomicBool>,
    }

    impl Operator for PendingAbortOperator {
        fn name(&self) -> &str {
            "pending_abort"
        }

        fn cancel(&mut self) {
            self.cancel_requested.store(true, Ordering::Release);
        }

        fn pending_finish(&self) -> bool {
            self.observed.store(true, Ordering::Release);
            self.pending.load(Ordering::Acquire)
        }
    }

    struct BlockingSource {
        entered: Arc<AtomicBool>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Operator for BlockingSource {
        fn name(&self) -> &str {
            "blocking_source"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for BlockingSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            self.entered.store(true, Ordering::Release);
            let (released, cv) = &*self.gate;
            let mut released = released.lock().expect("blocking source gate lock");
            while !*released {
                released = cv.wait(released).unwrap_or_else(|error| error.into_inner());
            }
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
    }

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

    #[test]
    fn failed_conclusion_does_not_claim_that_drivers_stopped() {
        let completion = FragmentCompletion::new(2);

        assert!(completion.fail("injected failure".to_string()));
        assert_eq!(
            completion.conclusion(),
            Some(Err("injected failure".to_string()))
        );
        assert_eq!(completion.remaining_drivers(), 2);
        assert_eq!(completion.stopped_fact(), None);

        assert!(!completion.driver_finished());
        assert_eq!(completion.remaining_drivers(), 1);
        assert_eq!(completion.stopped_fact(), None);

        assert!(completion.driver_finished());
        assert_eq!(completion.remaining_drivers(), 0);
        assert_eq!(
            completion
                .stopped_fact()
                .expect("final driver publishes actual stop")
                .conclusion(),
            Err("injected failure".to_string())
        );
    }

    #[test]
    fn stopped_subscription_before_and_after_transition_fires_once() {
        let completion = FragmentCompletion::new(1);
        let before = Arc::new(AtomicUsize::new(0));
        let before_callback = Arc::clone(&before);
        completion.subscribe_stopped(Box::new(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            before_callback.fetch_add(1, Ordering::SeqCst);
        }));

        assert!(completion.driver_finished());
        assert_eq!(before.load(Ordering::SeqCst), 1);

        let after = Arc::new(AtomicUsize::new(0));
        let after_callback = Arc::clone(&after);
        completion.subscribe_stopped(Box::new(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            after_callback.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert!(!completion.driver_finished());
        assert_eq!(before.load(Ordering::SeqCst), 1);
        assert_eq!(after.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stopped_subscription_racing_the_last_driver_cannot_lose_wakeup() {
        for _ in 0..1_000 {
            let completion = FragmentCompletion::new(1);
            let observed = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let finishing = Arc::clone(&completion);
            let finishing_barrier = Arc::clone(&barrier);
            let join = thread::spawn(move || {
                finishing_barrier.wait();
                finishing.driver_finished();
            });

            barrier.wait();
            let observed_callback = Arc::clone(&observed);
            completion.subscribe_stopped(Box::new(move |_| {
                observed_callback.fetch_add(1, Ordering::SeqCst);
            }));
            join.join().expect("finishing thread must not panic");

            assert_eq!(observed.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn shutdown_is_idempotent_and_joins_every_runtime_thread() {
        let executor = GlobalDriverExecutor::new(2);
        let shared = Arc::clone(&executor.shared);
        let event_exited = executor.event_dispatcher.exit_probe();
        let poller_exited = executor.poller.exit_probe();

        executor.shutdown().expect("first executor shutdown");
        executor.shutdown().expect("repeated executor shutdown");

        assert!(shared.admission_closed.load(Ordering::Acquire));
        assert!(shared.shutdown.load(Ordering::Acquire));
        assert_eq!(shared.live_workers.load(Ordering::Acquire), 0);
        assert!(event_exited.load(Ordering::Acquire));
        assert!(poller_exited.load(Ordering::Acquire));
        assert_eq!(
            executor
                .lifecycle
                .state
                .lock()
                .expect("executor lifecycle lock")
                .phase,
            ExecutorShutdownPhase::Joined
        );
    }

    #[test]
    fn dropping_and_rebuilding_runtime_exits_owned_threads() {
        for _ in 0..8 {
            let executor = GlobalDriverExecutor::new(1);
            let shared = Arc::clone(&executor.shared);
            let event_exited = executor.event_dispatcher.exit_probe();
            let poller_exited = executor.poller.exit_probe();

            drop(executor);

            assert!(wait_until(Duration::from_secs(1), || {
                shared.live_workers.load(Ordering::Acquire) == 0
                    && event_exited.load(Ordering::Acquire)
                    && poller_exited.load(Ordering::Acquire)
            }));
        }
    }

    #[test]
    fn shutdown_does_not_publish_stopped_while_pending_finish_is_live() {
        let executor = GlobalDriverExecutor::new(1);
        let pending = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let runtime_state = Arc::new(RuntimeState::default());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            Some((41_001, 41_002)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        executor.submit(vec![DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(PendingAbortOperator {
                    pending: Arc::clone(&pending),
                    observed: Arc::clone(&observed),
                    cancel_requested: Arc::clone(&cancel_requested),
                })],
                None,
                Vec::new(),
                runtime_state,
                Some((41_001, 41_002)),
            ),
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(1),
        )]);
        assert!(wait_until(Duration::from_secs(1), || observed.load(Ordering::Acquire)));

        let error = executor
            .shutdown_with_timeout(Duration::from_millis(20))
            .expect_err("live pending finish must keep shutdown incomplete");
        assert!(error.contains("did not stop within"));
        assert!(cancel_requested.load(Ordering::Acquire));
        assert_eq!(completion.stopped_fact(), None);

        pending.store(false, Ordering::Release);
        executor
            .shutdown_with_timeout(Duration::from_secs(1))
            .expect("pending finish release completes shutdown");
        assert_eq!(
            completion
                .stopped_fact()
                .expect("actual stop follows pending finish")
                .conclusion(),
            Err("driver executor is shutting down".to_string())
        );
    }

    #[test]
    fn drop_transfers_a_blocked_worker_to_the_reaper_without_waiting() {
        let executor = GlobalDriverExecutor::new(1);
        let probes = executor.exit_probes();
        let entered = Arc::new(AtomicBool::new(false));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let runtime_state = Arc::new(RuntimeState::default());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            Some((42_001, 42_002)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        executor.submit(vec![DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(BlockingSource {
                    entered: Arc::clone(&entered),
                    gate: Arc::clone(&gate),
                })],
                None,
                Vec::new(),
                runtime_state,
                Some((42_001, 42_002)),
            ),
            completion,
            fragment_ctx,
            Duration::from_millis(1),
        )]);
        assert!(wait_until(Duration::from_secs(1), || entered.load(Ordering::Acquire)));

        let started = Instant::now();
        drop(executor);
        assert!(started.elapsed() < Duration::from_millis(50));

        let (released, cv) = &*gate;
        *released.lock().expect("blocking source gate lock") = true;
        cv.notify_all();
        assert!(wait_until(Duration::from_secs(1), || probes.all_exited()));
    }

    #[test]
    fn submission_after_shutdown_is_rejected_with_actual_stop() {
        let executor = GlobalDriverExecutor::new(1);
        executor.shutdown().expect("executor shutdown");
        let runtime_state = Arc::new(RuntimeState::default());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            Some((31_001, 31_002)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                Vec::new(),
                None,
                Vec::new(),
                runtime_state,
                Some((31_001, 31_002)),
            ),
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(1),
        );

        assert!(!executor.submit(vec![task]));

        let stopped = completion
            .stopped_fact()
            .expect("rejected driver must publish actual stop");
        assert_eq!(
            stopped.conclusion(),
            Err("driver executor is shutting down".to_string())
        );
    }

    #[test]
    fn rejected_pending_finish_does_not_forge_actual_stop() {
        let executor = GlobalDriverExecutor::new(1);
        executor.shutdown().expect("executor shutdown");
        let pending = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let runtime_state = Arc::new(RuntimeState::default());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            Some((32_001, 32_002)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(PendingAbortOperator {
                    pending,
                    observed: Arc::clone(&observed),
                    cancel_requested: Arc::clone(&cancel_requested),
                })],
                None,
                Vec::new(),
                runtime_state,
                Some((32_001, 32_002)),
            ),
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(1),
        );

        assert!(!executor.submit(vec![task]));

        assert!(cancel_requested.load(Ordering::Acquire));
        assert!(observed.load(Ordering::Acquire));
        assert_eq!(
            completion.conclusion(),
            Some(Err("driver executor is shutting down".to_string()))
        );
        assert_eq!(completion.stopped_fact(), None);
        assert_eq!(completion.remaining_drivers(), 1);
    }
}
