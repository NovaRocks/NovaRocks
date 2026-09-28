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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use super::driver::{DriverState, PendingFinishWait, PipelineDriver};
use super::fragment_context::FragmentContext;
use super::operator::{BlockedReason, DriverBlockDeadline};
use super::schedule::event_scheduler::EventDispatcher;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::dispatch_metrics::{DispatchTransition, observe_dispatch};
use tracing::error;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REJECTED_PENDING_DRIVERS: usize = 256;

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

fn invoke_stopped_observer(observer: FragmentStoppedObserver, stopped: FragmentStoppedFact) {
    // The stopped fact is frozen before notification. A faulty observer cannot
    // revoke that fact or prevent the remaining one-shot observers from seeing it.
    if catch_unwind(AssertUnwindSafe(|| observer(stopped))).is_err() {
        tracing::error!("fragment stopped observer panicked after the fact was frozen");
    }
}

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
            invoke_stopped_observer(observer, stopped.clone());
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
            invoke_stopped_observer(
                observer
                    .take()
                    .expect("stopped observer was not registered"),
                stopped,
            );
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
    /// Held from admission until the task is dropped.
    lease: Option<ExecutorTaskLease>,
    /// When the task last entered the ready queue.
    queued_at: Option<Instant>,
}

/// Counts one admitted driver until its task is dropped.
///
/// The executor keeps its workers, and they keep the event dispatcher, until
/// every admitted driver has ended: a closing driver still waits for its
/// finish watches, and only a worker can complete it.
struct ExecutorTaskLease {
    shared: Weak<ExecutorShared>,
}

impl ExecutorTaskLease {
    fn acquire(shared: &Arc<ExecutorShared>) -> Self {
        shared.live_tasks.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: Arc::downgrade(shared),
        }
    }
}

impl Drop for ExecutorTaskLease {
    fn drop(&mut self) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        if shared.live_tasks.fetch_sub(1, Ordering::AcqRel) == 1
            && shared.shutdown.load(Ordering::Acquire)
        {
            // Workers read the count under the queue lock before they wait,
            // so taking it here cannot lose their last wake-up. No task is
            // ever dropped while its thread holds this lock.
            let _queue = shared.queue.lock().expect("global executor queue lock");
            shared.cv.notify_all();
        }
    }
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
            lease: None,
            queued_at: None,
        }
    }

    /// Stamps the task entering the ready queue.
    pub(crate) fn mark_queued(&mut self) {
        self.queued_at = Some(Instant::now());
    }

    /// Records the ready-queue wait of a task a worker starts.
    fn observe_worker_start(&mut self) {
        if let Some(queued_at) = self.queued_at.take() {
            observe_dispatch(DispatchTransition::EnqueueToWorker, queued_at.elapsed());
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

    pub(crate) fn reject_due_to_executor_shutdown(mut self) -> Option<Self> {
        self.fail("driver executor is shutting down".to_string());
        if self.driver.cancel_for_fragment_abort() == DriverState::PendingFinish {
            // Keep ownership until asynchronous operator cleanup actually exits.
            return Some(self);
        }
        self.driver_finished();
        None
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

    pub(crate) fn blocked_terminal_snapshot(&self) -> Option<(Arc<Observable>, u64)> {
        self.driver.blocked_terminal_snapshot()
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

    /// Asks the driver's operators about pending finish work; see
    /// [`PipelineDriver::pending_finish_wait_on_worker`].
    pub(crate) fn refresh_pending_finish_wait(&mut self) -> Option<PendingFinishWait> {
        self.driver.pending_finish_wait_on_worker()
    }

    pub(crate) fn operator_terminal_signal_delivered(&self) -> bool {
        self.driver.operator_terminal_signal_delivered()
    }

    pub(crate) fn try_mark_finish_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        self.driver.try_mark_finish_observer_registered(observable)
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
    /// Admitted drivers that have not ended yet, queued or parked.
    pub(crate) live_tasks: AtomicUsize,
    #[cfg(test)]
    pub(crate) live_workers: AtomicUsize,
}

impl ExecutorShared {
    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            admission_closed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            live_tasks: AtomicUsize::new(0),
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
    admission_gate: Mutex<()>,
    event_dispatcher: EventDispatcher,
    lifecycle: Arc<ExecutorLifecycleOwner>,
}

#[cfg(test)]
pub(crate) struct DriverExecutorExitProbes {
    shared: Arc<ExecutorShared>,
    event_exited: Arc<AtomicBool>,
}

#[cfg(test)]
impl DriverExecutorExitProbes {
    pub(crate) fn all_exited(&self) -> bool {
        self.shared.live_workers.load(Ordering::Acquire) == 0
            && self.event_exited.load(Ordering::Acquire)
    }
}

impl GlobalDriverExecutor {
    pub fn new(num_threads: usize) -> Self {
        let num_threads = num_threads.max(1);
        let shared = Arc::new(ExecutorShared::new());
        let event_dispatcher = EventDispatcher::new();

        let mut workers = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let shared_cloned = Arc::clone(&shared);
            workers.push(thread::spawn(move || worker_loop(shared_cloned)));
        }

        Self {
            shared,
            admission_gate: Mutex::new(()),
            event_dispatcher,
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
    pub fn submit(&self, mut tasks: Vec<DriverTask>) -> bool {
        if tasks.is_empty() {
            return true;
        }

        // Keep the admission verdict atomic with scheduler attachment and
        // enqueue. A rejected task must not register any runtime activity.
        let admission = self
            .admission_gate
            .lock()
            .expect("global executor admission gate lock");
        if self.shared.admission_closed.load(Ordering::Acquire) {
            drop(admission);
            abort_tasks(tasks);
            return false;
        }
        // Attachment stays outside the worker queue lock. Observable
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
        for task in &mut tasks {
            task.lease = Some(ExecutorTaskLease::acquire(&self.shared));
            task.mark_queued();
        }
        queue.extend(tasks);
        self.shared.cv.notify_all();
        drop(admission);
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
        let dispatcher = self.event_dispatcher.take_closer();
        let lifecycle = Arc::clone(&self.lifecycle);
        thread::Builder::new()
            .name("driver_executor_reaper".to_string())
            .spawn(move || {
                // Workers leave only once every admitted driver has ended.
                // Until then a closing driver may still wait for a finish
                // watch, which only the dispatcher delivers.
                for worker in workers {
                    let _ = worker.join();
                }
                if let Some(dispatcher) = dispatcher {
                    let leftover = dispatcher.close_and_join();
                    if !leftover.is_empty() {
                        error!(
                            "driver executor closed its event dispatcher with {} unadmitted parked drivers",
                            leftover.len()
                        );
                    }
                }
                let mut state = lifecycle.state.lock().expect("executor lifecycle lock");
                state.phase = ExecutorShutdownPhase::Joined;
                lifecycle.cv.notify_all();
            })
            .expect("spawn driver executor reaper");
    }

    fn begin_shutdown(&self) {
        {
            let _admission = self
                .admission_gate
                .lock()
                .expect("global executor admission gate lock");
            self.shared.admission_closed.store(true, Ordering::Release);
        }
        // Every admitted driver observes the shutdown on a worker, so wake the
        // parked ones. The dispatcher stays open: a closing driver still
        // waits for its asynchronous owners' finish watches.
        self.event_dispatcher.wake_all_blocked();
        let _queue = self
            .shared
            .queue
            .lock()
            .expect("global executor queue lock");
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.cv.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn exit_probes(&self) -> DriverExecutorExitProbes {
        DriverExecutorExitProbes {
            shared: Arc::clone(&self.shared),
            event_exited: self.event_dispatcher.exit_probe(),
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
        if let Some(task) = task.reject_due_to_executor_shutdown() {
            rejected_driver_reaper().enqueue(task);
        }
    }
}

struct RejectedDriverReaperState {
    pending: VecDeque<DriverTask>,
    retained: usize,
}

struct RejectedDriverReaper {
    state: Mutex<RejectedDriverReaperState>,
    wake: Condvar,
    capacity: Condvar,
    #[cfg(test)]
    threads_started: AtomicUsize,
}

impl RejectedDriverReaper {
    fn enqueue(&self, task: DriverTask) {
        let mut state = self.state.lock().expect("rejected driver reaper lock");
        while state.retained >= MAX_REJECTED_PENDING_DRIVERS {
            state = self
                .capacity
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.retained += 1;
        state.pending.push_back(task);
        self.wake.notify_one();
    }
}

fn rejected_driver_reaper() -> &'static Arc<RejectedDriverReaper> {
    static REAPER: OnceLock<Arc<RejectedDriverReaper>> = OnceLock::new();
    REAPER.get_or_init(|| {
        let reaper = Arc::new(RejectedDriverReaper {
            state: Mutex::new(RejectedDriverReaperState {
                pending: VecDeque::new(),
                retained: 0,
            }),
            wake: Condvar::new(),
            capacity: Condvar::new(),
            #[cfg(test)]
            threads_started: AtomicUsize::new(0),
        });
        let owner = Arc::clone(&reaper);
        thread::Builder::new()
            .name("rejected_driver_reaper".to_string())
            .spawn(move || run_rejected_driver_reaper(owner))
            .expect("spawn rejected driver reaper");
        reaper
    })
}

fn run_rejected_driver_reaper(reaper: Arc<RejectedDriverReaper>) {
    #[cfg(test)]
    reaper.threads_started.fetch_add(1, Ordering::AcqRel);
    loop {
        let mut pending = {
            let mut state = reaper.state.lock().expect("rejected driver reaper lock");
            while state.pending.is_empty() {
                state = reaper
                    .wake
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
            }
            std::mem::take(&mut state.pending)
        };
        let mut still_pending = VecDeque::new();
        while let Some(task) = pending.pop_front() {
            if !task.has_pending_finish() {
                task.driver_finished();
                let mut state = reaper.state.lock().expect("rejected driver reaper lock");
                state.retained -= 1;
                reaper.capacity.notify_one();
            } else {
                still_pending.push_back(task);
            }
        }
        let mut state = reaper.state.lock().expect("rejected driver reaper lock");
        state.pending.extend(still_pending);
        if !state.pending.is_empty() {
            drop(state);
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Parks a driver whose asynchronous owners still hold finish work on its
/// fragment's event scheduler until one of their finish watches fires.
fn park_pending_finish(task: DriverTask) {
    let scheduler = task.fragment_ctx().event_scheduler();
    if let Err(task) = scheduler.add_pending_finish(task) {
        // A fragment scheduler refuses parking only after it closed, when no
        // wake-up can reach this driver again. Dropping the task releases its
        // local handles without forging the stopped fact its asynchronous
        // owner still withholds.
        error!(
            "event scheduler refused a pending-finish driver: finst={:?} driver_id={}",
            task.fragment_instance_id(),
            task.driver_id()
        );
        rejected_driver_reaper().enqueue(*task);
    }
}

fn worker_loop(shared: Arc<ExecutorShared>) {
    #[cfg(test)]
    let _worker_guard = WorkerCountGuard::new(&shared.live_workers);
    loop {
        let mut task = {
            let mut queue = shared.queue.lock().expect("global executor queue lock");
            loop {
                if let Some(task) = queue.pop_front() {
                    break task;
                }
                // Stay while an admitted driver is parked: waking it needs a
                // worker, even after shutdown began.
                if shared.shutdown.load(Ordering::Acquire)
                    && shared.live_tasks.load(Ordering::Acquire) == 0
                {
                    return;
                }
                queue = shared
                    .cv
                    .wait(queue)
                    .expect("global executor queue condvar wait");
            }
        };
        task.observe_worker_start();

        if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort() {
            if shared.admission_closed.load(Ordering::Acquire) {
                task.fail("driver executor is shutting down".to_string());
            }
            if let Some(task) = task.finish_due_to_abort() {
                park_pending_finish(task);
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
            let error = format!("panic in driver execution: {msg}");
            task.fail(error.clone());
            task.driver.fail_after_panic(error)
        });

        if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort() {
            if shared.admission_closed.load(Ordering::Acquire) {
                task.fail("driver executor is shutting down".to_string());
            }
            if let Some(task) = task.finish_due_to_abort() {
                park_pending_finish(task);
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
                        park_pending_finish(task);
                    }
                    continue;
                }
                task.mark_queued();
                let mut queue = shared.queue.lock().expect("global executor queue lock");
                queue.push_back(task);
                shared.cv.notify_one();
            }
            DriverState::Blocked(reason) => {
                if task.should_abort_immediately() {
                    if let Some(task) = task.finish_due_to_abort() {
                        park_pending_finish(task);
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
                                    park_pending_finish(task);
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
                                    park_pending_finish(task);
                                }
                            }
                        }
                    }
                }
            }
            DriverState::PendingFinish => {
                park_pending_finish(task);
            }
            DriverState::Finished => {
                if task.has_pending_finish() {
                    // A terminal driver state must not complete the fragment while an
                    // asynchronous operator is still publishing its final output.
                    park_pending_finish(task);
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
    use std::sync::mpsc;
    use std::thread;

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::operator::{FinishWatch, Operator, ProcessorOperator};
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
        only_after_cancel: bool,
    }

    impl Operator for PendingAbortOperator {
        fn name(&self) -> &str {
            "pending_abort"
        }

        fn cancel(&mut self) {
            self.cancel_requested.store(true, Ordering::Release);
        }

        fn pending_finish(&self) -> Option<FinishWatch> {
            if self.only_after_cancel && !self.cancel_requested.load(Ordering::Acquire) {
                return None;
            }
            // An owner without a completion event: the driver rechecks.
            self.observed.store(true, Ordering::Release);
            self.pending
                .load(Ordering::Acquire)
                .then_some(FinishWatch::RecheckAfter(Duration::from_millis(1)))
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

    struct PanickingSource;

    impl Operator for PanickingSource {
        fn name(&self) -> &str {
            "panicking_source"
        }

        fn activate(&mut self, _state: &RuntimeState) -> Result<(), String> {
            panic!("injected driver panic")
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for PanickingSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            panic!("injected driver panic")
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            unreachable!("panicking source receives no input")
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            unreachable!("panicking source produces no output")
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
    fn stopped_observer_panic_before_transition_is_isolated() {
        let completion = FragmentCompletion::new(1);
        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        completion.subscribe_stopped(Box::new(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            first_callback.fetch_add(1, Ordering::SeqCst);
        }));
        completion.subscribe_stopped(Box::new(|_| {
            panic!("injected stopped observer panic");
        }));
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        completion.subscribe_stopped(Box::new(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            last_callback.fetch_add(1, Ordering::SeqCst);
        }));

        assert!(completion.driver_finished());
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
        assert_eq!(
            completion
                .stopped_fact()
                .expect("the terminal transition freezes a stopped fact")
                .conclusion(),
            Ok(())
        );
        assert!(!completion.driver_finished());
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stopped_observer_panic_after_transition_is_isolated() {
        let completion = FragmentCompletion::new(0);
        let frozen = completion
            .stopped_fact()
            .expect("zero-driver completion starts stopped");
        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        let expected = frozen.clone();
        completion.subscribe_stopped(Box::new(move |fact| {
            assert_eq!(fact, expected);
            first_callback.fetch_add(1, Ordering::SeqCst);
        }));
        completion.subscribe_stopped(Box::new(|_| {
            panic!("injected immediate stopped observer panic");
        }));
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        completion.subscribe_stopped(Box::new(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            last_callback.fetch_add(1, Ordering::SeqCst);
        }));

        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
        assert_eq!(completion.stopped_fact(), Some(frozen));
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

        executor.shutdown().expect("first executor shutdown");
        executor.shutdown().expect("repeated executor shutdown");

        assert!(shared.admission_closed.load(Ordering::Acquire));
        assert!(shared.shutdown.load(Ordering::Acquire));
        assert_eq!(shared.live_workers.load(Ordering::Acquire), 0);
        assert!(event_exited.load(Ordering::Acquire));
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

            drop(executor);

            assert!(wait_until(Duration::from_secs(1), || {
                shared.live_workers.load(Ordering::Acquire) == 0
                    && event_exited.load(Ordering::Acquire)
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
                    only_after_cancel: false,
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

    /// A finished operator whose asynchronous finish work ends when the test
    /// says so, announced on `observable` unless it has no completion event.
    struct WatchedPendingOperator {
        pending: Arc<AtomicBool>,
        observable: Option<Arc<Observable>>,
        checks: Arc<AtomicUsize>,
        cancelled: Arc<AtomicBool>,
    }

    impl Operator for WatchedPendingOperator {
        fn name(&self) -> &str {
            "watched_pending"
        }

        fn cancel(&mut self) {
            self.cancelled.store(true, Ordering::Release);
        }

        fn is_finished(&self) -> bool {
            true
        }

        fn pending_finish(&self) -> Option<FinishWatch> {
            self.checks.fetch_add(1, Ordering::AcqRel);
            if !self.pending.load(Ordering::Acquire) {
                return None;
            }
            Some(match self.observable.as_ref() {
                Some(observable) => FinishWatch::Notify(Arc::clone(observable)),
                None => FinishWatch::RecheckAfter(Duration::from_millis(5)),
            })
        }
    }

    struct PendingHarness {
        pending: Arc<AtomicBool>,
        observable: Option<Arc<Observable>>,
        checks: Arc<AtomicUsize>,
        cancelled: Arc<AtomicBool>,
        completion: Arc<FragmentCompletion>,
        fragment_ctx: Arc<FragmentContext>,
    }

    impl PendingHarness {
        fn submit(executor: &GlobalDriverExecutor, finst: (i64, i64), notify: bool) -> Self {
            let pending = Arc::new(AtomicBool::new(true));
            let observable = notify.then(|| Arc::new(Observable::new()));
            let checks = Arc::new(AtomicUsize::new(0));
            let cancelled = Arc::new(AtomicBool::new(false));
            let runtime_state = Arc::new(RuntimeState::default());
            let fragment_ctx = Arc::new(FragmentContext::new(
                None,
                Arc::clone(&runtime_state),
                Some(finst),
                None,
                None,
                None,
            ));
            let completion = FragmentCompletion::new(1);
            assert!(executor.submit(vec![DriverTask::new(
                PipelineDriver::new(
                    1,
                    vec![Box::new(WatchedPendingOperator {
                        pending: Arc::clone(&pending),
                        observable: observable.clone(),
                        checks: Arc::clone(&checks),
                        cancelled: Arc::clone(&cancelled),
                    })],
                    None,
                    Vec::new(),
                    runtime_state,
                    Some(finst),
                ),
                Arc::clone(&completion),
                Arc::clone(&fragment_ctx),
                Duration::from_millis(1),
            )]));
            Self {
                pending,
                observable,
                checks,
                cancelled,
                completion,
                fragment_ctx,
            }
        }

        /// Waits until the driver asked about its pending work and stopped
        /// asking, i.e. it is parked.
        fn wait_parked(&self) -> usize {
            assert!(wait_until(Duration::from_secs(1), || {
                self.checks.load(Ordering::Acquire) > 0
            }));
            let mut settled = self.checks.load(Ordering::Acquire);
            loop {
                thread::sleep(Duration::from_millis(20));
                let now = self.checks.load(Ordering::Acquire);
                if now == settled {
                    return now;
                }
                settled = now;
            }
        }

        fn finish_work(&self) {
            self.pending.store(false, Ordering::Release);
            if let Some(observable) = self.observable.as_ref() {
                observable.notify_observers();
            }
        }
    }

    #[test]
    fn pending_finish_parks_on_its_watch_until_notified() {
        let executor = GlobalDriverExecutor::new(1);
        let harness = PendingHarness::submit(&executor, (51_001, 51_002), true);

        let settled = harness.wait_parked();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            harness.checks.load(Ordering::Acquire),
            settled,
            "a parked pending-finish driver must not be polled"
        );
        assert_eq!(harness.completion.stopped_fact(), None);

        harness.finish_work();
        assert!(wait_until(Duration::from_secs(1), || {
            harness.completion.stopped_fact().is_some()
        }));
        assert_eq!(harness.completion.conclusion(), Some(Ok(())));
        executor.shutdown().expect("executor shutdown");
    }

    #[test]
    fn pending_finish_without_notification_is_rechecked_at_its_interval() {
        let executor = GlobalDriverExecutor::new(1);
        let harness = PendingHarness::submit(&executor, (52_001, 52_002), false);
        assert!(wait_until(Duration::from_secs(1), || {
            harness.checks.load(Ordering::Acquire) > 0
        }));

        let before = harness.checks.load(Ordering::Acquire);
        thread::sleep(Duration::from_millis(100));
        let rechecks = harness.checks.load(Ordering::Acquire) - before;
        // About one recheck every 5 ms, a few questions each: a bounded wait,
        // not a spin over the global ready queue.
        assert!(rechecks > 0, "a recheck deadline must fire");
        assert!(rechecks < 200, "rechecked {rechecks} times in 100 ms");
        assert_eq!(harness.completion.stopped_fact(), None);

        harness.finish_work();
        assert!(wait_until(Duration::from_secs(1), || {
            harness.completion.stopped_fact().is_some()
        }));
        assert_eq!(harness.completion.conclusion(), Some(Ok(())));
        executor.shutdown().expect("executor shutdown");
    }

    #[test]
    fn a_closing_driver_waits_for_its_owner_without_spinning() {
        let executor = GlobalDriverExecutor::new(1);
        let harness = PendingHarness::submit(&executor, (53_001, 53_002), true);
        harness.wait_parked();

        // The fragment fails elsewhere: the parked driver gets one turn to
        // deliver cancellation, then waits for its owner again.
        assert!(harness.completion.fail("injected failure".to_string()));
        harness
            .fragment_ctx
            .set_final_status("injected failure".to_string());
        assert!(wait_until(Duration::from_secs(1), || {
            harness.cancelled.load(Ordering::Acquire)
        }));
        let settled = harness.wait_parked();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            harness.checks.load(Ordering::Acquire),
            settled,
            "an aborted driver whose operators were cancelled must not be requeued until its owner ends"
        );
        assert_eq!(harness.completion.stopped_fact(), None);

        harness.finish_work();
        assert!(wait_until(Duration::from_secs(1), || {
            harness.completion.stopped_fact().is_some()
        }));
        assert_eq!(
            harness.completion.conclusion(),
            Some(Err("injected failure".to_string()))
        );
        executor.shutdown().expect("executor shutdown");
    }

    #[test]
    fn shutdown_keeps_the_dispatcher_until_a_notified_owner_ends() {
        let executor = GlobalDriverExecutor::new(1);
        let probes = executor.exit_probes();
        let harness = PendingHarness::submit(&executor, (54_001, 54_002), true);
        harness.wait_parked();

        let error = executor
            .shutdown_with_timeout(Duration::from_millis(20))
            .expect_err("a live owner keeps shutdown incomplete");
        assert!(error.contains("did not stop within"));
        assert!(harness.cancelled.load(Ordering::Acquire));
        assert!(
            !probes.all_exited(),
            "workers and the dispatcher stay while a driver is closing"
        );

        // Only the dispatcher can deliver this wake-up.
        harness.finish_work();
        executor
            .shutdown_with_timeout(Duration::from_secs(1))
            .expect("the owner's end completes shutdown");
        assert!(probes.all_exited());
        assert_eq!(
            harness.completion.conclusion(),
            Some(Err("driver executor is shutting down".to_string()))
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
        let shared_owners_before_submit = Arc::strong_count(&executor.shared);
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
            Arc::clone(&fragment_ctx),
            Duration::from_millis(1),
        );

        assert!(!executor.submit(vec![task]));
        assert_eq!(
            Arc::strong_count(&executor.shared),
            shared_owners_before_submit,
            "rejected submission must not attach its event scheduler"
        );

        let stopped = completion
            .stopped_fact()
            .expect("rejected driver must publish actual stop");
        assert_eq!(
            stopped.conclusion(),
            Err("driver executor is shutting down".to_string())
        );
    }

    #[test]
    fn driver_panic_waits_for_operator_cleanup_before_actual_stop() {
        let executor = GlobalDriverExecutor::new(1);
        let pending = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let runtime_state = Arc::new(RuntimeState::default());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            Some((33_001, 33_002)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![
                    Box::new(PanickingSource),
                    Box::new(PendingAbortOperator {
                        pending: Arc::clone(&pending),
                        observed: Arc::clone(&observed),
                        cancel_requested: Arc::clone(&cancel_requested),
                        only_after_cancel: true,
                    }),
                ],
                None,
                Vec::new(),
                runtime_state,
                Some((33_001, 33_002)),
            ),
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_secs(1),
        );
        assert!(executor.submit(vec![task]));

        assert!(wait_until(Duration::from_secs(1), || {
            cancel_requested.load(Ordering::Acquire)
                && observed.load(Ordering::Acquire)
                && completion.conclusion().is_some()
        }));
        assert_eq!(completion.stopped_fact(), None);
        assert_eq!(completion.remaining_drivers(), 1);
        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(1), || completion
            .stopped_fact()
            .is_some()));
        assert_eq!(completion.remaining_drivers(), 0);
        assert!(
            completion
                .stopped_fact()
                .expect("driver stopped after cleanup")
                .conclusion()
                .expect_err("panic is a failure")
                .contains("panic in driver execution: injected driver panic")
        );
    }

    #[test]
    fn rejected_pending_finish_waits_for_actual_stop() {
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
                    pending: Arc::clone(&pending),
                    observed: Arc::clone(&observed),
                    cancel_requested: Arc::clone(&cancel_requested),
                    only_after_cancel: false,
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
        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(1), || completion
            .stopped_fact()
            .is_some()));
        assert_eq!(completion.remaining_drivers(), 0);
        assert_eq!(
            completion
                .stopped_fact()
                .expect("rejected driver stopped")
                .conclusion(),
            Err("driver executor is shutting down".to_string())
        );
    }

    #[test]
    fn closed_event_scheduler_retains_pending_finish_and_executor_lease() {
        let shared = Arc::new(ExecutorShared::new());
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
        let mut task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(PendingAbortOperator {
                    pending: Arc::clone(&pending),
                    observed: Arc::clone(&observed),
                    cancel_requested: Arc::clone(&cancel_requested),
                    only_after_cancel: false,
                })],
                None,
                Vec::new(),
                runtime_state,
                Some((32_001, 32_002)),
            ),
            Arc::clone(&completion),
            Arc::clone(&fragment_ctx),
            Duration::from_millis(1),
        );

        task.lease = Some(ExecutorTaskLease::acquire(&shared));
        let task = task
            .reject_due_to_executor_shutdown()
            .expect("asynchronous owner still holds the task");
        fragment_ctx.event_scheduler().shutdown();
        park_pending_finish(task);
        assert_eq!(shared.live_tasks.load(Ordering::Acquire), 1);

        assert!(cancel_requested.load(Ordering::Acquire));
        assert!(observed.load(Ordering::Acquire));
        assert_eq!(
            completion.conclusion(),
            Some(Err("driver executor is shutting down".to_string()))
        );
        assert_eq!(completion.stopped_fact(), None);
        assert_eq!(completion.remaining_drivers(), 1);
        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(1), || completion
            .stopped_fact()
            .is_some()));
        assert_eq!(completion.remaining_drivers(), 0);
        assert!(wait_until(Duration::from_secs(1), || shared
            .live_tasks
            .load(Ordering::Acquire)
            == 0));
        assert_eq!(
            completion
                .stopped_fact()
                .expect("rejected driver stopped")
                .conclusion(),
            Err("driver executor is shutting down".to_string())
        );
    }

    #[test]
    fn concurrent_rejected_drivers_share_one_bounded_cleanup_owner() {
        let executor = Arc::new(GlobalDriverExecutor::new(1));
        executor.shutdown().expect("executor shutdown");
        let pending = Arc::new(AtomicBool::new(true));
        let (tx, rx) = mpsc::channel();
        let mut submitters = Vec::new();
        for group in 0..8 {
            let executor = Arc::clone(&executor);
            let pending = Arc::clone(&pending);
            let tx = tx.clone();
            submitters.push(thread::spawn(move || {
                for ordinal in 0..8 {
                    let identity = group * 8 + ordinal;
                    let runtime_state = Arc::new(RuntimeState::default());
                    let fragment_ctx = Arc::new(FragmentContext::new(
                        None,
                        Arc::clone(&runtime_state),
                        Some((34_001, identity)),
                        None,
                        None,
                        None,
                    ));
                    let completion = FragmentCompletion::new(1);
                    let observed = Arc::new(AtomicBool::new(false));
                    let cancel_requested = Arc::new(AtomicBool::new(false));
                    let task = DriverTask::new(
                        PipelineDriver::new(
                            1,
                            vec![Box::new(PendingAbortOperator {
                                pending: Arc::clone(&pending),
                                observed: Arc::clone(&observed),
                                cancel_requested: Arc::clone(&cancel_requested),
                                only_after_cancel: false,
                            })],
                            None,
                            Vec::new(),
                            runtime_state,
                            Some((34_001, identity)),
                        ),
                        Arc::clone(&completion),
                        fragment_ctx,
                        Duration::from_millis(1),
                    );
                    assert!(!executor.submit(vec![task]));
                    tx.send((completion, observed, cancel_requested))
                        .expect("test receives rejection");
                }
            }));
        }
        drop(tx);
        for submitter in submitters {
            submitter.join().expect("rejection submitter");
        }
        let completions = rx.into_iter().collect::<Vec<_>>();
        assert_eq!(completions.len(), 64);
        for (completion, observed, cancel_requested) in &completions {
            assert!(cancel_requested.load(Ordering::Acquire));
            assert!(observed.load(Ordering::Acquire));
            assert_eq!(completion.stopped_fact(), None);
        }
        assert!(wait_until(Duration::from_secs(1), || {
            rejected_driver_reaper()
                .threads_started
                .load(Ordering::Acquire)
                == 1
        }));

        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(3), || completions
            .iter()
            .all(|(completion, _, _)| completion.stopped_fact().is_some())));
        assert_eq!(
            rejected_driver_reaper()
                .threads_started
                .load(Ordering::Acquire),
            1,
            "concurrent rejections must not create per-task cleanup threads"
        );
    }
}
