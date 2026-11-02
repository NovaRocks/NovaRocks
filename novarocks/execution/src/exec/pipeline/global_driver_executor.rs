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

#[cfg(test)]
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

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

#[path = "once_role.rs"]
pub(crate) mod once_role;

#[path = "driver_batch.rs"]
mod driver_batch;
pub(crate) use driver_batch::DriverBatch;
pub(crate) use driver_batch::{Poll as RetirementPoll, Retirement};
pub(crate) struct ParkingPhase {
    roles: [driver_batch::Phase; 2],
}
impl ParkingPhase {
    fn new() -> Self {
        Self {
            roles: std::array::from_fn(|_| driver_batch::Phase::new()),
        }
    }
    fn is_unknown(&self) -> bool {
        self.roles.iter().any(driver_batch::Phase::is_unknown)
    }
}
use driver_batch::{Poll as ReaperPoll, ReaperEntry};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Completion result payload reported when a fragment finishes execution.
pub struct FragmentCompletion {
    mu: Mutex<FragmentCompletionState>,
    cv: Condvar,
    resources_finisher: Arc<once_role::OnceRole<FragmentStoppedFact>>,
    wall_time: Arc<once_role::OnceRole<FragmentStoppedFact>>,
    control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    local_completion: std::sync::OnceLock<crate::runtime::local_completion::LocalCompletionDriver>,
}

/// Immutable proof that every submitted driver has actually stopped.
///
/// A failed conclusion may be available before this fact. Observers must use
/// this fact when they release execution-owned resources or publish a stopped
/// lifecycle transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentStoppedFact {
    conclusion: ExecutionResult<()>,
}

impl FragmentStoppedFact {
    pub fn conclusion(&self) -> ExecutionResult<()> {
        self.conclusion.clone()
    }
}

struct FragmentCompletionState {
    remaining: usize,
    activated: bool,
    // Cold start/capture obligations, separate from actual driver exit receipts.
    start_holds: usize,
    stopped: Option<FragmentStoppedFact>,
}

impl FragmentCompletion {
    pub fn new(
        driver_count: usize,
        control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> Arc<Self> {
        let stopped = (driver_count == 0).then(|| FragmentStoppedFact {
            conclusion: control.error().map_or(Ok(()), Err),
        });
        Arc::new(Self {
            mu: Mutex::new(FragmentCompletionState {
                remaining: driver_count,
                activated: true,
                start_holds: 0,
                stopped,
            }),
            cv: Condvar::new(),
            resources_finisher: once_role::OnceRole::new(once_role::Role::PipelineFinisher),
            wall_time: once_role::OnceRole::new(once_role::Role::WallTime),
            control,
            local_completion: std::sync::OnceLock::new(),
        })
    }

    pub(crate) fn new_prepared(
        driver_count: usize,
        control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> Arc<Self> {
        let completion = Self::new(driver_count, control);
        {
            let mut state = completion.mu.lock().unwrap_or_else(|e| e.into_inner());
            state.activated = false;
            state.stopped = None;
        }
        completion
    }

    pub(crate) fn defer_start(&self) {
        let mut state = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopped.is_none() {
            state.start_holds = state
                .start_holds
                .checked_add(1)
                .expect("start holders are derived from original prepared slots");
            state.activated = false;
        }
    }

    pub(crate) fn activate(&self) {
        let stopped = {
            let mut state = self.mu.lock().unwrap_or_else(|e| e.into_inner());
            if state.start_holds != 0 {
                state.start_holds -= 1;
            } else if state.activated {
                return;
            }
            if state.start_holds != 0 {
                return;
            }
            state.activated = true;
            if state.remaining != 0 {
                return;
            }
            let stopped = FragmentStoppedFact {
                conclusion: self.control.error().map_or(Ok(()), Err),
            };
            state.stopped = Some(stopped.clone());
            self.cv.notify_all();
            stopped
        };
        self.run_completion_roles(&stopped);
    }

    pub fn should_abort(&self) -> bool {
        self.control.is_stopped()
    }

    pub fn fail(&self, err: impl Into<ExecutionFailure>) {
        // The shared owner publishes outside the physical completion lock.
        // A parked driver's notification may reenter completion inspection.
        self.control.set_error(err.into())
    }

    pub fn driver_finished(&self) -> bool {
        let stopped = {
            let mut st = self.mu.lock().expect("fragment completion lock");
            if st.remaining == 0 {
                return false;
            }
            // A real exit receipt can precede the start-work capture's exit.
            // Preserve it; activation later publishes only after both facts.
            st.remaining -= 1;
            if st.remaining != 0 || !st.activated {
                return false;
            }

            let conclusion = self.control.error().map_or(Ok(()), Err);
            let stopped = FragmentStoppedFact { conclusion };
            st.stopped = Some(stopped.clone());
            self.cv.notify_all();
            stopped
        };
        // The body proof is frozen before either named role. Their independent
        // capture scopes cannot revoke it or suppress the sibling role.
        self.run_completion_roles(&stopped);
        true
    }

    /// A failure is the current shared owner's source. Ok only proves that
    /// this group has no remaining drivers, never whole-Task success.
    pub fn conclusion(&self) -> Option<ExecutionResult<()>> {
        if let Some(error) = self.control.error() {
            return Some(Err(error));
        }
        self.mu
            .lock()
            .expect("fragment completion lock")
            .stopped
            .as_ref()
            .map(|_| Ok(()))
    }

    pub fn stopped_fact(&self) -> Option<FragmentStoppedFact> {
        self.mu
            .lock()
            .expect("fragment completion lock")
            .stopped
            .clone()
    }

    pub(crate) fn resources_finisher(&self) -> Arc<once_role::OnceRole<FragmentStoppedFact>> {
        Arc::clone(&self.resources_finisher)
    }

    pub(crate) fn admit_completion_work(
        &self,
        finisher: novarocks_execution_contract::CompletionWork,
        wall_time: novarocks_execution_contract::CompletionWork,
    ) {
        self.resources_finisher
            .admit_work(&mut Some(finisher))
            .expect("original dormant finisher has no bound callback or prior work");
        self.wall_time
            .admit_work(&mut Some(wall_time))
            .expect("original dormant wall-time role has no bound callback or prior work");
    }

    pub(crate) fn admit_local_completion_driver(
        &self,
        driver: crate::runtime::local_completion::LocalCompletionDriver,
    ) {
        assert!(
            self.local_completion.set(driver).is_ok(),
            "one local completion driver"
        );
    }

    fn run_completion_roles(&self, stopped: &FragmentStoppedFact) {
        self.wall_time.invoke(stopped, &self.control);
        self.resources_finisher.invoke(stopped, &self.control);
        if let Some(driver) = self.local_completion.get() {
            // The original callback's stack and return-publication step have
            // finished before this taskless barrier can invoke its one child.
            driver.run();
        }
    }

    /// Bind the unique original resource owner before submitting its drivers.
    pub(crate) fn bind_resources_finisher(
        &self,
        slot: &mut Option<once_role::Callback<FragmentStoppedFact>>,
    ) -> ExecutionResult<()> {
        self.resources_finisher.bind(slot)?;
        if let Some(stopped) = self.stopped_fact() {
            self.resources_finisher.invoke(&stopped, &self.control);
            if let Some(driver) = self.local_completion.get() {
                driver.run();
            }
        }
        Ok(())
    }

    pub(crate) fn bind_wall_time(
        &self,
        slot: &mut Option<once_role::Callback<FragmentStoppedFact>>,
    ) -> ExecutionResult<()> {
        self.wall_time.bind(slot)?;
        if let Some(stopped) = self.stopped_fact() {
            self.wall_time.invoke(&stopped, &self.control);
        }
        Ok(())
    }

    pub fn wait(&self) -> ExecutionResult<()> {
        let mut st = self.mu.lock().expect("fragment completion lock");
        while st.remaining > 0 {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        drop(st);
        self.control.error().map_or(Ok(()), Err)
    }

    pub fn wait_timeout(&self, timeout: Duration, err: ExecutionFailure) -> ExecutionResult<()> {
        self.wait_timeout_with_local_cancel(timeout, err, || {})
    }

    pub(crate) fn wait_timeout_with_local_cancel<F>(
        &self,
        timeout: Duration,
        err: ExecutionFailure,
        on_timeout: F,
    ) -> ExecutionResult<()>
    where
        F: FnOnce(),
    {
        let deadline = Instant::now() + timeout;
        let mut st = self.mu.lock().expect("fragment completion lock");
        while st.remaining > 0 {
            let now = Instant::now();
            if now >= deadline {
                drop(st);
                // Time does not prove exit. Submit the real deadline cause,
                // request local stand-down even if an earlier cause won, and
                // still join the exact submitted driver group.
                self.fail(err);
                on_timeout();
                return self.wait();
            }
            let (guard, _) = self
                .cv
                .wait_timeout(st, deadline.saturating_duration_since(now))
                .unwrap_or_else(|e| e.into_inner());
            st = guard;
        }
        drop(st);
        self.control.error().map_or(Ok(()), Err)
    }

    #[cfg(test)]
    fn bind_test_resources_finisher(
        &self,
        mut callback: impl FnMut(&FragmentStoppedFact) + Send + 'static,
    ) {
        let mut slot: Option<once_role::Callback<FragmentStoppedFact>> =
            Some(Box::new(move |fact| {
                callback(fact);
                true
            }));
        self.bind_resources_finisher(&mut slot)
            .expect("one test resource owner");
    }
    #[cfg(test)]
    fn bind_test_wall_time(&self, mut callback: impl FnMut(&FragmentStoppedFact) + Send + 'static) {
        let mut slot: Option<once_role::Callback<FragmentStoppedFact>> =
            Some(Box::new(move |fact| {
                callback(fact);
                true
            }));
        self.bind_wall_time(&mut slot)
            .expect("one test wall-time owner");
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
        assert!(
            Arc::ptr_eq(
                &completion.control,
                &fragment_ctx.runtime_state().error_state()
            ),
            "driver completion must use its runtime's exact failure owner"
        );
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

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn writer_diagnostic(
        &self,
    ) -> Option<(
        crate::runtime::writer_diagnostics::WriterDiagnostics,
        String,
        String,
    )> {
        crate::runtime::writer_diagnostics::WriterDiagnostics::from_runtime(
            self.fragment_ctx.runtime_state(),
        )
        .map(|scope| {
            (
                scope,
                self.driver.source_name().to_owned(),
                self.driver.sink_name().to_owned(),
            )
        })
    }

    pub(crate) fn fragment_instance_id(&self) -> Option<(i64, i64)> {
        self.driver.fragment_instance_id()
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn exchange_test_observer(
        &self,
    ) -> Option<Arc<dyn crate::runtime::exchange::TestEventObserver>> {
        self.fragment_ctx.runtime_state().exchange_test_observer()
    }

    pub(crate) fn fragment_ctx(&self) -> &Arc<FragmentContext> {
        &self.fragment_ctx
    }

    pub(crate) fn should_abort(&self) -> bool {
        self.completion.should_abort()
    }

    #[cfg(test)]
    pub(crate) fn has_pending_finish(&mut self) -> bool {
        self.driver.has_pending_finish()
    }

    pub(crate) fn should_abort_immediately(&self) -> bool {
        self.should_abort()
    }

    pub(crate) fn finish_due_to_abort(self) -> Option<Self> {
        Retirement::on_worker(self, true, None)
    }
    fn finish_abort_with_failure(self, cause: Option<ExecutionFailure>) -> Option<Self> {
        Retirement::on_worker(self, true, cause)
    }
    #[cfg(test)]
    pub(crate) fn reject_due_to_executor_shutdown(self) -> Option<Self> {
        Retirement::on_worker_shutdown(self)
    }
    fn finish_abort_for_control(self, executor_closed: bool) -> Option<Self> {
        if executor_closed {
            Retirement::on_worker_shutdown(self)
        } else {
            self.finish_due_to_abort()
        }
    }
    pub(crate) fn driver_finished(self) {
        if let Some(task) = Retirement::on_worker(self, false, None) {
            park_pending_finish(task);
        }
    }
    fn finish_with_failure(self, cause: ExecutionFailure) {
        if let Some(task) = Retirement::on_worker(self, false, Some(cause)) {
            park_pending_finish(task);
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
    pub(crate) fn refresh_pending_finish_wait(
        &mut self,
    ) -> Result<Option<PendingFinishWait>, crate::runtime::observable::ObservableCapacityError>
    {
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

#[derive(Debug)]
struct ExecutorDrainDeadline {
    timeout: Duration,
    live_tasks: usize,
}
impl std::fmt::Display for ExecutorDrainDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "driver executor did not stop within {} ms ({} live tasks)",
            self.timeout.as_millis(),
            self.live_tasks
        )
    }
}
impl std::error::Error for ExecutorDrainDeadline {}
impl novarocks_failure_contract::FailureProjection for ExecutorDrainDeadline {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Control,
            crate::runtime::fragment::ExecutionReason::ExecutorDrainDeadline.reason(),
        )
    }
}

#[cfg(test)]
type StartActivationGate = Arc<(Mutex<bool>, Condvar)>;

/// Global executor that schedules and runs pipeline driver tasks across worker threads.
pub struct GlobalDriverExecutor {
    shared: Arc<ExecutorShared>,
    admission_gate: Mutex<()>,
    event_dispatcher: EventDispatcher,
    lifecycle: Arc<ExecutorLifecycleOwner>,
    #[cfg(test)]
    start_activation_gate: Mutex<Option<StartActivationGate>>,
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
        // The existing bounded cleanup owner exists before any prepared Task
        // can be accepted. Rejection never performs the first thread spawn.
        let _ = rejected_driver_reaper();
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
            #[cfg(test)]
            start_activation_gate: Mutex::new(None),
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
    pub fn submit(&self, tasks: Vec<DriverTask>) -> bool {
        let batch = DriverBatch::new(tasks);
        self.submit_owned(&batch)
    }

    /// Borrow the original prepared slots. Only the final queue transfer
    /// consumes tasks; every earlier hook runs while its slot still owns it.
    pub(crate) fn submit_owned(&self, batch: &DriverBatch) -> bool {
        batch.defer_activation();
        if !batch.begin_submission() {
            return false;
        }
        if batch.len() == 0 {
            batch.mark_metadata_complete();
            return true;
        }
        let control =
            std::mem::ManuallyDrop::new(batch.control().expect("a nonempty original driver batch"));
        let admitted = batch.metadata(3, &control, || {
            if self.shared.admission_closed.load(Ordering::Acquire) {
                return false;
            }
            // Tracing subscribers and their diagnostics never run under the
            // admission fence. Only the final verdict and pure Core attachment
            // are serialized with shutdown below.
            for index in 0..batch.len() {
                batch.attach(index, |task| task.driver.print_pipeline_structure());
            }
            let admission = self
                .admission_gate
                .lock()
                .expect("global executor admission gate lock");
            if self.shared.admission_closed.load(Ordering::Acquire) {
                return false;
            }
            for index in 0..batch.len() {
                batch.borrow_prepared(index, |task| {
                    task.fragment_ctx()
                        .event_scheduler()
                        .attach_executor(Arc::clone(&self.shared), &self.event_dispatcher);
                });
            }
            let mut queue = self
                .shared
                .queue
                .lock()
                .expect("global executor queue lock");
            batch.move_to_queue(&mut queue, &self.shared);
            self.shared.cv.notify_all();
            drop(queue);
            drop(admission);
            true
        });
        let outcome = match admitted {
            Some(admitted) => {
                #[cfg(test)]
                let activation_gate = self.start_activation_gate.lock().unwrap().take();
                #[cfg(test)]
                if let Some(gate) = activation_gate {
                    let (lock, cv) = &*gate;
                    let mut open = lock.lock().unwrap();
                    while !*open {
                        open = cv.wait(open).unwrap();
                    }
                }
                // Workers can have exited after queue unlock. Their original
                // receipts were preserved until this start-work handoff.
                batch.activate();
                if !admitted {
                    batch.drain(true);
                }
                admitted
            }
            None => {
                // A metadata/construction unwind is not proof of shutdown and
                // cannot author ExecutorShutdown or allow an Ok stopped fact.
                batch.drain(false);
                false
            }
        };
        batch.retire_submit_control(std::mem::ManuallyDrop::into_inner(control));
        outcome
    }

    /// Close task admission and start abort/drain for every admitted driver.
    ///
    /// The call waits up to the configured bound for every runtime-owned
    /// scheduler thread to join. Cleanup remains owned by the reaper after a
    /// timeout, and repeated calls observe the same lifecycle transition.
    pub fn shutdown(&self) -> ExecutionResult<()> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    pub(crate) fn shutdown_with_timeout(&self, timeout: Duration) -> ExecutionResult<()> {
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
                return Err(ExecutionFailure::from_domain(ExecutorDrainDeadline {
                    timeout,
                    live_tasks: self.shared.live_tasks.load(Ordering::Acquire),
                }));
            }
            let (next, wait) = self
                .lifecycle
                .cv
                .wait_timeout(lifecycle, deadline.saturating_duration_since(now))
                .unwrap_or_else(|error| error.into_inner());
            lifecycle = next;
            if wait.timed_out() && lifecycle.phase != ExecutorShutdownPhase::Joined {
                return Err(ExecutionFailure::from_domain(ExecutorDrainDeadline {
                    timeout,
                    live_tasks: self.shared.live_tasks.load(Ordering::Acquire),
                }));
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

struct RejectedDriverReaperState {
    pending: VecDeque<ReaperEntry>,
}

struct RejectedDriverReaper {
    state: Mutex<RejectedDriverReaperState>,
    wake: Condvar,
    #[cfg(test)]
    threads_started: AtomicUsize,
}

impl RejectedDriverReaper {
    fn enqueue(&self, task: DriverTask) {
        self.enqueue_entry(ReaperEntry::from_pending(task));
    }
    fn enqueue_entry(&self, entry: ReaperEntry) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Original admission bounds accepted work. Blocking on a second,
        // synthetic cleanup quota can deadlock scopes whose exit depends on
        // another accepted batch. Exactly one entry is installed per batch.
        state.pending.push_back(entry);
        self.wake.notify_one();
    }
}

fn rejected_driver_reaper() -> &'static Arc<RejectedDriverReaper> {
    static REAPER: OnceLock<Arc<RejectedDriverReaper>> = OnceLock::new();
    REAPER.get_or_init(|| {
        let reaper = Arc::new(RejectedDriverReaper {
            state: Mutex::new(RejectedDriverReaperState {
                pending: VecDeque::new(),
            }),
            wake: Condvar::new(),
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
        while let Some(entry) = pending.pop_front() {
            match entry.poll() {
                ReaperPoll::Pending => still_pending.push_back(entry),
                ReaperPoll::Exited | ReaperPoll::Unresolved => {
                    // Only this batch's poll-entry has exited. Unknown slots
                    // retain their original holders and completion obligations.
                }
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
    let custody = task.driver.exit_custody();
    if !custody.try_begin_parking() {
        rejected_driver_reaper().enqueue(task);
        return;
    }
    let control = std::mem::ManuallyDrop::new(task.completion.control.clone());
    let health = control.exit_health_reporter();
    let mut slot = std::mem::ManuallyDrop::new(Some(task));
    let mut phase = ParkingPhase::new();
    phase.roles[0].run(
        &control,
        crate::runtime::fragment::ExecutionReason::DriverParking
            .code()
            .get(),
        || {
            if let Err(refusal) = scheduler.add_pending_finish(&mut slot) {
                // Known capacity remains a typed cleanup/resource fact. An actual
                // diagnostic callback panic is separately supervised by this role.
                control.report_cleanup(&ExecutionFailure::from_domain(refusal));
            }
        },
    );
    if let Some(task) = slot.take() {
        rejected_driver_reaper().enqueue(task);
    }
    // A concurrent ready worker may already have retired the original task.
    // Last control/capture Drop has its detached same-health reporter.
    phase.roles[1].run_metadata(
        &health,
        crate::runtime::fragment::ExecutionReason::DriverParkingControlDrop
            .code()
            .get(),
        || drop(std::mem::ManuallyDrop::into_inner(control)),
    );
    // Captures installed above contain weak scheduler/observable state plus
    // immutable driver identity. The saved roles contain no Task/control field.
    custody.finish_parking(phase.is_unknown().then_some(phase));
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

        // Stop is a hint, not a verdict about a just-handed-off proposal.
        // A delayed stop takes the ordinary failure retirement below, handing
        // off the same occurrence again before its own physical exit.
        if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort() {
            if let Some(task) =
                task.finish_abort_for_control(shared.admission_closed.load(Ordering::Acquire))
            {
                park_pending_finish(task);
            }
            continue;
        }

        let state = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.driver.process(task.time_slice)
        }))
        .unwrap_or_else(|payload| {
            if crate::exec::operators::scan::is_scan_exit_unwind(payload.as_ref()) {
                // No output is held in a local Scan frame at this marker.
                // Its once cleanup/health state is already saved. The next
                // turn advances the original EOS and actual-exit observers.
                return DriverState::Ready;
            }
            let context = Arc::clone(&task.fragment_ctx);
            task.driver.capture_process_panic(payload, |cause| {
                context.set_final_status(cause);
            })
        });

        if shared.admission_closed.load(Ordering::Acquire) || task.completion.should_abort() {
            if let Some(task) =
                task.finish_abort_for_control(shared.admission_closed.load(Ordering::Acquire))
            {
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
                    if let Some(task) = task
                        .finish_abort_for_control(shared.admission_closed.load(Ordering::Acquire))
                    {
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
                                let err = super::driver_failure::DriverFailure::MissingObservable {
                                    driver_id: task.driver_id(),
                                };
                                let task = *task;
                                if let Some(task) = task.finish_abort_with_failure(Some(err.into()))
                                {
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
                                let err = super::driver_failure::DriverFailure::DependencyScheduleRefused { driver_id: task.driver_id() };
                                let task = *task;
                                if let Some(task) = task.finish_abort_with_failure(Some(err.into()))
                                {
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
                task.driver_finished();
            }
            DriverState::Canceled => {
                task.finish_with_failure(
                    super::driver_failure::DriverFailure::UnrequestedCancellation.into(),
                );
            }
            DriverState::Failed(err) => {
                task.finish_with_failure(err);
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
    fn assert_executor_shutdown(result: crate::runtime::fragment::ExecutionResult<()>) {
        use novarocks_failure_contract::FailureProjection;
        use std::error::Error;
        let failure = result.expect_err("driver must retain the accepted shutdown cause");
        let expected = super::super::driver_failure::DriverFailure::ExecutorShutdown;
        assert_eq!(failure.failure_fact(), expected.failure_fact());
        assert_eq!(failure.to_string(), "driver executor is shutting down");
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<super::super::driver_failure::DriverFailure>()
            .unwrap();
        assert_eq!(source, &expected);
    }

    use super::*;
    use std::sync::mpsc;
    use std::thread;

    use crate::exec::pipeline::operator::FinishWatch;
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
        awaiting_cancel: Arc<Observable>,
    }

    impl OperatorLifecycle for PendingAbortOperator {
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

    impl crate::exec::pipeline::shape::SourceKernel for PendingAbortOperator {
        fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
            if self.only_after_cancel {
                crate::exec::pipeline::shape::SourceStart::Ready
            } else {
                crate::exec::pipeline::shape::SourceStart::Finished
            }
        }
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            Ok(crate::exec::pipeline::step::StepResult::new(
                None,
                crate::exec::pipeline::step::NextStep::Wait(
                    crate::exec::pipeline::step::WaitFact {
                        reason: crate::exec::pipeline::step::WaitReason::Input,
                        observable: self.awaiting_cancel.clone(),
                        observed_generation: self.awaiting_cancel.generation(),
                        deadline: None,
                    },
                ),
                0,
            ))
        }
    }
    struct BlockingSource {
        entered: Arc<AtomicBool>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl OperatorLifecycle for BlockingSource {
        fn name(&self) -> &str {
            "blocking_source"
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for BlockingSource {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            self.entered.store(true, Ordering::Release);
            let (released, cv) = &*self.gate;
            let mut released = released.lock().expect("blocking source gate lock");
            while !*released {
                released = cv.wait(released).unwrap_or_else(|error| error.into_inner());
            }
            Ok(crate::exec::pipeline::step::StepResult::new(
                None,
                crate::exec::pipeline::step::NextStep::Finished,
                0,
            ))
        }
    }

    struct PanickingSource;

    impl OperatorLifecycle for PanickingSource {
        fn name(&self) -> &str {
            "panicking_source"
        }

        fn activate(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            panic!("injected driver panic")
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for PanickingSource {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            panic!("injected driver panic")
        }
    }

    #[test]
    fn stopped_observer_sees_driver_owned_backing_already_released() {
        use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
        struct OwnedBufferOperator {
            _buffer: Vec<u8>,
            _responsibility: TrackedBytes,
        }
        impl OperatorLifecycle for OwnedBufferOperator {
            fn name(&self) -> &str {
                "owned_buffer"
            }
        }
        impl crate::exec::pipeline::shape::SourceKernel for OwnedBufferOperator {
            fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
                crate::exec::pipeline::shape::SourceStart::Finished
            }
            fn poll_output(
                &mut self,
                _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
                unreachable!("backing-only owner has no data work")
            }
        }
        let tracker = MemTracker::new_root("driver actual stop backing");
        let responsibility = TrackedBytes::try_new(512, tracker.clone()).unwrap();
        let operator = OwnedBufferOperator {
            _buffer: Vec::with_capacity(512),
            _responsibility: responsibility,
        };
        let runtime_state = Arc::new(RuntimeState::default());
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            None,
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        let observed_bytes = Arc::new(std::sync::atomic::AtomicI64::new(-1));
        let observed = Arc::clone(&observed_bytes);
        let original_tracker = tracker.clone();
        completion.bind_test_resources_finisher(move |_| {
            observed.store(original_tracker.current(), Ordering::Release);
        });
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    operator,
                ))],
                None,
                Vec::new(),
                runtime_state,
                None,
            ),
            Arc::clone(&completion),
            fragment_ctx,
            Duration::from_millis(1),
        );
        assert_eq!(tracker.current(), 512);
        task.driver_finished();
        assert!(completion.stopped_fact().is_some());
        assert_eq!(
            observed_bytes.load(Ordering::Acquire),
            0,
            "actual-stop callbacks must observe destruction of the driver's real backing"
        );
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn fragment_completion_wait_timeout_drains_before_returning_timeout_error() {
        let completion = FragmentCompletion::new(
            1,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let waiter = Arc::clone(&completion);
        let (result_tx, result_rx) = mpsc::sync_channel(1);

        let timeout_failure = ExecutionFailure::from(
            crate::exec::pipeline::driver_failure::DriverFailure::DeadlineExceeded(
                novarocks_failure_contract::BoundedDetail::truncating("query timed out after 5 ms"),
            ),
        );
        let expected_timeout = timeout_failure.clone();
        let join = thread::spawn(move || {
            result_tx
                .send(waiter.wait_timeout(Duration::from_millis(5), timeout_failure))
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

        assert!(err.same_occurrence(&expected_timeout));
        assert!(completion.should_abort());
    }

    #[test]
    fn failed_conclusion_does_not_claim_that_drivers_stopped() {
        let completion = FragmentCompletion::new(
            2,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );

        let failure = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
        let submitted_failure: crate::runtime::fragment::ExecutionFailure = failure.clone();
        completion.fail(submitted_failure.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&submitted_failure)
        );
        assert_eq!(completion.conclusion(), Some(Err(failure.clone())));
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
            Err(failure.clone())
        );
    }

    #[test]
    fn prepared_zero_driver_group_freezes_only_after_initial_failure_and_role_installation() {
        let control = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
        let completion = FragmentCompletion::new_prepared(0, control.clone());
        assert!(completion.stopped_fact().is_none());
        let seen = Arc::new(Mutex::new(None));
        let observed = seen.clone();
        completion.bind_test_resources_finisher(move |fact| {
            *observed.lock().unwrap() = Some(fact.conclusion());
        });
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::DeadlineExceeded.into();
        let submitted_failure: crate::runtime::fragment::ExecutionFailure = original.clone();
        completion.fail(submitted_failure.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&submitted_failure)
        );
        assert!(
            seen.lock().unwrap().is_none(),
            "binding a dormant no-job scope is not activation"
        );
        completion.activate();
        let observed = seen.lock().unwrap().take().unwrap().unwrap_err();
        assert!(observed.same_occurrence(&original));
        assert_eq!(
            completion.stopped_fact().unwrap().conclusion(),
            Err(original)
        );
        completion.activate();
        assert!(
            seen.lock().unwrap().is_none(),
            "activation and named notification are once"
        );
    }

    #[test]
    fn stopped_subscription_before_and_after_transition_fires_once() {
        let completion = FragmentCompletion::new(
            1,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let before = Arc::new(AtomicUsize::new(0));
        let before_callback = Arc::clone(&before);
        completion.bind_test_resources_finisher(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            before_callback.fetch_add(1, Ordering::SeqCst);
        });

        assert!(completion.driver_finished());
        assert_eq!(before.load(Ordering::SeqCst), 1);

        let after = Arc::new(AtomicUsize::new(0));
        let after_callback = Arc::clone(&after);
        completion.bind_test_wall_time(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            after_callback.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert!(!completion.driver_finished());
        assert_eq!(before.load(Ordering::SeqCst), 1);
        assert_eq!(after.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stopped_observer_panic_before_transition_is_isolated() {
        let completion = FragmentCompletion::new(
            1,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let peer = FragmentCompletion::new(
            0,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        peer.bind_test_resources_finisher(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            first_callback.fetch_add(1, Ordering::SeqCst);
        });
        completion.bind_test_wall_time(|_| {
            panic!("injected stopped observer panic");
        });
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        completion.bind_test_resources_finisher(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            last_callback.fetch_add(1, Ordering::SeqCst);
        });

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
        let completion = FragmentCompletion::new(
            0,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let frozen = completion
            .stopped_fact()
            .expect("zero-driver completion starts stopped");
        let peer = FragmentCompletion::new(
            0,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        let expected = frozen.clone();
        peer.bind_test_resources_finisher(move |fact| {
            assert_eq!(fact, &expected);
            first_callback.fetch_add(1, Ordering::SeqCst);
        });
        completion.bind_test_wall_time(|_| {
            panic!("injected immediate stopped observer panic");
        });
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        completion.bind_test_resources_finisher(move |fact| {
            assert_eq!(fact.conclusion(), Ok(()));
            last_callback.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
        assert_eq!(completion.stopped_fact(), Some(frozen));
    }

    #[test]
    fn stopped_subscription_racing_the_last_driver_cannot_lose_wakeup() {
        for _ in 0..1_000 {
            let completion = FragmentCompletion::new(
                1,
                Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
            );
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
            completion.bind_test_resources_finisher(move |_| {
                observed_callback.fetch_add(1, Ordering::SeqCst);
            });
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
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        executor.submit(vec![DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    PendingAbortOperator {
                        pending: Arc::clone(&pending),
                        observed: Arc::clone(&observed),
                        cancel_requested: Arc::clone(&cancel_requested),
                        only_after_cancel: false,
                        awaiting_cancel: Arc::new(Observable::new()),
                    },
                ))],
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
        assert!(error.to_string().contains("did not stop within"));
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Control);
        assert_eq!(error.failure_fact().reason().code(), 513);
        assert!(cancel_requested.load(Ordering::Acquire));
        assert_eq!(completion.stopped_fact(), None);

        pending.store(false, Ordering::Release);
        executor
            .shutdown_with_timeout(Duration::from_secs(1))
            .expect("pending finish release completes shutdown");
        assert_executor_shutdown(
            completion
                .stopped_fact()
                .expect("actual stop follows pending finish")
                .conclusion(),
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

    impl OperatorLifecycle for WatchedPendingOperator {
        fn name(&self) -> &str {
            "watched_pending"
        }

        fn cancel(&mut self) {
            self.cancelled.store(true, Ordering::Release);
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

    impl crate::exec::pipeline::shape::SourceKernel for WatchedPendingOperator {
        fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
            crate::exec::pipeline::shape::SourceStart::Finished
        }
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            unreachable!("finished data has no work")
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
            let completion = FragmentCompletion::new(1, runtime_state.error_state());
            assert!(executor.submit(vec![DriverTask::new(
                PipelineDriver::new(
                    1,
                    vec![Box::new(crate::exec::pipeline::shape::Source::new(
                        WatchedPendingOperator {
                            pending: Arc::clone(&pending),
                            observable: observable.clone(),
                            checks: Arc::clone(&checks),
                            cancelled: Arc::clone(&cancelled),
                        }
                    ))],
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
        let failure = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
        let submitted_failure: crate::runtime::fragment::ExecutionFailure = failure.clone();
        harness.completion.fail(submitted_failure.clone());
        assert!(
            harness
                .completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&submitted_failure)
        );
        harness.fragment_ctx.set_final_status(failure.clone());
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
        assert_eq!(harness.completion.conclusion(), Some(Err(failure.clone())));
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
        assert!(error.to_string().contains("did not stop within"));
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Control);
        assert_eq!(error.failure_fact().reason().code(), 513);
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
        assert_executor_shutdown(
            harness
                .completion
                .conclusion()
                .expect("accepted executor shutdown conclusion"),
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
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        executor.submit(vec![DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    BlockingSource {
                        entered: Arc::clone(&entered),
                        gate: Arc::clone(&gate),
                    },
                ))],
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
    fn stopped_preparation_on_closed_executor_keeps_peer_receipt_until_actual_exit() {
        use crate::runtime::runtime_state::RuntimeErrorState;
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        };
        let executor = GlobalDriverExecutor::new(1);
        executor.shutdown().unwrap();
        let owner = crate::runtime::task_control::test_support::Owner::new();
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let receipt = owner.accepted().unwrap();
        let control = Arc::new(RuntimeErrorState::for_task(Arc::new(
            owner.view(TaskFailurePhase::Execution),
        )));
        let state = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            control.clone(),
        ));
        let fragment = Arc::new(FragmentContext::new(
            None,
            state.clone(),
            Some((8801, 8802)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, control.clone());
        let pending = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    PendingAbortOperator {
                        pending: pending.clone(),
                        observed: observed.clone(),
                        cancel_requested: cancelled.clone(),
                        only_after_cancel: true,
                        awaiting_cancel: Arc::new(Observable::new()),
                    },
                ))],
                None,
                Vec::new(),
                state,
                Some((8801, 8802)),
            ),
            completion.clone(),
            fragment,
            Duration::from_millis(1),
        );
        assert!(!executor.submit(vec![task]));
        assert!(cancelled.load(Ordering::Acquire));
        assert!(observed.load(Ordering::Acquire));
        assert!(completion.stopped_fact().is_none());
        assert!(
            Arc::ptr_eq(&owner.accepted().unwrap(), &receipt),
            "shutdown cannot refine an existing peer with a fabricated new origin"
        );
        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(1), || completion
            .stopped_fact()
            .is_some()));
        assert!(Arc::ptr_eq(&owner.accepted().unwrap(), &receipt));
        assert!(owner.accepted().unwrap().originating_failure().is_none());
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
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
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
        assert_executor_shutdown(stopped.conclusion());
    }

    // Exercise the production panic catch and retirement paths with delayed adjudication.
    #[test]
    fn deferred_verdict_driver_panic_retires_through_the_failure_path() {
        use crate::runtime::runtime_state::RuntimeErrorState;
        use crate::runtime::task_control::test_support::DeferredOwner;
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        struct ReleasePendingOnDrop(Arc<AtomicBool>);
        impl Drop for ReleasePendingOnDrop {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let executor = GlobalDriverExecutor::new(1);
        let pending = Arc::new(AtomicBool::new(true));
        let _release = ReleasePendingOnDrop(pending.clone());
        let observed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let owner = DeferredOwner::new();
        let runtime_state = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(RuntimeErrorState::for_task(Arc::new(
                owner.view(TaskFailurePhase::Execution),
            ))),
        ));
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            runtime_state.clone(),
            Some((33_041, 33_042)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![
                    Box::new(crate::exec::pipeline::shape::Source::new(PanickingSource)),
                    Box::new(crate::exec::pipeline::shape::Source::new(
                        PendingAbortOperator {
                            pending: pending.clone(),
                            observed: observed.clone(),
                            cancel_requested: cancel_requested.clone(),
                            only_after_cancel: true,
                            awaiting_cancel: Arc::new(Observable::new()),
                        },
                    )),
                ],
                None,
                Vec::new(),
                runtime_state.clone(),
                Some((33_041, 33_042)),
            ),
            completion.clone(),
            fragment_ctx,
            Duration::from_secs(1),
        );
        assert!(executor.submit(vec![task]));
        assert!(wait_until(Duration::from_secs(2), || cancel_requested
            .load(Ordering::Acquire)
            && observed.load(Ordering::Acquire)));
        // The catch handler really ran and signaled operators, but its proposal is
        // still pending. A synchronous status expectation would hide this branch.
        assert!(owner.accepted().is_none());
        assert!(!runtime_state.error_state().is_stopped());
        assert!(completion.conclusion().is_none());
        assert!(owner.pending_len() > 0);
        assert!(completion.stopped_fact().is_none());
        assert_eq!(completion.remaining_drivers(), 1);
        assert_eq!(executor.shared.live_tasks.load(Ordering::Acquire), 1);
        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(2), || completion
            .stopped_fact()
            .is_some()));
        assert!(wait_until(Duration::from_secs(2), || executor
            .shared
            .live_tasks
            .load(Ordering::Acquire)
            == 0));
        assert_eq!(completion.remaining_drivers(), 0);
        assert!(
            owner.accepted().is_none(),
            "physical exit precedes adjudication"
        );
        assert!(
            completion.stopped_fact().unwrap().conclusion().is_ok(),
            "the completion payload is only a lagging hint, never the fragment's final outcome"
        );
        assert!(
            owner.pending_len() >= 2,
            "panic capture and failed retirement hand off the original occurrence"
        );
        let settled = Arc::new(Mutex::new(None));
        let result = settled.clone();
        owner
            .admit_barrier()
            .register(Box::new(move |snapshot| *result.lock().unwrap() = snapshot));
        assert!(settled.lock().unwrap().is_none());
        assert!(owner.advance_one());
        let accepted = owner
            .accepted()
            .expect("the actual caught panic is accepted first");
        let original = accepted.originating_failure().unwrap().clone();
        assert!(
            original
                .detail()
                .contains("panic in driver execution: injected driver panic")
        );
        assert_eq!(accepted.neutral().generation().get(), 1);
        owner.advance_all();
        let current = owner.accepted().unwrap();
        assert!(
            Arc::ptr_eq(&accepted, &current),
            "duplicate handoffs lose without changing the accepted source"
        );
        assert!(
            settled
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert_eq!(owner.pending_len(), 0);
        assert!(
            crate::runtime::task_control::TaskExecutionPort::execution_health(owner.as_ref())
                .failure()
                .is_none()
        );
        executor
            .shutdown()
            .expect("the actual admitted driver and executor converge");
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
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![
                    Box::new(crate::exec::pipeline::shape::Source::new(PanickingSource)),
                    Box::new(crate::exec::pipeline::shape::Source::new(
                        PendingAbortOperator {
                            pending: Arc::clone(&pending),
                            observed: Arc::clone(&observed),
                            cancel_requested: Arc::clone(&cancel_requested),
                            only_after_cancel: true,
                            awaiting_cancel: Arc::new(Observable::new()),
                        },
                    )),
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
                .detail()
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
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        let task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    PendingAbortOperator {
                        pending: Arc::clone(&pending),
                        observed: Arc::clone(&observed),
                        cancel_requested: Arc::clone(&cancel_requested),
                        only_after_cancel: false,
                        awaiting_cancel: Arc::new(Observable::new()),
                    },
                ))],
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
        assert_executor_shutdown(
            completion
                .conclusion()
                .expect("accepted executor shutdown conclusion"),
        );
        assert_eq!(completion.stopped_fact(), None);
        assert_eq!(completion.remaining_drivers(), 1);
        pending.store(false, Ordering::Release);
        assert!(wait_until(Duration::from_secs(1), || completion
            .stopped_fact()
            .is_some()));
        assert_eq!(completion.remaining_drivers(), 0);
        assert_executor_shutdown(
            completion
                .stopped_fact()
                .expect("rejected driver stopped")
                .conclusion(),
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
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        let mut task = DriverTask::new(
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    PendingAbortOperator {
                        pending: Arc::clone(&pending),
                        observed: Arc::clone(&observed),
                        cancel_requested: Arc::clone(&cancel_requested),
                        only_after_cancel: false,
                        awaiting_cancel: Arc::new(Observable::new()),
                    },
                ))],
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
        assert_executor_shutdown(
            completion
                .conclusion()
                .expect("accepted executor shutdown conclusion"),
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
        assert_executor_shutdown(
            completion
                .stopped_fact()
                .expect("rejected driver stopped")
                .conclusion(),
        );
    }

    #[test]
    fn concurrent_rejected_drivers_share_one_cleanup_thread() {
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
                    let completion = FragmentCompletion::new(1, runtime_state.error_state());
                    let observed = Arc::new(AtomicBool::new(false));
                    let cancel_requested = Arc::new(AtomicBool::new(false));
                    let task = DriverTask::new(
                        PipelineDriver::new(
                            1,
                            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                                PendingAbortOperator {
                                    pending: Arc::clone(&pending),
                                    observed: Arc::clone(&observed),
                                    cancel_requested: Arc::clone(&cancel_requested),
                                    only_after_cancel: false,
                                    awaiting_cancel: Arc::new(Observable::new()),
                                },
                            ))],
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

    #[test]
    fn typed_failure_completion_first_cause_precedes_actual_stop_and_survives_second_failure() {
        let completion = FragmentCompletion::new(
            2,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let first: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
        let submitted_failure: crate::runtime::fragment::ExecutionFailure = first.clone();
        completion.fail(submitted_failure.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&submitted_failure)
        );
        let previous = completion.conclusion().unwrap().unwrap_err();
        completion.fail(novarocks_functions::KernelFailure::Cancelled);
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&previous)
        );
        assert_eq!(completion.conclusion(), Some(Err(first.clone())));
        assert_eq!(completion.remaining_drivers(), 2);
        assert!(completion.stopped_fact().is_none());
        assert!(!completion.driver_finished());
        assert!(completion.stopped_fact().is_none());
        assert!(completion.driver_finished());
        assert_eq!(completion.stopped_fact().unwrap().conclusion(), Err(first));
    }
}

#[cfg(test)]
#[path = "aggregate_count_host_panic_tests.rs"]
mod original_count_host_panic_tests;
