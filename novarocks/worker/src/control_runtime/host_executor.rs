// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed physical host workers driving the original fair preparation queue.
//! A physical body return, queue drain or thread join supplies no Task, Source,
//! capture or report exit. Those facts belong to each original job's custody.

mod pair;
pub(crate) use pair::{HostExecutorPair, HostPairShutdownError};

use super::preparation::{
    HostFairnessGroup, PreparationQueue, QueuedPreparation, RunningPreparation,
};
use novarocks_control_runtime::notify::Notify;
use novarocks_control_runtime_core::{
    fair_queue::FairQueueError,
    identity::{ScopeId, WorkId},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    task::Waker,
    thread::{self, JoinHandle},
};

type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct HostExecutorLimits {
    pub contexts: NonZeroUsize,
    pub jobs: NonZeroUsize,
    pub per_context: NonZeroUsize,
    pub workers: NonZeroUsize,
}
impl HostExecutorLimits {
    /// Cover admitted loaders plus at most N physical running tails whose
    /// business holders may already have exited. Per-Context Task identities
    /// are cumulative, so their original T bound still covers historical tails.
    /// This pure check can run before capturing the original runner.
    pub(crate) fn for_loading(
        config: &crate::task_registry_config::TaskExecutionRegistryConfig,
        max_contexts: NonZeroUsize,
    ) -> Result<Self, String> {
        config.validate_capacity_limits()?;
        let jobs = config
            .max_active_tasks_per_backend
            .checked_add(config.max_prepare_workers)
            .ok_or_else(|| "loading executor active-plus-worker capacity overflow".to_owned())?;
        let contexts = max_contexts
            .get()
            .checked_add(config.max_prepare_workers)
            .ok_or_else(|| "loading executor context-plus-worker capacity overflow".to_owned())?;
        Ok(Self {
            contexts: NonZeroUsize::new(contexts).expect("positive Context and worker capacities"),
            jobs: NonZeroUsize::new(jobs).expect("positive active Task and worker capacities"),
            per_context: NonZeroUsize::new(config.max_tasks_per_context)
                .expect("validated per-Context Task capacity"),
            workers: NonZeroUsize::new(config.max_prepare_workers)
                .expect("validated physical worker capacity"),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostSubmitError {
    Closed,
    DriverFailed,
    Queue(FairQueueError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostShutdownError {
    /// No physical worker can drain the retained original queue.
    UndrainedAfterFailure,
    /// The queue is physically empty, but an original panic remains unknown.
    DriverFailed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerState {
    NotSpawned,
    Alive,
    Exited,
    Failed,
}
struct WorkerSlot {
    state: WorkerState,
    panic: Option<PanicPayload>,
}
struct Gate {
    accepting: bool,
    // Exactly N physical-worker metadata records, never per-job accounting.
    workers: Box<[WorkerSlot]>,
}
struct HostJob<J> {
    key: (ScopeId, WorkId),
    group: HostFairnessGroup,
    original: J,
}
impl<J: QueuedPreparation> QueuedPreparation for HostJob<J> {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        self.key
    }
    fn fairness_group(&self, _scope: ScopeId) -> HostFairnessGroup {
        self.group
    }
}
struct Shared<J: QueuedPreparation> {
    queue: PreparationQueue<HostJob<J>>,
    gate: Mutex<Gate>,
    ready: Notify,
    // One fixed driver I/O wake primitive, never a per-job callback.
    waker: Option<Waker>,
}
enum NextBody<J: QueuedPreparation> {
    Run(RunningPreparation<HostJob<J>>),
    Drained,
    Wait(u64),
}
impl<J: QueuedPreparation> Shared<J> {
    fn wake_driver(&self) {
        // Call only after all executor/queue locks have been released. A
        // driver may immediately inspect capacity or retry its original job.
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
    fn close_external(&self) {
        self.gate.lock().expect("host admission gate").accepting = false;
        self.ready.notify();
        self.wake_driver();
    }
    fn failed(&self) -> bool {
        self.gate
            .lock()
            .expect("host admission gate")
            .workers
            .iter()
            .any(|worker| worker.state == WorkerState::Failed)
    }
    fn has_live_workers(&self) -> bool {
        self.gate
            .lock()
            .expect("host admission gate")
            .workers
            .iter()
            .any(|worker| worker.state == WorkerState::Alive)
    }
    fn wait_after(&self, observed: u64) {
        // Consume the observation made before the predicate. Resampling here
        // would lose a ready event delivered between the predicate and sleep.
        self.ready.wait_after(observed);
    }
    fn next_body(&self) -> NextBody<J> {
        // Sample before *both* predicates. An enqueue, close or body exit
        // between this observation and sleep then prevents that sleep.
        let observed = self.ready.version();
        if let Some(body) = self.queue.pop() {
            return NextBody::Run(body);
        }
        let gate = self.gate.lock().expect("host admission gate");
        let drained = !gate.accepting && self.queue.is_empty();
        drop(gate);
        if drained {
            NextBody::Drained
        } else {
            NextBody::Wait(observed)
        }
    }
    fn record_failure(&self, index: usize, payload: Box<dyn Any + Send>) {
        let payload = ManuallyDrop::new(payload);
        {
            let mut gate = self.gate.lock().expect("host admission gate");
            let worker = &mut gate.workers[index];
            // One failing worker stops taking jobs. A second panic while its
            // own failure machinery runs is a process-level invariant failure;
            // do not destroy either unknown payload or invent an exit.
            if worker.panic.is_some() {
                std::process::abort();
            }
            worker.panic = Some(payload);
            worker.state = WorkerState::Failed;
            gate.accepting = false;
        }
        self.ready.notify();
        self.wake_driver();
    }
    fn record_exit(&self, index: usize) {
        {
            let mut gate = self.gate.lock().expect("host admission gate");
            let worker = &mut gate.workers[index];
            if worker.state != WorkerState::Failed {
                worker.state = WorkerState::Exited;
            }
        }
        self.ready.notify();
        self.wake_driver();
    }
}

/// Submission and indexed cancellation return the same original J on refusal
/// or removal. They never dispose it or sign its Work/report exit.
pub(crate) struct HostExecutorHandle<J: QueuedPreparation> {
    shared: Arc<Shared<J>>,
}
impl<J: QueuedPreparation> Clone for HostExecutorHandle<J> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl<J: QueuedPreparation> HostExecutorHandle<J> {
    pub(crate) fn try_preparation_snapshot(
        &self,
    ) -> Result<
        Option<super::preparation::PreparationQueueSnapshot>,
        super::preparation::PreparationQueueSnapshotError,
    > {
        self.shared.queue.try_snapshot()
    }
    pub(crate) fn try_submit(&self, job: J) -> Result<(), (HostSubmitError, J)> {
        // The original trait method is foreign code. Only our fixed metadata
        // implementation of HostJob::queue_key runs behind the submit gate.
        let key = job.queue_key();
        let group = job.fairness_group(key.0);
        let gate = self.shared.gate.lock().expect("host admission gate");
        if !gate.accepting {
            let reason = if gate
                .workers
                .iter()
                .any(|worker| worker.state == WorkerState::Failed)
            {
                HostSubmitError::DriverFailed
            } else {
                HostSubmitError::Closed
            };
            drop(gate);
            return Err((reason, job));
        }
        let result = self.shared.queue.enqueue(HostJob {
            key,
            group,
            original: job,
        });
        drop(gate);
        match result {
            Ok(()) => {
                self.shared.ready.notify();
                Ok(())
            }
            Err((reason, job)) => Err((HostSubmitError::Queue(reason), job.original)),
        }
    }
    pub(crate) fn close_external(&self) {
        self.shared.close_external();
    }
    pub(crate) fn cancel_queued(&self, scope: ScopeId, work: WorkId) -> Result<J, FairQueueError> {
        let original = self.shared.queue.cancel(scope, work)?.original;
        self.shared.ready.notify();
        self.shared.wake_driver();
        Ok(original)
    }
}

/// An explicit physical-thread owner. Dropping this owner neither cancels nor
/// joins: its original queue, runner and JoinHandles stay retained. Service
/// must close and drain explicitly, and retain failed owners for its failure
/// policy. The runner must supervise each original job's Work/captures itself.
#[must_use = "close and drain the original host executor explicitly"]
pub(crate) struct HostExecutor<J, F>
where
    J: QueuedPreparation,
    F: Fn(J) + Send + Sync + 'static,
{
    shared: ManuallyDrop<Arc<Shared<J>>>,
    threads: ManuallyDrop<Vec<(usize, JoinHandle<()>)>>,
    runner: ManuallyDrop<Arc<F>>,
}
impl<J, F> HostExecutor<J, F>
where
    J: QueuedPreparation,
    F: Fn(J) + Send + Sync + 'static,
{
    /// Spawn only the configured N workers. A partial startup returns this
    /// same owner so the caller can explicitly close/drain/join those workers
    /// and recover the original runner; no capture is silently disposed.
    pub(crate) fn start(
        limits: HostExecutorLimits,
        runner: F,
    ) -> Result<Self, (std::io::Error, Self)> {
        Self::start_inner(limits, runner, None, false)
    }
    /// The original driver retries retained jobs after real physical capacity
    /// or shutdown state changes. Its standard I/O waker runs without any
    /// executor lock and grants no job, Work, report or capture exit.
    pub(crate) fn start_with_waker(
        limits: HostExecutorLimits,
        runner: F,
        waker: Waker,
    ) -> Result<Self, (std::io::Error, Self)> {
        Self::start_inner(limits, runner, Some(waker), false)
    }
    /// Task preparation preserves the original enqueue order among currently
    /// eligible Context heads. Other host pools retain group round-robin.
    pub(crate) fn start_eligible_fifo_with_waker(
        limits: HostExecutorLimits,
        runner: F,
        waker: Waker,
    ) -> Result<Self, (std::io::Error, Self)> {
        Self::start_inner(limits, runner, Some(waker), true)
    }
    fn start_inner(
        limits: HostExecutorLimits,
        runner: F,
        waker: Option<Waker>,
        eligible_fifo: bool,
    ) -> Result<Self, (std::io::Error, Self)> {
        let count = limits.workers.get();
        let shared = Arc::new(Shared {
            queue: if eligible_fifo {
                PreparationQueue::eligible_fifo(
                    limits.contexts.get(),
                    limits.jobs.get(),
                    limits.per_context.get(),
                    count,
                )
            } else {
                PreparationQueue::new(
                    limits.contexts.get(),
                    limits.jobs.get(),
                    limits.per_context.get(),
                    count,
                )
            },
            gate: Mutex::new(Gate {
                accepting: true,
                workers: (0..count)
                    .map(|_| WorkerSlot {
                        state: WorkerState::NotSpawned,
                        panic: None,
                    })
                    .collect(),
            }),
            ready: Notify::default(),
            waker,
        });
        let mut executor = Self {
            shared: ManuallyDrop::new(shared),
            threads: ManuallyDrop::new(Vec::with_capacity(count)),
            runner: ManuallyDrop::new(Arc::new(runner)),
        };
        for index in 0..count {
            executor
                .shared
                .gate
                .lock()
                .expect("host admission gate")
                .workers[index]
                .state = WorkerState::Alive;
            let shared = Arc::clone(&executor.shared);
            let runner = Arc::clone(&executor.runner);
            match thread::Builder::new()
                .name(format!("worker-host-{index}"))
                .spawn(move || worker_entry(shared, runner, index))
            {
                Ok(thread) => executor.threads.push((index, thread)),
                Err(error) => {
                    executor
                        .shared
                        .gate
                        .lock()
                        .expect("host admission gate")
                        .workers[index]
                        .state = WorkerState::NotSpawned;
                    executor.shared.close_external();
                    return Err((error, executor));
                }
            }
        }
        Ok(executor)
    }
    pub(crate) fn handle(&self) -> HostExecutorHandle<J> {
        HostExecutorHandle {
            shared: Arc::clone(&self.shared),
        }
    }
    /// Close external submission, drain original bodies and only then join.
    /// Success returns the same runner for caller-supervised capture disposal;
    /// it proves only physical executor drain, never Source/Task convergence.
    /// Failure returns this same retained owner, including every panic slot.
    pub(crate) fn shutdown(mut self) -> Result<Arc<F>, (HostShutdownError, Self)> {
        self.shared.close_external();
        loop {
            let observed = self.shared.ready.version();
            if self.shared.queue.is_empty() {
                break;
            }
            if !self.shared.has_live_workers() {
                return Err((HostShutdownError::UndrainedAfterFailure, self));
            }
            self.shared.wait_after(observed);
        }
        // No queued/running body remains. Joining does not manufacture any
        // original job's whole-exit, report consumption or capacity release.
        for (_index, thread) in std::mem::take(&mut *self.threads) {
            if let Err(payload) = thread.join() {
                // Callback unwind is contained in worker_entry. An unexpected
                // thread-wrapper/TLS failure belongs to shared runtime policy.
                std::mem::forget(payload);
                std::process::abort();
            }
        }
        if self.shared.failed() {
            return Err((HostShutdownError::DriverFailed, self));
        }
        let Self {
            shared,
            threads,
            runner,
        } = self;
        drop(ManuallyDrop::into_inner(threads));
        drop(ManuallyDrop::into_inner(shared));
        Ok(ManuallyDrop::into_inner(runner))
    }
}
fn worker_entry<J, F>(shared: Arc<Shared<J>>, runner: Arc<F>, index: usize)
where
    J: QueuedPreparation,
    F: Fn(J) + Send + Sync + 'static,
{
    let driver = catch_unwind(AssertUnwindSafe(|| {
        loop {
            match shared.next_body() {
                NextBody::Run(body) => {
                    // Only the original callback is a recoverable job boundary.
                    // Keep its outcome inert while the original queue returns
                    // the physical slot. Queue/Notify invariant failure belongs
                    // to process fail-stop, not local job recovery.
                    let result = body.run_body(|job| {
                        ManuallyDrop::new(catch_unwind(AssertUnwindSafe(|| runner(job.original))))
                    });
                    match ManuallyDrop::into_inner(result) {
                        Ok(()) => {
                            // run_body has already retired the actual running
                            // membership/key and physical worker slot. An
                            // earlier business-result notice cannot do this.
                            shared.ready.notify();
                            shared.wake_driver();
                        }
                        Err(payload) => {
                            shared.record_failure(index, payload);
                            break;
                        }
                    }
                }
                NextBody::Drained => break,
                NextBody::Wait(observed) => shared.wait_after(observed),
            }
        }
        shared.record_exit(index);
    }));
    if let Err(payload) = driver {
        // Poisoned bookkeeping cannot justify local recovery or opaque Drop.
        // This is the shared-runtime invariant policy, not a Task exit fact.
        std::mem::forget(payload);
        std::process::abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::Duration,
    };

    const WAIT: Duration = Duration::from_secs(5);
    struct Job {
        key: (ScopeId, WorkId),
        body: Box<dyn FnOnce() + Send>,
    }
    impl QueuedPreparation for Job {
        fn queue_key(&self) -> (ScopeId, WorkId) {
            self.key
        }
    }
    fn job(scope: u64, work: u64, body: impl FnOnce() + Send + 'static) -> Job {
        Job {
            key: (ScopeId::new(scope).unwrap(), WorkId::new(work).unwrap()),
            body: Box::new(body),
        }
    }
    fn limits(workers: usize) -> HostExecutorLimits {
        HostExecutorLimits {
            contexts: NonZeroUsize::new(8).unwrap(),
            jobs: NonZeroUsize::new(16).unwrap(),
            per_context: NonZeroUsize::new(8).unwrap(),
            workers: NonZeroUsize::new(workers).unwrap(),
        }
    }
    fn run(job: Job) {
        (job.body)();
    }
    fn start(workers: usize) -> HostExecutor<Job, fn(Job)> {
        match HostExecutor::start(limits(workers), run as fn(Job)) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual host thread start failed: {error}"),
        }
    }
    fn submit(handle: &HostExecutorHandle<Job>, job: Job) {
        if let Err((reason, _original)) = handle.try_submit(job) {
            panic!("actual submit refused: {reason:?}");
        }
    }
    fn shutdown(executor: HostExecutor<Job, fn(Job)>) {
        match executor.shutdown() {
            Ok(runner) => drop(runner),
            Err((reason, _owner)) => panic!("actual executor drain failed: {reason:?}"),
        }
    }

    #[test]
    fn loading_geometry_covers_active_inventory_independently_of_preparation_p() {
        use crate::task_registry_config::TaskExecutionRegistryConfig;
        use novarocks_types::identity::BackendProcessId;

        let mut config =
            TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 17, 9);
        config.max_preparing_tasks = 2;
        config.max_preparing_tasks_per_context = 1;
        config.max_prepare_workers = 4;
        let limits = HostExecutorLimits::for_loading(&config, NonZeroUsize::new(3).unwrap())
            .expect("valid original loading geometry");
        assert_eq!(limits.contexts.get(), 7);
        assert_eq!(limits.jobs.get(), 13);
        assert_eq!(limits.per_context.get(), 17);
        assert_eq!(limits.workers.get(), 4);
    }

    #[test]
    fn loading_geometry_rejects_invalid_config_before_any_runner_transfer() {
        use crate::task_registry_config::TaskExecutionRegistryConfig;
        use novarocks_types::identity::BackendProcessId;

        for field in 0..3 {
            let mut config =
                TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 2, 3);
            let expected = match field {
                0 => {
                    config.max_tasks_per_context = 0;
                    "max_tasks_per_context"
                }
                1 => {
                    config.max_active_tasks_per_backend = 0;
                    "max_active_tasks_per_backend"
                }
                _ => {
                    config.max_prepare_workers = 0;
                    "max_prepare_workers"
                }
            };
            let error = HostExecutorLimits::for_loading(&config, NonZeroUsize::new(2).unwrap())
                .expect_err("invalid loading geometry must fail before executor start");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn loading_geometry_checks_physical_tail_overflow_without_incrementing_cumulative_t() {
        use crate::task_registry_config::TaskExecutionRegistryConfig;
        use novarocks_types::identity::BackendProcessId;

        let mut config =
            TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 1, usize::MAX);
        config.max_prepare_workers = 1;
        let error = HostExecutorLimits::for_loading(&config, NonZeroUsize::new(1).unwrap())
            .expect_err("A plus N must be checked before runner transfer");
        assert!(error.contains("active-plus-worker"), "{error}");
        config.max_active_tasks_per_backend = 1;
        let error =
            HostExecutorLimits::for_loading(&config, NonZeroUsize::new(usize::MAX).unwrap())
                .expect_err("C plus N must be checked before runner transfer");
        assert!(error.contains("context-plus-worker"), "{error}");
        config.max_tasks_per_context = usize::MAX;
        let limits = HostExecutorLimits::for_loading(&config, NonZeroUsize::new(1).unwrap())
            .expect("cumulative Task identities require no T plus one arithmetic");
        assert_eq!(limits.per_context.get(), usize::MAX);
    }

    #[test]
    fn loading_geometry_preserves_all_live_positions_behind_a_retired_business_tail() {
        use crate::task_registry_config::TaskExecutionRegistryConfig;
        use novarocks_types::identity::BackendProcessId;

        let mut config = TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 3, 3);
        config.max_preparing_tasks = 1;
        config.max_preparing_tasks_per_context = 1;
        config.max_prepare_workers = 1;
        let limits = HostExecutorLimits::for_loading(&config, NonZeroUsize::new(1).unwrap())
            .expect("valid actual inventory bound");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let executor = match HostExecutor::start(limits, move |job: Job| {
            let old = job.queue_key() == (ScopeId::new(1).unwrap(), WorkId::new(1).unwrap());
            run(job);
            if old {
                // The original job has been consumed, but the physical queue
                // still retains its running key until this outer runner returns.
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        }) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual host thread start failed: {error}"),
        };
        let handle = executor.handle();
        submit(&handle, job(1, 1, || {}));
        started_rx.recv_timeout(WAIT).unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let mut refused = Vec::with_capacity(4);
        let mut admitted = 0;
        // A=3 live candidates belong to one new Context, distinct from the
        // old physical tail. C=1 plus N=1 must retain both Context addresses.
        for work in 1..=3 {
            let completed = completed.clone();
            match handle.try_submit(job(2, work, move || {
                completed.fetch_add(1, Ordering::SeqCst);
            })) {
                Ok(()) => admitted += 1,
                Err(original) => refused.push(original),
            }
        }
        let ran_before_release = completed.load(Ordering::SeqCst);
        // Only the physical A+N bound is tested here. The queue itself grants
        // no business admission, including for a body whose key fits storage.
        let overflow = match handle.try_submit(job(2, 4, || {})) {
            Ok(()) => None,
            Err((reason, original)) => {
                let metadata = (reason, original.queue_key());
                refused.push((reason, original));
                Some(metadata)
            }
        };
        release_tx.send(()).unwrap();
        match executor.shutdown() {
            Ok(runner) => drop(runner),
            Err((reason, _owner)) => panic!("actual executor drain failed: {reason:?}"),
        }
        // Even a deliberately restored old geometry must release the gate,
        // join its actual worker and dispose original refusals before failing.
        drop(refused);
        assert_eq!(admitted, 3);
        assert_eq!(ran_before_release, 0);
        assert_eq!(
            overflow,
            Some((
                HostSubmitError::Queue(FairQueueError::Full),
                (ScopeId::new(2).unwrap(), WorkId::new(4).unwrap())
            ))
        );
        assert_eq!(completed.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn finite_workers_overlap_scopes_but_preserve_one_running_body_per_scope() {
        let executor = start(2);
        let handle = executor.handle();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_a_tx, release_a_rx) = mpsc::channel();
        let (release_b_tx, release_b_rx) = mpsc::channel();
        let tx = started_tx.clone();
        submit(
            &handle,
            job(1, 1, move || {
                tx.send((1, 1)).unwrap();
                release_a_rx.recv().unwrap();
            }),
        );
        assert_eq!(started_rx.recv_timeout(WAIT).unwrap(), (1, 1));
        let tx = started_tx.clone();
        submit(
            &handle,
            job(1, 2, move || {
                tx.send((1, 2)).unwrap();
            }),
        );
        let tx = started_tx.clone();
        submit(
            &handle,
            job(2, 1, move || {
                tx.send((2, 1)).unwrap();
                release_b_rx.recv().unwrap();
            }),
        );
        assert_eq!(started_rx.recv_timeout(WAIT).unwrap(), (2, 1));
        assert!(started_rx.try_recv().is_err());
        release_a_tx.send(()).unwrap();
        assert_eq!(started_rx.recv_timeout(WAIT).unwrap(), (1, 2));
        release_b_tx.send(()).unwrap();
        shutdown(executor);
    }

    #[test]
    fn fairness_group_is_independent_of_the_original_queued_and_running_owner_keys() {
        struct Grouped {
            group: HostFairnessGroup,
            original: Job,
        }
        impl QueuedPreparation for Grouped {
            fn queue_key(&self) -> (ScopeId, WorkId) {
                self.original.key
            }
            fn fairness_group(&self, _scope: ScopeId) -> HostFairnessGroup {
                self.group
            }
        }
        let group = |n| HostFairnessGroup::Scope(ScopeId::new(n).unwrap());
        let executor = HostExecutor::start(limits(2), |job: Grouped| run(job.original))
            .unwrap_or_else(|_| panic!("two original host workers"));
        let handle = executor.handle();
        let submit = |group, original| {
            handle
                .try_submit(Grouped { group, original })
                .unwrap_or_else(|_| panic!("bounded grouped original job"));
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (release_a_tx, release_a_rx) = mpsc::channel();
        let (release_b_tx, release_b_rx) = mpsc::channel();
        let tx = started_tx.clone();
        submit(
            group(80),
            job(1, 1, move || {
                tx.send(1).unwrap();
                release_a_rx.recv().unwrap();
            }),
        );
        assert_eq!(started_rx.recv_timeout(WAIT).unwrap(), 1);
        let tx = started_tx.clone();
        submit(
            group(80),
            job(2, 1, move || {
                tx.send(2).unwrap();
                release_b_rx.recv().unwrap();
            }),
        );
        // A queued real key cannot be recaptured using a different group.
        let queued_duplicate = handle
            .try_submit(Grouped {
                group: group(81),
                original: job(2, 1, || {}),
            })
            .err()
            .unwrap();
        let canceled = handle
            .cancel_queued(ScopeId::new(2).unwrap(), WorkId::new(1).unwrap())
            .expect("indexed cancel resolves the original group");
        let canceled_key = canceled.queue_key();
        let canceled_group = canceled.fairness_group(canceled_key.0);
        handle
            .try_submit(canceled)
            .unwrap_or_else(|_| panic!("same canceled original"));
        // Running members also remain indexed until their physical body exits.
        let running_duplicate = handle
            .try_submit(Grouped {
                group: group(81),
                original: job(1, 1, || {}),
            })
            .err()
            .unwrap();
        let tx = started_tx.clone();
        submit(
            group(81),
            job(3, 1, move || {
                tx.send(3).unwrap();
            }),
        );
        let next = started_rx.recv_timeout(WAIT);
        // Always release and join both real workers before checking fairness.
        // A regression that starts sibling 2 must fail, never hang teardown.
        release_a_tx.send(()).unwrap();
        release_b_tx.send(()).unwrap();
        drop(
            executor
                .shutdown()
                .unwrap_or_else(|_| panic!("physical grouped exit")),
        );
        assert_eq!(next.unwrap(), 3);
        assert_eq!(
            queued_duplicate.0,
            HostSubmitError::Queue(FairQueueError::DuplicateWork)
        );
        assert_eq!(
            running_duplicate.0,
            HostSubmitError::Queue(FairQueueError::DuplicateWork)
        );
        assert_eq!(queued_duplicate.1.queue_key(), canceled_key);
        assert_eq!(
            canceled_key,
            (ScopeId::new(2).unwrap(), WorkId::new(1).unwrap())
        );
        assert_eq!(canceled_group, group(80));
        drop(queued_duplicate);
        drop(running_duplicate);
    }

    #[test]
    fn closed_submission_returns_the_same_original_and_drains_old_jobs() {
        let executor = start(1);
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let ran = Arc::new(AtomicUsize::new(0));
        submit(
            &handle,
            job(1, 1, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        let second = ran.clone();
        submit(
            &handle,
            job(1, 2, move || {
                second.fetch_add(1, Ordering::SeqCst);
            }),
        );
        handle.close_external();
        let refused = job(2, 1, || panic!("refused original must not run"));
        let original_body = (&*refused.body as *const dyn FnOnce()) as *const ();
        let (reason, refused) = handle.try_submit(refused).err().unwrap();
        assert_eq!(reason, HostSubmitError::Closed);
        assert_eq!(
            (&*refused.body as *const dyn FnOnce()) as *const (),
            original_body
        );
        drop(refused);
        release_tx.send(()).unwrap();
        shutdown(executor);
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn callback_reenters_submission_without_executor_locks() {
        let executor = start(1);
        let handle = executor.handle();
        let (done_tx, done_rx) = mpsc::channel();
        let reenter = handle.clone();
        submit(
            &handle,
            job(1, 1, move || {
                let nested = done_tx.clone();
                submit(
                    &reenter,
                    job(2, 1, move || {
                        nested.send(()).unwrap();
                    }),
                );
            }),
        );
        done_rx.recv_timeout(WAIT).unwrap();
        shutdown(executor);
    }

    #[test]
    fn later_jobs_reuse_only_the_original_finite_worker_threads() {
        let executor = start(2);
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_a_tx, release_a_rx) = mpsc::channel();
        let (release_b_tx, release_b_rx) = mpsc::channel();
        let first = entered_tx.clone();
        submit(
            &handle,
            job(1, 1, move || {
                first.send(thread::current().id()).unwrap();
                release_a_rx.recv().unwrap();
            }),
        );
        let first_id = entered_rx.recv_timeout(WAIT).unwrap();
        submit(
            &handle,
            job(2, 1, move || {
                entered_tx.send(thread::current().id()).unwrap();
                release_b_rx.recv().unwrap();
            }),
        );
        let second_id = entered_rx.recv_timeout(WAIT).unwrap();
        assert_ne!(first_id, second_id);
        let (third_tx, third_rx) = mpsc::channel();
        submit(
            &handle,
            job(3, 1, move || {
                third_tx.send(thread::current().id()).unwrap();
            }),
        );
        release_a_tx.send(()).unwrap();
        let third_id = third_rx.recv_timeout(WAIT).unwrap();
        assert!(
            third_id == first_id || third_id == second_id,
            "no per-job thread is spawned"
        );
        release_b_tx.send(()).unwrap();
        shutdown(executor);
    }

    #[test]
    fn full_queue_returns_same_job_and_its_capture_drops_outside_submission_gate() {
        struct Capture(HostExecutorHandle<Job>, Arc<AtomicUsize>);
        impl Drop for Capture {
            fn drop(&mut self) {
                self.0.close_external();
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let mut bound = limits(1);
        bound.jobs = NonZeroUsize::new(2).unwrap();
        let executor = match HostExecutor::start(bound, run as fn(Job)) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual worker spawn: {error}"),
        };
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        submit(
            &handle,
            job(1, 1, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        submit(&handle, job(2, 1, || {}));
        let dropped = Arc::new(AtomicUsize::new(0));
        let capture = Capture(handle.clone(), dropped.clone());
        let original = job(3, 1, move || {
            let _capture = &capture;
        });
        let original_body = (&*original.body as *const dyn FnOnce()) as *const ();
        let (reason, returned) = handle.try_submit(original).err().unwrap();
        assert_eq!(reason, HostSubmitError::Queue(FairQueueError::Full));
        assert_eq!(
            (&*returned.body as *const dyn FnOnce()) as *const (),
            original_body
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(returned);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        release_tx.send(()).unwrap();
        shutdown(executor);
    }

    #[test]
    fn queued_scope_gets_its_turn_before_a_running_scope_tail() {
        let executor = start(1);
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        submit(
            &handle,
            job(1, 1, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        let (order_tx, order_rx) = mpsc::channel();
        let first = order_tx.clone();
        submit(
            &handle,
            job(1, 2, move || {
                first.send((1, 2)).unwrap();
            }),
        );
        submit(
            &handle,
            job(2, 1, move || {
                order_tx.send((2, 1)).unwrap();
            }),
        );
        release_tx.send(()).unwrap();
        assert_eq!(order_rx.recv_timeout(WAIT).unwrap(), (2, 1));
        assert_eq!(order_rx.recv_timeout(WAIT).unwrap(), (1, 2));
        shutdown(executor);
    }

    #[test]
    fn original_metadata_getter_may_reenter_and_close_submission() {
        struct ReentrantJob {
            owner: Option<HostExecutorHandle<ReentrantJob>>,
            identity: Arc<()>,
        }
        impl QueuedPreparation for ReentrantJob {
            fn queue_key(&self) -> (ScopeId, WorkId) {
                self.owner.as_ref().unwrap().close_external();
                (ScopeId::new(1).unwrap(), WorkId::new(1).unwrap())
            }
        }
        let executor = match HostExecutor::start(limits(1), |_job: ReentrantJob| {}) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual worker spawn: {error}"),
        };
        let handle = executor.handle();
        let identity = Arc::new(());
        let original = ReentrantJob {
            owner: Some(handle.clone()),
            identity: identity.clone(),
        };
        let (reason, returned) = match handle.try_submit(original) {
            Err(refused) => refused,
            Ok(()) => panic!("getter closed external admission"),
        };
        assert_eq!(reason, HostSubmitError::Closed);
        assert!(Arc::ptr_eq(&returned.identity, &identity));
        drop(returned);
        match executor.shutdown() {
            Ok(runner) => drop(runner),
            Err((reason, _owner)) => panic!("actual physical drain: {reason:?}"),
        }
    }

    #[test]
    fn runner_capture_is_returned_and_its_destructor_runs_without_executor_locks() {
        struct Capture {
            handle: Arc<Mutex<Option<HostExecutorHandle<Job>>>>,
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                let handle = self.handle.lock().unwrap().clone().unwrap();
                let refused = handle.try_submit(job(1, 1, || panic!("closed runner capture job")));
                assert!(matches!(refused, Err((HostSubmitError::Closed, _))));
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let handle_slot = Arc::new(Mutex::new(None));
        let dropped = Arc::new(AtomicUsize::new(0));
        let capture = Capture {
            handle: handle_slot.clone(),
            dropped: dropped.clone(),
        };
        let executor = match HostExecutor::start(limits(1), move |job: Job| {
            let _retained_capture = &capture;
            run(job);
        }) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual worker spawn: {error}"),
        };
        *handle_slot.lock().unwrap() = Some(executor.handle());
        let runner = match executor.shutdown() {
            Ok(runner) => runner,
            Err((reason, _owner)) => panic!("actual physical drain: {reason:?}"),
        };
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(runner);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn surviving_worker_drains_old_jobs_after_sibling_failure() {
        struct OriginalPanic(Arc<AtomicUsize>);
        impl Drop for OriginalPanic {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let executor = start(2);
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (panic_tx, panic_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let dropped = Arc::new(AtomicUsize::new(0));
        let payload = OriginalPanic(dropped.clone());
        let first = entered_tx.clone();
        submit(
            &handle,
            job(1, 1, move || {
                first.send(()).unwrap();
                panic_rx.recv().unwrap();
                std::panic::panic_any(payload);
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        submit(
            &handle,
            job(2, 1, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        let (drained_tx, drained_rx) = mpsc::channel();
        submit(
            &handle,
            job(1, 2, move || {
                drained_tx.send(()).unwrap();
            }),
        );
        panic_tx.send(()).unwrap();
        loop {
            let observed = executor.shared.ready.version();
            if executor.shared.failed() {
                break;
            }
            executor.shared.wait_after(observed);
        }
        let (reason, refused) = handle
            .try_submit(job(3, 1, || panic!("failed admission")))
            .err()
            .unwrap();
        assert_eq!(reason, HostSubmitError::DriverFailed);
        drop(refused);
        release_tx.send(()).unwrap();
        drained_rx.recv_timeout(WAIT).unwrap();
        let (reason, owner) = match executor.shutdown() {
            Err(failed) => failed,
            Ok(_) => panic!("original panic is still unknown"),
        };
        assert_eq!(reason, HostShutdownError::DriverFailed);
        assert!(owner.shared.queue.is_empty());
        assert!(owner.threads.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        let _retained_owner = ManuallyDrop::new(owner);
    }

    #[test]
    fn cancellation_returns_the_original_without_running_or_disposing_it() {
        let executor = start(1);
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        submit(
            &handle,
            job(1, 1, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        let original = job(1, 2, || panic!("removed original body must not run"));
        let original_body = (&*original.body as *const dyn FnOnce()) as *const ();
        submit(&handle, original);
        let returned = handle
            .cancel_queued(ScopeId::new(1).unwrap(), WorkId::new(2).unwrap())
            .unwrap();
        assert_eq!(
            (&*returned.body as *const dyn FnOnce()) as *const (),
            original_body
        );
        drop(returned);
        release_tx.send(()).unwrap();
        shutdown(executor);
    }

    #[cfg(unix)]
    #[test]
    fn shared_driver_invariant_failure_is_process_fail_stop() {
        use std::os::unix::process::ExitStatusExt;
        const CHILD: &str = "NOVAROCKS_HOST_DRIVER_FAILURE_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let shared = Arc::new(Shared::<Job> {
                queue: PreparationQueue::new(1, 1, 1, 1),
                gate: Mutex::new(Gate {
                    accepting: true,
                    workers: vec![WorkerSlot {
                        state: WorkerState::Alive,
                        panic: None,
                    }]
                    .into_boxed_slice(),
                }),
                ready: Notify::default(),
                waker: None,
            });
            let _original = catch_unwind(AssertUnwindSafe(|| {
                let _gate = shared.gate.lock().unwrap();
                panic!("controlled shared metadata poison");
            }));
            worker_entry(shared, Arc::new(run as fn(Job)), 0);
            panic!("poisoned driver must not return or sign an exit");
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("control_runtime::host_executor::tests::shared_driver_invariant_failure_is_process_fail_stop")
            .arg("--nocapture")
            .env(CHILD,"1")
            .env("RUST_BACKTRACE","0")
            .output().unwrap();
        assert_eq!(
            output.status.signal(),
            Some(6),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn readiness_between_predicate_and_sleep_is_not_lost() {
        // This is the same next_body/wait split used by every physical worker.
        let shared = Arc::new(Shared::<Job> {
            queue: PreparationQueue::new(1, 1, 1, 1),
            gate: Mutex::new(Gate {
                accepting: true,
                workers: Box::new([]),
            }),
            ready: Notify::default(),
            waker: None,
        });
        let observed = match shared.next_body() {
            NextBody::Wait(v) => v,
            _ => panic!("empty open queue"),
        };
        let handle = HostExecutorHandle {
            shared: shared.clone(),
        };
        submit(&handle, job(1, 1, || {}));
        let (returned_tx, returned_rx) = mpsc::channel();
        let waiting = shared.clone();
        let waiter = thread::spawn(move || {
            waiting.wait_after(observed);
            returned_tx.send(()).unwrap();
        });
        returned_rx.recv_timeout(WAIT).unwrap();
        waiter.join().unwrap();
        match shared.next_body() {
            NextBody::Run(body) => body.run_body(|job| run(job.original)),
            _ => panic!("original queued body"),
        }
        assert!(shared.queue.is_empty());
    }

    #[test]
    fn callback_panic_releases_physical_slot_and_retains_bounded_failure_custody() {
        let executor = start(1);
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        submit(
            &handle,
            job(1, 1, move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                panic!("original host body panic");
            }),
        );
        entered_rx.recv_timeout(WAIT).unwrap();
        submit(
            &handle,
            job(1, 2, || {
                panic!("retained queued job must not run without a physical worker")
            }),
        );
        release_tx.send(()).unwrap();
        let observed = executor.shared.ready.version();
        if !executor.shared.failed() {
            executor.shared.wait_after(observed);
        }
        let (reason, executor) = match executor.shutdown() {
            Err(value) => value,
            Ok(_) => panic!("unknown job cannot be normal shutdown"),
        };
        assert_eq!(reason, HostShutdownError::UndrainedAfterFailure);
        let retained = executor
            .shared
            .queue
            .pop()
            .expect("panicking body returned original physical slot");
        std::mem::forget(retained);
        assert_eq!(
            executor
                .shared
                .gate
                .lock()
                .unwrap()
                .workers
                .iter()
                .filter(|w| w.panic.is_some())
                .count(),
            1
        );
        // The failed original owner remains retained. No Work exit or report
        // consumption is synthesized by this physical-driver test.
        let _retained_owner = ManuallyDrop::new(executor);
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RetryWake {
        Idle,
        Accepted,
        Refused(HostSubmitError),
    }
    #[derive(Clone, Copy, Debug)]
    struct WakeObservation {
        empty: bool,
        closed: bool,
        failed: bool,
        exited: bool,
        retry: RetryWake,
    }
    struct CapacityWake {
        handle: Mutex<Option<HostExecutorHandle<Job>>>,
        pending: Mutex<Option<Job>>,
        notices: mpsc::Sender<WakeObservation>,
    }
    impl std::task::Wake for CapacityWake {
        fn wake(self: Arc<Self>) {
            std::task::Wake::wake_by_ref(&self);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let handle = self.handle.lock().unwrap().clone();
            let Some(handle) = handle else { return };
            // Real reentrant reads and submission deliberately acquire both
            // executor locks. The standard wake must not be called under them.
            let empty = handle.shared.queue.is_empty();
            let (closed, failed, exited) = {
                let gate = handle.shared.gate.lock().unwrap();
                (
                    !gate.accepting,
                    gate.workers
                        .iter()
                        .any(|worker| worker.state == WorkerState::Failed),
                    gate.workers
                        .iter()
                        .any(|worker| worker.state == WorkerState::Exited),
                )
            };
            let original = self.pending.lock().unwrap().take();
            let retry = match original {
                None => RetryWake::Idle,
                Some(original) => match handle.try_submit(original) {
                    Ok(()) => RetryWake::Accepted,
                    Err((reason, original)) => {
                        assert!(self.pending.lock().unwrap().replace(original).is_none());
                        RetryWake::Refused(reason)
                    }
                },
            };
            let _sent = self.notices.send(WakeObservation {
                empty,
                closed,
                failed,
                exited,
                retry,
            });
        }
    }

    #[test]
    fn the_real_physical_tail_reawakens_a_consumed_busy_retry_and_the_wake_can_reenter_submission()
    {
        let (wake_tx, wake_rx) = mpsc::channel();
        let (retried_tx, retried_rx) = mpsc::channel();
        let identity = Arc::new(vec![19_u8]);
        let captured = identity.clone();
        let retry = job(1, 1, move || retried_tx.send(captured).unwrap());
        let original_body = (&*retry.body as *const dyn FnOnce()) as *const ();
        let probe = Arc::new(CapacityWake {
            handle: Mutex::new(None),
            pending: Mutex::new(Some(retry)),
            notices: wake_tx,
        });
        let waker = Waker::from(probe.clone());
        let business_waker = waker.clone();
        let (business_tx, business_rx) = mpsc::channel();
        let (tail_tx, tail_rx) = mpsc::channel();
        let tail = Mutex::new(tail_rx);
        let calls = AtomicUsize::new(0);
        let executor = match HostExecutor::start_with_waker(
            HostExecutorLimits {
                contexts: NonZeroUsize::new(2).unwrap(),
                jobs: NonZeroUsize::new(2).unwrap(),
                per_context: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(1).unwrap(),
            },
            move |original: Job| {
                // The actual business call has returned. A domain notice may
                // already wake its owner, while this real runner still retains
                // RunningPreparation's physical membership/key below.
                run(original);
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    business_waker.wake_by_ref();
                    tail.lock().unwrap().recv().unwrap();
                }
            },
            waker,
        ) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual physical worker startup: {error}"),
        };
        let handle = executor.handle();
        *probe.handle.lock().unwrap() = Some(handle.clone());
        submit(&handle, job(1, 1, move || business_tx.send(()).unwrap()));
        business_rx.recv_timeout(WAIT).unwrap();
        // Consume the early business notice while the true key remains Busy.
        let busy = wake_rx.recv_timeout(WAIT).unwrap();
        let same_refused_job = probe
            .pending
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|original| {
                (&*original.body as *const dyn FnOnce()) as *const () == original_body
            });
        let (queued_tx, queued_rx) = mpsc::channel();
        submit(&handle, job(2, 1, move || queued_tx.send(()).unwrap()));
        let full_capture = identity.clone();
        let full = job(1, 3, move || {
            panic!(
                "the refused full original cannot run: {}",
                full_capture.len()
            )
        });
        let full_body = (&*full.body as *const dyn FnOnce()) as *const ();
        let (full_error, full) = handle.try_submit(full).err().unwrap();
        let full_original = (&*full.body as *const dyn FnOnce()) as *const () == full_body;
        drop(full);
        let canceled = handle
            .cancel_queued(ScopeId::new(2).unwrap(), WorkId::new(1).unwrap())
            .unwrap();
        let cancel_wake = wake_rx.recv_timeout(WAIT).unwrap();
        submit(&handle, canceled);
        let no_unearned_tail_notice = wake_rx.try_recv().is_err();
        tail_tx.send(()).unwrap();
        // Only the real run_body return frees this exact key; its subsequent
        // wake reenters try_submit and accepts the same previously refused J.
        let available = wake_rx.recv_timeout(WAIT).unwrap();
        let same_capture = Arc::ptr_eq(&retried_rx.recv_timeout(WAIT).unwrap(), &identity);
        queued_rx.recv_timeout(WAIT).unwrap();
        drop(
            executor
                .shutdown()
                .unwrap_or_else(|_| panic!("true physical tail drain")),
        );
        let closed_capture = identity.clone();
        let closed = job(3, 1, move || {
            panic!(
                "the original closed job cannot run: {}",
                closed_capture.len()
            )
        });
        let closed_body = (&*closed.body as *const dyn FnOnce()) as *const ();
        let (closed_error, closed) = handle.try_submit(closed).err().unwrap();
        let closed_original = (&*closed.body as *const dyn FnOnce()) as *const () == closed_body;
        drop(closed);
        let later = wake_rx.try_iter().collect::<Vec<_>>();
        probe.handle.lock().unwrap().take();
        drop(handle);

        assert!(
            !busy.empty
                && busy.retry
                    == RetryWake::Refused(HostSubmitError::Queue(FairQueueError::DuplicateWork))
        );
        assert!(same_refused_job && full_original && same_capture && closed_original);
        assert_eq!(full_error, HostSubmitError::Queue(FairQueueError::Full));
        assert_eq!(closed_error, HostSubmitError::Closed);
        assert_eq!(
            cancel_wake.retry,
            RetryWake::Refused(HostSubmitError::Queue(FairQueueError::DuplicateWork))
        );
        assert!(no_unearned_tail_notice && available.retry == RetryWake::Accepted);
        assert!(later.iter().any(|notice| notice.closed));
        assert!(later.iter().any(|notice| notice.exited));
    }

    #[test]
    fn a_real_worker_failure_wakes_the_external_owner_after_retaining_its_original_panic() {
        let (wake_tx, wake_rx) = mpsc::channel();
        let probe = Arc::new(CapacityWake {
            handle: Mutex::new(None),
            pending: Mutex::new(None),
            notices: wake_tx,
        });
        let executor = match HostExecutor::start_with_waker(
            limits(1),
            run as fn(Job),
            Waker::from(probe.clone()),
        ) {
            Ok(executor) => executor,
            Err((error, _owner)) => panic!("actual physical worker startup: {error}"),
        };
        let handle = executor.handle();
        *probe.handle.lock().unwrap() = Some(handle.clone());
        submit(
            &handle,
            job(1, 1, || panic!("the original physical runner failure")),
        );
        let observed = wake_rx.recv_timeout(WAIT).unwrap();
        let (reason, owner) = match executor.shutdown() {
            Err(original) => original,
            Ok(runner) => {
                drop(runner);
                panic!("a retained panic cannot prove successful shutdown");
            }
        };
        let retained_panic = owner
            .shared
            .gate
            .lock()
            .unwrap()
            .workers
            .iter()
            .filter(|worker| worker.panic.is_some())
            .count()
            == 1;
        probe.handle.lock().unwrap().take();
        drop(handle);
        // Mechanical driver failure custody only: the original panic remains
        // quarantined; no business Work, report or capture exit is fabricated.
        let _retained_owner = ManuallyDrop::new(owner);
        assert!(observed.empty && observed.closed && observed.failed);
        assert_eq!(reason, HostShutdownError::DriverFailed);
        assert!(retained_panic);
    }
}
