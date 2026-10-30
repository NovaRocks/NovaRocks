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

use std::{
    any::Any,
    collections::BTreeMap,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

use crate::cancellation::{QueryCancellationReason, QueryCancellationView};

pub(crate) mod bounded_worker;
mod preserving_cpu;
use bounded_worker::{
    BoundedWorkerConfig, BoundedWorkerHandle, BoundedWorkerJob, BoundedWorkerOwner,
};
pub use preserving_cpu::{
    QueryCpuPrepareRefusal, QueryCpuPreparedJob, QueryCpuPreservingGeometry,
    QueryCpuPublishRefusal, QueryCpuReceipt,
};

type CpuResult = Box<dyn Any + Send>;

/// Original opaque failure custody. No formatter or destructor is invoked when
/// classifying or sharing a worker failure. The real receiver owns disposition.
pub struct QueryBlockingFailurePayload {
    original: Mutex<Box<dyn Any + Send>>,
}
impl QueryBlockingFailurePayload {
    fn new(original: Box<dyn Any + Send>) -> Arc<Self> {
        Arc::new(Self {
            original: Mutex::new(original),
        })
    }
    fn is<T: Any>(&self) -> bool {
        self.original
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is::<T>()
    }
}
impl std::fmt::Debug for QueryBlockingFailurePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueryBlockingFailurePayload")
    }
}

/// Safe presentation and the same original worker cause remain separate.
/// Cloning this carrier shares its original payload; it never clones that value.
#[derive(Clone, Debug)]
pub enum QueryBlockingFailure {
    Panicked(Arc<QueryBlockingFailurePayload>),
    ExecutorClosed,
    SubmissionClosed,
    InvalidResultType(Arc<QueryBlockingFailurePayload>),
}
impl QueryBlockingFailure {
    /// Known requested layout of the original opaque-payload carrier only.
    /// This includes its Arc header and the actual Mutex/Box handle fields.
    /// It excludes the arbitrary original Box pointee and any platform Mutex
    /// backing outside its public inline layout. No capacity is granted.
    pub fn payload_holder_layout() -> Result<
        std::alloc::Layout,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            QueryBlockingFailurePayload,
        >())
    }
    pub fn panicked(original: Box<dyn Any + Send>) -> Self {
        Self::Panicked(QueryBlockingFailurePayload::new(original))
    }
    pub fn is_unexpected(&self) -> bool {
        !matches!(self, Self::SubmissionClosed)
    }
    pub fn original_payload_is<T: Any>(&self) -> bool {
        match self {
            Self::Panicked(payload) | Self::InvalidResultType(payload) => payload.is::<T>(),
            Self::ExecutorClosed | Self::SubmissionClosed => false,
        }
    }
    pub fn shares_original_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Panicked(a), Self::Panicked(b))
            | (Self::InvalidResultType(a), Self::InvalidResultType(b)) => Arc::ptr_eq(a, b),
            (Self::ExecutorClosed, Self::ExecutorClosed)
            | (Self::SubmissionClosed, Self::SubmissionClosed) => true,
            _ => false,
        }
    }
}
impl PartialEq for QueryBlockingFailure {
    fn eq(&self, other: &Self) -> bool {
        self.shares_original_with(other)
    }
}
impl Eq for QueryBlockingFailure {}
impl std::fmt::Display for QueryBlockingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Panicked(_) => "bounded worker worker panicked",
            Self::ExecutorClosed => "bounded worker executor closed before completing the job",
            Self::SubmissionClosed => "query blocking executor closed before admitting work",
            Self::InvalidResultType(_) => "query blocking worker returned an invalid result type",
        })
    }
}
impl std::error::Error for QueryBlockingFailure {}
impl novarocks_failure_contract::FailureProjection for QueryBlockingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::execution_error::QueryFailureReason;
        match self {
            Self::Panicked(_) => QueryFailureReason::BlockingWorkerPanicked,
            Self::ExecutorClosed | Self::SubmissionClosed => {
                QueryFailureReason::BlockingExecutorClosed
            }
            Self::InvalidResultType(_) => QueryFailureReason::BlockingSourceContract,
        }
        .failure_fact()
    }
}

/// Reuse window for one idle synchronous-preparation worker. This does not
/// bound query concurrency or keep a backlog: every admitted job either takes
/// an idle worker or causes one new worker to be created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCpuExecutorConfig {
    idle_keepalive: Duration,
}

impl QueryCpuExecutorConfig {
    pub const fn with_idle_keepalive(idle_keepalive: Duration) -> Self {
        Self { idle_keepalive }
    }

    pub const fn idle_keepalive(self) -> Duration {
        self.idle_keepalive
    }
}

/// The Frontend process holds this unique owner for its entire lifetime.
///
/// Query sessions receive only [`QueryCpuExecutor`], so closing or dropping a
/// session cannot close the CPU queue or join its fixed workers.
pub struct QueryCpuExecutorOwner {
    pool: Arc<ElasticPlanningPool>,
    executor: QueryCpuExecutor,
}

#[derive(Clone)]
pub struct QueryCpuExecutor {
    pool: Arc<ElasticPlanningPool>,
}

type WorkerId = u64;

enum ElasticPlanningJob {
    Legacy {
        run: Box<dyn FnOnce() -> CpuResult + Send>,
        result: oneshot::Sender<Result<CpuResult, String>>,
    },
    Preserving(Arc<preserving_cpu::CpuWork>),
}

enum WorkerCommand {
    Run(ElasticPlanningJob),
    Shutdown,
}

enum ElasticWorkerState {
    Starting,
    Running,
    Idle(Sender<WorkerCommand>),
    Assigned,
    Retiring,
    Failed {
        // The failed row retains the original work even when no reader needs
        // its contents. Actual thread exit is not failure disposition.
        _original: Arc<preserving_cpu::CpuWork>,
    },
}

struct ElasticPlanningPoolState {
    accepting: bool,
    next_worker_id: WorkerId,
    workers: BTreeMap<WorkerId, ElasticWorkerState>,
    // Accepted starts stay owed through publication AND the original join,
    // even when the actual worker already exited and removed its worker row.
    unjoined_starts: usize,
    joins: BTreeMap<WorkerId, JoinHandle<()>>,
}

impl Default for ElasticPlanningPoolState {
    fn default() -> Self {
        Self {
            accepting: true,
            next_worker_id: 0,
            workers: BTreeMap::new(),
            unjoined_starts: 0,
            joins: BTreeMap::new(),
        }
    }
}

struct ElasticPlanningPool {
    idle_keepalive: Duration,
    state: Mutex<ElasticPlanningPoolState>,
}

impl ElasticPlanningPool {
    fn try_new(config: QueryCpuExecutorConfig) -> Result<Self, String> {
        if config.idle_keepalive().is_zero() {
            return Err("query CPU executor idle keepalive must be positive".to_owned());
        }
        Ok(Self {
            idle_keepalive: config.idle_keepalive(),
            state: Mutex::new(ElasticPlanningPoolState::default()),
        })
    }

    fn submit(
        self: &Arc<Self>,
        run: impl FnOnce() -> CpuResult + Send + 'static,
    ) -> Result<oneshot::Receiver<Result<CpuResult, String>>, String> {
        self.submit_with_spawn_gates(run, || {}, |_| {})
    }

    // Production uses the zero-sized no-op above. Tests can pause this SAME
    // creator around the real Builder::spawn without substituting a worker or
    // publishing a synthetic join. Both gates always run outside State.
    fn submit_with_spawn_gates(
        self: &Arc<Self>,
        run: impl FnOnce() -> CpuResult + Send + 'static,
        before_spawn: impl FnOnce(),
        before_publication: impl FnOnce(&JoinHandle<()>),
    ) -> Result<oneshot::Receiver<Result<CpuResult, String>>, String> {
        self.reap_finished();
        let (result, receipt) = oneshot::channel();
        let job = ElasticPlanningJob::Legacy {
            run: Box::new(run),
            result,
        };
        let assignment = self.assign_worker().map_err(|error| error.to_string())?;
        match assignment {
            ElasticAssignment::Idle { id, sender } => match sender.send(WorkerCommand::Run(job)) {
                Ok(()) => Ok(receipt),
                Err(error) => {
                    self.state.lock().unwrap().workers.remove(&id);
                    let WorkerCommand::Run(job) = error.0 else {
                        unreachable!("only a job is assigned to an idle worker")
                    };
                    let ElasticPlanningJob::Legacy { result, .. } = job else {
                        unreachable!("legacy submission keeps its original receipt")
                    };
                    let _ = result.send(Err(
                        "query CPU worker exited before accepting work".to_owned()
                    ));
                    Ok(receipt)
                }
            },
            ElasticAssignment::Start { id } => {
                let (sender, receiver) = mpsc::channel();
                let pool = Arc::clone(self);
                before_spawn();
                let spawned = std::thread::Builder::new()
                    .name(format!("novarocks-query-planning-{id}"))
                    // Planning traverses nested SQL trees on this worker,
                    // outside the runtime's explicitly sized worker threads.
                    .stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                    .spawn(move || worker_loop(pool, id, sender, receiver, job));
                if let Ok(join) = &spawned {
                    before_publication(join);
                }
                match spawned {
                    Ok(join) => {
                        let mut state = self.state.lock().unwrap();
                        state.joins.insert(id, join);
                        drop(state);
                        Ok(receipt)
                    }
                    Err(error) => {
                        let detail = format!("create query CPU worker: {error}");
                        drop(error);
                        let mut state = self.state.lock().unwrap();
                        state.workers.remove(&id);
                        state.unjoined_starts -= 1;
                        drop(state);
                        Err(detail)
                    }
                }
            }
        }
    }

    fn assign_worker(&self) -> Result<ElasticAssignment, ElasticAssignmentError> {
        Ok({
            let mut state = self.state.lock().unwrap();
            if !state.accepting {
                return Err(ElasticAssignmentError::Closed);
            }
            if let Some((id, sender)) = state.workers.iter().find_map(|(&id, worker)| {
                matches!(worker, ElasticWorkerState::Idle(_)).then(|| match worker {
                    ElasticWorkerState::Idle(sender) => (id, sender.clone()),
                    _ => unreachable!(),
                })
            }) {
                state.workers.insert(id, ElasticWorkerState::Assigned);
                ElasticAssignment::Idle { id, sender }
            } else {
                let id = state
                    .next_worker_id
                    .checked_add(1)
                    .ok_or(ElasticAssignmentError::WorkerIdOverflow)?;
                let unjoined_starts = state
                    .unjoined_starts
                    .checked_add(1)
                    .ok_or(ElasticAssignmentError::JoinCountOverflow)?;
                state.next_worker_id = id;
                state.unjoined_starts = unjoined_starts;
                state.workers.insert(id, ElasticWorkerState::Starting);
                ElasticAssignment::Start { id }
            }
        })
    }

    fn request_close(&self) {
        let senders = {
            let mut state = self.state.lock().unwrap();
            state.accepting = false;
            let ids = state
                .workers
                .iter()
                .filter_map(|(&id, worker)| match worker {
                    ElasticWorkerState::Idle(sender) => Some((id, sender.clone())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            for (id, _) in &ids {
                state.workers.insert(*id, ElasticWorkerState::Retiring);
            }
            ids
        };
        for (_, sender) in senders {
            let _ = sender.send(WorkerCommand::Shutdown);
        }
    }

    async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.request_close();
        loop {
            self.reap_finished();
            let drained = {
                let state = self.state.lock().unwrap();
                state.unjoined_starts == 0
                    && state.joins.is_empty()
                    && !state
                        .workers
                        .values()
                        .any(|worker| matches!(worker, ElasticWorkerState::Failed { .. }))
            };
            if drained {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("query CPU workers did not exit before shutdown deadline".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn reap_finished(&self) {
        self.reap_finished_with_join_gate(|_| {});
    }

    // The production gate is a zero-sized no-op. Tests may retain the actual
    // original handle between taking it from State and the same lock-free join.
    fn reap_finished_with_join_gate(&self, mut before_join: impl FnMut(&JoinHandle<()>)) {
        let joins = {
            let mut state = self.state.lock().unwrap();
            let completed = state
                .joins
                .iter()
                .filter_map(|(&id, join)| join.is_finished().then_some(id))
                .collect::<Vec<_>>();
            completed
                .into_iter()
                .filter_map(|id| state.joins.remove(&id))
                .collect::<Vec<_>>()
        };
        for join in joins {
            before_join(&join);
            // The original outcome (including a panic payload's destruction)
            // is consumed outside State before this accepted start retires.
            let _ = join.join();
            self.state.lock().unwrap().unjoined_starts -= 1;
        }
    }

    #[cfg(test)]
    fn snapshot(&self) -> ElasticPlanningPoolSnapshot {
        let state = self.state.lock().unwrap();
        ElasticPlanningPoolSnapshot {
            workers: state.workers.len(),
            unjoined_starts: state.unjoined_starts,
            joins: state.joins.len(),
            idle: state
                .workers
                .values()
                .filter(|worker| matches!(worker, ElasticWorkerState::Idle(_)))
                .count(),
            running: state
                .workers
                .values()
                .filter(|worker| matches!(worker, ElasticWorkerState::Running))
                .count(),
            accepting: state.accepting,
        }
    }
}

#[derive(Debug)]
enum ElasticAssignmentError {
    Closed,
    WorkerIdOverflow,
    JoinCountOverflow,
}

impl std::fmt::Display for ElasticAssignmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "query CPU executor closed before admitting work",
            Self::WorkerIdOverflow => "query CPU worker id overflow",
            Self::JoinCountOverflow => "query CPU worker join obligation count overflow",
        })
    }
}

enum ElasticAssignment {
    Idle {
        id: WorkerId,
        sender: Sender<WorkerCommand>,
    },
    Start {
        id: WorkerId,
    },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ElasticPlanningPoolSnapshot {
    workers: usize,
    unjoined_starts: usize,
    joins: usize,
    idle: usize,
    running: usize,
    accepting: bool,
}

fn worker_loop(
    pool: Arc<ElasticPlanningPool>,
    id: WorkerId,
    sender: Sender<WorkerCommand>,
    receiver: Receiver<WorkerCommand>,
    mut job: ElasticPlanningJob,
) {
    loop {
        {
            let mut state = pool.state.lock().unwrap();
            let Some(worker) = state.workers.get_mut(&id) else {
                return;
            };
            *worker = ElasticWorkerState::Running;
        }
        let preserving = match job {
            ElasticPlanningJob::Legacy { run, result } => {
                let output = catch_unwind(AssertUnwindSafe(run));
                let panicked = output.is_err();
                let output = output.map_err(|_| "query CPU worker panicked".to_owned());
                let _ = result.send(output);
                if panicked {
                    pool.state.lock().unwrap().workers.remove(&id);
                    return;
                }
                None
            }
            ElasticPlanningJob::Preserving(work) => {
                work.run(&pool, id);
                Some(work)
            }
        };

        if let Some(work) = &preserving {
            work.finish_custody(&pool, id);
        }
        let should_retire = {
            let mut state = pool.state.lock().unwrap();
            if !state.accepting {
                state.workers.remove(&id);
                true
            } else {
                state
                    .workers
                    .insert(id, ElasticWorkerState::Idle(sender.clone()));
                false
            }
        };
        // The call, captures and undelivered outcome are gone and this
        // original worker is no longer Running before foreign exit wake.
        if let Some(work) = preserving {
            work.publish_exit(&pool, id);
        }
        if should_retire {
            return;
        }

        let next = match receiver.recv_timeout(pool.idle_keepalive) {
            Ok(WorkerCommand::Run(job)) => job,
            Ok(WorkerCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                pool.state.lock().unwrap().workers.remove(&id);
                return;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let assigned = {
                    let mut state = pool.state.lock().unwrap();
                    match state.workers.get(&id) {
                        Some(ElasticWorkerState::Assigned) => true,
                        Some(ElasticWorkerState::Idle(_)) | Some(ElasticWorkerState::Retiring) => {
                            state.workers.remove(&id);
                            false
                        }
                        _ => false,
                    }
                };
                if assigned {
                    match receiver.recv() {
                        Ok(WorkerCommand::Run(job)) => job,
                        Ok(WorkerCommand::Shutdown) | Err(_) => {
                            pool.state.lock().unwrap().workers.remove(&id);
                            return;
                        }
                    }
                } else {
                    return;
                }
            }
        };
        job = next;
    }
}

/// The outcome of waiting for query-preparation CPU work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCpuRunError {
    Cancelled(QueryCancellationReason),
    Executor(String),
}

/// Fixed process limits for synchronous query command edges.
///
/// This is deliberately separate from [`QueryCpuExecutorConfig`]: command
/// routes may call providers or wait on durable catalog work, so charging them
/// to compiler CPU capacity would let those waits starve pure preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryBlockingExecutorConfig {
    worker_threads: NonZeroUsize,
    queue_capacity: NonZeroUsize,
}

impl QueryBlockingExecutorConfig {
    pub const fn new(worker_threads: NonZeroUsize, queue_capacity: NonZeroUsize) -> Self {
        Self {
            worker_threads,
            queue_capacity,
        }
    }

    pub const fn worker_threads(self) -> NonZeroUsize {
        self.worker_threads
    }

    pub const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }
}

/// The process owner for bounded synchronous command edges that have not yet
/// acquired an asynchronous provider contract.
pub struct QueryBlockingExecutorOwner {
    owner: BoundedWorkerOwner<CpuResult>,
    executor: QueryBlockingExecutor,
}

#[derive(Clone)]
pub struct QueryBlockingExecutor {
    handle: BoundedWorkerHandle<CpuResult>,
}

/// Original worker teardown failure, retained in the same fixed worker entry.
/// Taking this value moves that exact custody; it does not acknowledge health
/// or prove the held physical source has exited. Drop is explicit disposition.
pub struct QueryBlockingWorkerFailure {
    original: Box<dyn Any + Send>,
}
impl QueryBlockingWorkerFailure {
    fn new(original: Box<dyn Any + Send>) -> Self {
        Self { original }
    }
    pub fn cause(&self) -> Option<&QueryBlockingFailure> {
        self.original
            .downcast_ref::<bounded_worker::WorkerRetirementFailure>()
            .map(|failure| failure.cause())
    }
    pub fn notification(&self, index: usize) -> Option<&QueryBlockingFailure> {
        self.original
            .downcast_ref::<bounded_worker::WorkerRetirementFailure>()?
            .notification(index)
    }
    pub fn original_payload_is<T: Any>(&self) -> bool {
        self.cause().map_or_else(
            || self.original.is::<T>(),
            |cause| cause.original_payload_is::<T>(),
        )
    }
}
impl std::fmt::Debug for QueryBlockingWorkerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBlockingWorkerFailure")
            .finish_non_exhaustive()
    }
}

pub use bounded_worker::{
    QueuePublishRefusal as QueryBlockingPublishRefusal,
    QueueReserveRefusal as QueryBlockingReserveRefusal,
    WorkerActualExitFailure as QueryBlockingActualExitFailure,
};

/// Only a position in the original pool, never a workload/finite-source grant.
/// Its private weak identity prevents cross-pool publication without a new ID.
#[must_use = "Keep the original reservation until acceptance or real refusal cleanup"]
pub struct QueryBlockingReservation {
    inner: bounded_worker::BoundedWorkerReservation,
}
impl std::fmt::Debug for QueryBlockingReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBlockingReservation")
            .finish_non_exhaustive()
    }
}
/// A normal preparation refusal preserves the exact unexecuted F.
pub struct QueryBlockingPrepareRefusal<F> {
    reason: QueryBlockingPublishRefusal,
    work: F,
}
impl<F> QueryBlockingPrepareRefusal<F> {
    pub fn reason(&self) -> QueryBlockingPublishRefusal {
        self.reason
    }
    pub fn into_work(self) -> F {
        self.work
    }
}
impl<F> std::fmt::Debug for QueryBlockingPrepareRefusal<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBlockingPrepareRefusal")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// A named, concrete blocking source. Its business input/result must already
/// reside in the caller's stable original custody; this borrowed call is never
/// the unique owner. Implement on the provider's private purpose type, not a
/// caller-supplied closure. This trait grants no workload or provider authority.
pub trait QueryBlockingOriginal: Send + Sync + 'static {
    fn run_original(&self);
}

/// Purpose-typed view of the existing ordinary pool. It has no new worker,
/// queue, source registry, finite stamp, or workload grant. The composition
/// author retains responsibility for the concrete original's authority.
pub struct QueryBlockingOriginalExecutor<O> {
    executor: QueryBlockingExecutor,
    _original: std::marker::PhantomData<fn() -> O>,
}
impl<O> Clone for QueryBlockingOriginalExecutor<O> {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            _original: std::marker::PhantomData,
        }
    }
}
fn original_call<O: QueryBlockingOriginal>(original: Arc<O>) -> impl FnOnce() -> CpuResult + Send {
    move || {
        original.run_original();
        Box::new(()) as CpuResult
    }
}
fn original_call_type<O: QueryBlockingOriginal>() -> std::any::TypeId {
    // Name the same opaque call type without constructing an input or
    // invoking provider code.
    fn returned_type<A, F: 'static>(_: fn(A) -> F) -> std::any::TypeId {
        std::any::TypeId::of::<F>()
    }
    returned_type(original_call::<O>)
}
impl<O: QueryBlockingOriginal> QueryBlockingOriginalExecutor<O> {
    /// Same cancellable Semaphore wait; callers select their original deadline
    /// and cancellation around this F while keeping Original outside it.
    pub async fn reserve(&self) -> Result<QueryBlockingReservation, QueryBlockingReserveRefusal> {
        self.executor.reserve().await
    }
    pub fn try_reserve(&self) -> Result<QueryBlockingReservation, QueryBlockingReserveRefusal> {
        self.executor.try_reserve()
    }
    /// Installs the independent result/exit receiver before queue publication.
    /// Refusal leaves the original, reservation and receiving slot untouched.
    pub fn prepare_in(
        &self,
        reservation: &QueryBlockingReservation,
        original: &Arc<O>,
        receipt: &mut Option<QueryBlockingReceipt<()>>,
    ) -> Result<QueryBlockingPreparedJob, QueryBlockingPublishRefusal> {
        if self.executor.handle.closed_factory_descriptor().is_some() {
            return Err(QueryBlockingPublishRefusal::FactoryMismatch);
        }
        if receipt.is_some() {
            return Err(QueryBlockingPublishRefusal::Empty);
        }
        if !self.executor.handle.same_pool(&reservation.inner) {
            return Err(QueryBlockingPublishRefusal::ForeignPool);
        }
        let (prepared, original_receipt, exit) = self
            .executor
            .handle
            .prepare(BoundedWorkerJob::new(original_call(Arc::clone(original))));
        *receipt = Some(QueryBlockingReceipt {
            receipt: Some(original_receipt),
            exit,
            _result: std::marker::PhantomData,
        });
        Ok(QueryBlockingPreparedJob {
            inner: Some(prepared),
        })
    }
    /// Silent same-FIFO publication. Every refusal preserves both original
    /// slots. Notify only after the caller records acceptance outside its lock.
    /// On a normal unpublished refusal, destroy prepared before reservation;
    /// only normal return proves NeverPublishedReturned. Its unused exit
    /// header remains Pending: no worker ran and no actual exit is invented.
    pub fn publish_in(
        &self,
        reservation: &mut Option<QueryBlockingReservation>,
        prepared: &mut Option<QueryBlockingPreparedJob>,
    ) -> Result<QueryBlockingPublication, QueryBlockingPublishRefusal> {
        let original = prepared
            .as_mut()
            .ok_or(QueryBlockingPublishRefusal::Empty)?;
        let inner = self.executor.handle.publish_ordinary_in(
            &mut original.inner,
            reservation,
            original_call_type::<O>(),
        )?;
        prepared.take();
        Ok(QueryBlockingPublication { inner })
    }
}

/// The actual move-only boxed call and its one existing completion sender.
/// The caller retains the original source and actual input before preparation.
#[must_use = "Publish or dispose the same unexecuted input under its original custody"]
pub struct QueryBlockingPreparedJob {
    inner: Option<bounded_worker::BoundedWorkerPrepared<CpuResult>>,
}
impl QueryBlockingPreparedJob {
    /// Requested Box-call layout only, excluding capture pointees and result.
    pub fn call_layout(&self) -> std::alloc::Layout {
        self.inner
            .as_ref()
            .expect("unpublished original call")
            .call_layout()
    }
}
impl std::fmt::Debug for QueryBlockingPreparedJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBlockingPreparedJob")
            .finish_non_exhaustive()
    }
}
/// Same result channel, plus one independent actual-exit header. Keep this
/// stable after poll_result: business output may precede physical source exit.
#[must_use = "Result delivery is distinct from the original physical actual exit"]
pub struct QueryBlockingReceipt<T> {
    receipt: Option<bounded_worker::BoundedWorkerReceipt<CpuResult>>,
    exit: Arc<bounded_worker::BoundedWorkerActualExit>,
    _result: std::marker::PhantomData<fn() -> T>,
}
impl<T: Send + 'static> QueryBlockingReceipt<T> {
    pub fn poll_result(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<T, QueryBlockingFailure>> {
        use std::{future::Future, pin::Pin, task::Poll};
        let Some(receipt) = self.receipt.as_mut() else {
            return Poll::Ready(Err(QueryBlockingFailure::ExecutorClosed));
        };
        let result = std::task::ready!(Pin::new(&mut receipt.receiver).poll(cx));
        self.receipt.take();
        Poll::Ready(
            result
                .unwrap_or(Err(QueryBlockingFailure::ExecutorClosed))
                .and_then(|value| {
                    value
                        .downcast::<T>()
                        .map(|value| *value)
                        .map_err(|original| {
                            QueryBlockingFailure::InvalidResultType(
                                QueryBlockingFailurePayload::new(original),
                            )
                        })
                }),
        )
    }
    pub fn poll_actual_exit(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), QueryBlockingActualExitFailure>> {
        self.exit.poll(cx)
    }
    /// Same original failure carrier as the fixed worker entry, independent
    /// of any earlier successful result. No raw payload or guard is copied.
    pub fn actual_exit_failure(&self) -> Option<QueryBlockingFailure> {
        self.exit.failure()
    }
}
impl<T> std::fmt::Debug for QueryBlockingReceipt<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBlockingReceipt")
            .finish_non_exhaustive()
    }
}
/// Notification for an already stable original queue publication. Drop also
/// wakes the original workers; cancelling a caller cannot strand the row.
pub struct QueryBlockingPublication {
    inner: bounded_worker::BoundedWorkerPublication<CpuResult>,
}
impl QueryBlockingPublication {
    pub fn notify(self) {
        self.inner.notify();
    }
}

/// Known inline/requested layouts of this exact original creator. This is not
/// payment. Oneshot/platform Mutex backing, Waker and call/result pointees are
/// deliberately not inferred from these layouts.
#[derive(Clone, Copy, Debug)]
pub struct QueryBlockingQueueGeometry {
    pub reservation_inline: std::alloc::Layout,
    pub prepared_inline: std::alloc::Layout,
    pub receipt_inline: std::alloc::Layout,
    pub publication_inline: std::alloc::Layout,
    pub queue_record_inline: std::alloc::Layout,
    pub worker_entry_inline: std::alloc::Layout,
    pub retirement_failure_box: std::alloc::Layout,
    pub actual_exit_header: std::alloc::Layout,
}

impl QueryCpuExecutorOwner {
    pub fn try_new(config: QueryCpuExecutorConfig) -> Result<Self, String> {
        let pool = Arc::new(ElasticPlanningPool::try_new(config)?);
        let executor = QueryCpuExecutor {
            pool: Arc::clone(&pool),
        };
        Ok(Self { pool, executor })
    }

    pub fn executor(&self) -> QueryCpuExecutor {
        self.executor.clone()
    }

    /// Stops new CPU work and observes every accepted planning worker.
    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.pool.shutdown_until(deadline).await
    }

    /// Closes CPU admission when the role has committed to process exit.
    pub fn request_shutdown_for_process_exit(&self) {
        self.pool.request_close();
    }
}

impl QueryCpuExecutor {
    pub async fn run<T, F>(&self, work: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.pool
            .submit(move || Box::new(work()) as CpuResult)?
            .await
            .map_err(|_| "query CPU worker exited without reporting its result".to_owned())??
            .downcast::<T>()
            .map(|result| *result)
            .map_err(|_| "query CPU worker returned an invalid result type".to_owned())
    }

    /// Runs preparation work until it completes or the statement is cancelled.
    ///
    /// This does not interrupt a synchronous worker that already owns a job.
    /// It does release the awaiting statement immediately, and a job that only
    /// reaches a worker after cancellation skips its closure entirely. Callers
    /// must therefore use it only before handing off any external effect.
    pub async fn run_cancellable<T, F>(
        &self,
        cancellation: QueryCancellationView,
        work: F,
    ) -> Result<T, QueryCpuRunError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        if let Some(reason) = cancellation.reason() {
            return Err(QueryCpuRunError::Cancelled(reason));
        }
        let worker_cancellation = cancellation.clone();
        let receipt = self
            .pool
            .submit(move || Box::new((!worker_cancellation.is_cancelled()).then(work)) as CpuResult)
            .map_err(QueryCpuRunError::Executor)?;
        tokio::pin!(receipt);
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => Err(QueryCpuRunError::Cancelled(reason)),
            result = &mut receipt => match result {
                Ok(value) => value
                    .map_err(QueryCpuRunError::Executor)
                    .and_then(|value| value
                    .downcast::<Option<T>>()
                    .map(|value| *value)
                    .map_err(|_| QueryCpuRunError::Executor("query CPU worker returned an invalid result type".to_owned()))
                    .and_then(|value| value.ok_or_else(|| QueryCpuRunError::Cancelled(
                    cancellation.reason().expect("cancelled CPU work retains its reason"),
                )))),
                Err(_) => Err(QueryCpuRunError::Executor("query CPU worker exited without reporting its result".to_owned())),
            },
        }
    }
}

impl QueryBlockingExecutorOwner {
    pub fn try_new(config: QueryBlockingExecutorConfig) -> Result<Self, String> {
        let owner = BoundedWorkerOwner::try_new(BoundedWorkerConfig::new(
            config.worker_threads(),
            config.queue_capacity(),
        ))
        .map_err(|error| format!("open query blocking executor: {error}"))?;
        let executor = QueryBlockingExecutor {
            handle: owner.handle(),
        };
        Ok(Self { owner, executor })
    }

    /// Trusted first-party composition only. The one supported kind is the
    /// reviewed FA InternalResultClosedFactory, never a generic FnOnce claim
    /// of finite behavior. The original queue author fences all old raw views.
    pub fn bind_internal_result_closed_factory_for_composition<I, C, T, F>(
        &mut self,
        _factory: fn(I, C) -> F,
    ) -> Result<
        novarocks_workload_control::joint_admission::InternalResultClosedFactoryDescriptor,
        novarocks_workload_control::WorkError,
    >
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let physical_queue_bytes = bounded_worker::queue_record_layout::<CpuResult>()
            .size()
            .checked_add(QueryBlockingExecutor::preserving_call_layout::<T, F>().size())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .and_then(std::num::NonZeroU64::new)
            .ok_or(novarocks_workload_control::WorkError::ArithmeticOverflow)?;
        self.executor
            .handle
            .bind_internal_result_closed_factory_for_composition(
                preserving_call_type::<T, F>(),
                physical_queue_bytes,
            )
    }

    pub fn executor(&self) -> QueryBlockingExecutor {
        self.executor.clone()
    }

    /// Only already-joined failures are movable. Shutdown retains them on Err;
    /// the role owner must preserve or explicitly dispose this exact value.
    pub fn take_worker_failure(&mut self) -> Option<QueryBlockingWorkerFailure> {
        self.owner.take_worker_failure()
    }

    /// Stops admission and joins the same fixed worker set.
    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.owner
            .shutdown_until(deadline)
            .await
            .map_err(|error| format!("shut down query blocking executor: {error}"))
    }

    /// Only process teardown may close this executor without waiting.
    pub fn request_shutdown_for_process_exit(&self) {
        self.owner.request_close();
    }
}

fn preserving_call<T, F>(work: F) -> impl FnOnce() -> CpuResult + Send + 'static
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    move || Box::new(work()) as CpuResult
}

fn preserving_call_type<T, F>() -> std::any::TypeId
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    fn output_type<I, O: 'static>(_: fn(I) -> O) -> std::any::TypeId {
        std::any::TypeId::of::<O>()
    }
    output_type(preserving_call::<T, F>)
}

impl QueryBlockingExecutor {
    /// The real same call factory's return layout, before allocating a job,
    /// channel, observer header or constructing any F. No single-capture ABI
    /// equivalence is assumed. Capture pointees remain the original author.
    pub fn preserving_call_layout<T, F>() -> std::alloc::Layout
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        fn output_layout<I, O>(_: fn(I) -> O) -> std::alloc::Layout {
            std::alloc::Layout::new::<O>()
        }
        output_layout(preserving_call::<T, F>)
    }

    /// Nonwaiting reservation in this exact original physical queue. A full
    /// queue is ordinary backpressure, independent of logical result retention.
    pub fn try_reserve(&self) -> Result<QueryBlockingReservation, QueryBlockingReserveRefusal> {
        if self.handle.closed_factory_descriptor().is_some() {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        self.handle
            .try_reserve()
            .map(|inner| QueryBlockingReservation { inner })
    }

    /// Await space in this exact original physical queue without constructing
    /// or taking the caller's input. Dropping the future withdraws its wait.
    /// Registered finite factories require their separate bound entry point.
    pub async fn reserve(&self) -> Result<QueryBlockingReservation, QueryBlockingReserveRefusal> {
        if self.handle.closed_factory_descriptor().is_some() {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        let inner = self.handle.reserve().await?;
        if self.handle.closed_factory_descriptor().is_some() {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        Ok(QueryBlockingReservation { inner })
    }

    /// Nonwaiting original reservation, bound to both the WC composition
    /// decision and this exact physical pool allocation epoch.
    pub fn try_reserve_internal_result_closed_factory(
        &self,
        binding: &novarocks_workload_control::joint_admission::InternalCpuFactoryBinding,
    ) -> Result<QueryBlockingReservation, QueryBlockingReserveRefusal> {
        let Some(descriptor) = self.handle.closed_factory_descriptor() else {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        };
        if !binding.matches_descriptor(&descriptor) {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        self.handle
            .try_reserve()
            .map(|inner| QueryBlockingReservation { inner })
    }

    /// Await space in the original physical queue without constructing a job.
    /// The caller owns cancellation/deadline selection and retains its input.
    /// Factory identity is checked both before and after the wait.
    pub async fn reserve_internal_result_closed_factory(
        &self,
        binding: &novarocks_workload_control::joint_admission::InternalCpuFactoryBinding,
    ) -> Result<QueryBlockingReservation, QueryBlockingReserveRefusal> {
        let matches = || {
            self.handle
                .closed_factory_descriptor()
                .is_some_and(|descriptor| binding.matches_descriptor(&descriptor))
        };
        if !matches() {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        let inner = self.handle.reserve().await?;
        if !matches() {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        Ok(QueryBlockingReservation { inner })
    }

    /// Selects a named original on this ordinary executor. This is a typed
    /// call/pool check only; the role/Action must first install its stable
    /// strong receiver and validate the selected provider/purpose separately.
    pub fn original_executor<O: QueryBlockingOriginal>(
        &self,
    ) -> Result<QueryBlockingOriginalExecutor<O>, QueryBlockingReserveRefusal> {
        if self.handle.closed_factory_descriptor().is_some() {
            return Err(QueryBlockingReserveRefusal::FactoryMismatch);
        }
        Ok(QueryBlockingOriginalExecutor {
            executor: self.clone(),
            _original: std::marker::PhantomData,
        })
    }

    /// The original caller supplies an empty stable receipt slot before any
    /// queue publication. Refusing a nonempty slot or foreign accepted reservation returns the untouched F.
    /// Invoke only after original admission pays this actual call/frame/input.
    pub fn prepare_preserving_in<T, F>(
        &self,
        physical: &mut novarocks_workload_control::joint_admission::JointLocalGrant<
            QueryBlockingReservation,
        >,
        work: F,
        receipt: &mut Option<QueryBlockingReceipt<T>>,
    ) -> Result<QueryBlockingPreparedJob, QueryBlockingPrepareRefusal<F>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let factory_matches = match self.handle.closed_factory_descriptor() {
            Some(descriptor) => physical.matches_internal_result_closed_factory(&descriptor),
            None => !physical.is_internal_result_closed_factory(),
        };
        let reason = if !factory_matches
            || !self
                .handle
                .accepts_call_type(preserving_call_type::<T, F>())
        {
            Some(QueryBlockingPublishRefusal::FactoryMismatch)
        } else if receipt.is_some() {
            Some(QueryBlockingPublishRefusal::Empty)
        } else if !self.handle.same_pool(&physical.physical_mut().inner) {
            Some(QueryBlockingPublishRefusal::ForeignPool)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(QueryBlockingPrepareRefusal { reason, work });
        }
        let (prepared, original, exit) = self
            .handle
            .prepare(BoundedWorkerJob::new(preserving_call(work)));
        *receipt = Some(QueryBlockingReceipt {
            receipt: Some(original),
            exit,
            _result: std::marker::PhantomData,
        });
        Ok(QueryBlockingPreparedJob {
            inner: Some(prepared),
        })
    }

    /// Move only the same accepted grant and prepared call into the original
    /// row. All refusal paths leave both original slots populated/unexecuted.
    /// The same receipt must remain in its original stable caller through exit.
    /// ```compile_fail
    /// use novarocks_query_application::cpu::{QueryBlockingExecutor,
    ///     QueryBlockingReservation, QueryBlockingPreparedJob};
    /// fn raw_is_not_a_grant(executor: &QueryBlockingExecutor,
    ///     mut raw: Option<QueryBlockingReservation>,
    ///     mut job: Option<QueryBlockingPreparedJob>) {
    ///     executor.publish_joint_in(&mut raw, &mut job);
    /// }
    /// ```
    pub fn publish_joint_in(
        &self,
        physical: &mut Option<
            novarocks_workload_control::joint_admission::JointLocalGrant<QueryBlockingReservation>,
        >,
        prepared: &mut Option<QueryBlockingPreparedJob>,
    ) -> Result<QueryBlockingPublication, QueryBlockingPublishRefusal> {
        // Borrow the original prepared field without allocating an adapter.
        let Some(original) = prepared.as_mut() else {
            return Err(QueryBlockingPublishRefusal::Empty);
        };
        self.handle
            .publish_joint_in(&mut original.inner, physical)
            .map(|inner| {
                prepared.take();
                QueryBlockingPublication { inner }
            })
    }

    pub fn queue_geometry(
        &self,
    ) -> Result<
        QueryBlockingQueueGeometry,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        Self::queue_geometry_for_composition()
    }

    /// Pure requested layouts of the original queue types. No pool is opened,
    /// reserved, or certified finite by reading this value.
    pub fn queue_geometry_for_composition() -> Result<
        QueryBlockingQueueGeometry,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        Ok(QueryBlockingQueueGeometry {
            reservation_inline: std::alloc::Layout::new::<QueryBlockingReservation>(),
            prepared_inline: std::alloc::Layout::new::<QueryBlockingPreparedJob>(),
            receipt_inline: std::alloc::Layout::new::<QueryBlockingReceipt<()>>(),
            publication_inline: std::alloc::Layout::new::<QueryBlockingPublication>(),
            queue_record_inline: bounded_worker::queue_record_layout::<CpuResult>(),
            worker_entry_inline: bounded_worker::worker_entry_layout(),
            retirement_failure_box: std::alloc::Layout::new::<
                bounded_worker::WorkerRetirementFailure,
            >(),
            actual_exit_header: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<bounded_worker::BoundedWorkerActualExit>(),
            )?,
        })
    }

    /// Original installed physical geometry, for composition checks. Reading
    /// this declaration does not reserve a worker or a waiting queue position.
    pub fn configured_capacity(&self) -> QueryBlockingExecutorConfig {
        let snapshot = self.handle.snapshot();
        QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(snapshot.worker_threads).expect("installed worker geometry"),
            NonZeroUsize::new(snapshot.queue_capacity).expect("installed queue geometry"),
        )
    }

    /// Runs one already-classified command edge after bounded admission.
    ///
    /// This is intentionally not a generic statement runner: callers must
    /// keep parser admission, CPU compilation, protocol delivery, and every
    /// adapter-owned synchronous effect in separate closures.
    pub async fn execute<T, F>(&self, work: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        // Legacy callers retain their original String-only surface. New owners
        // which must keep opaque causes use the preserving method below.
        self.execute_preserving(work)
            .await
            .map_err(|error| match &error {
                QueryBlockingFailure::SubmissionClosed
                | QueryBlockingFailure::InvalidResultType(_) => error.to_string(),
                _ => format!("query blocking worker failed: {error}"),
            })
    }

    /// Uses the same original queue, workers and receipt. Cancellation of a
    /// waiter is not completion; preserving callers retain and await this F.
    pub async fn execute_preserving<T, F>(&self, work: F) -> Result<T, QueryBlockingFailure>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let receipt = self
            .handle
            .submit(BoundedWorkerJob::new(preserving_call(work)))
            .await
            .map_err(|refused| {
                // The original unexecuted input exits here, outside queue locks.
                drop(refused.into_job());
                QueryBlockingFailure::SubmissionClosed
            })?;
        receipt
            .complete()
            .await?
            .downcast::<T>()
            .map(|result| *result)
            .map_err(|original| {
                QueryBlockingFailure::InvalidResultType(QueryBlockingFailurePayload::new(original))
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::cancellation::{QueryCancellationReason, QueryCancellationSource};

    use super::{
        QueryBlockingExecutorConfig, QueryBlockingExecutorOwner, QueryCpuExecutorConfig,
        QueryCpuExecutorOwner, QueryCpuRunError,
    };

    fn executor(keepalive: Duration) -> QueryCpuExecutorOwner {
        QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::with_idle_keepalive(keepalive))
            .expect("open CPU executor")
    }

    #[tokio::test]
    async fn planning_worker_compiles_branching_recursive_union() {
        use novarocks_sql::compiler::{
            SessionOptimizerSettings, SqlAnalyzeRequest, SqlCatalogSnapshot, SqlCompileControl,
            SqlCompileIntent, SqlCompiler, SqlOptimizeRequest, SqlPlanningEnvironment,
            SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
        };
        use novarocks_sql::planning::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
        use novarocks_sql::planning::dml::DmlStatisticsSnapshot;

        struct EmptyCatalog;
        impl PlannerTableProvider for EmptyCatalog {
            fn resolve_table_for_analysis(
                &self,
                _: Option<&str>,
                _: &str,
                _: &str,
            ) -> Result<ResolvedAnalyzerTable, String> {
                Err("recursive sequence has no external table".into())
            }
        }
        impl SqlCatalogSnapshot for EmptyCatalog {
            fn planner_table_provider(&self) -> &dyn PlannerTableProvider {
                self
            }
        }

        let mut owner = executor(Duration::from_secs(1));
        owner
            .executor()
            .run(|| {
                let request = SqlAnalyzeRequest::new(
                    SqlStatementInput::sql(
                        "WITH RECURSIVE numbers AS (
                        SELECT CAST(1 AS BIGINT) AS n, CAST(1 AS BIGINT) AS category
                        UNION SELECT n + 1, 1 FROM numbers WHERE n < 5
                        UNION SELECT n, 2 FROM numbers
                    ) SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ DISTINCT n, category
                      FROM numbers ORDER BY n, category",
                    ),
                    SqlCompileIntent::Query,
                    SqlSessionContext {
                        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                        current_catalog: None,
                        current_database: "default".into(),
                        optimizer_settings: SessionOptimizerSettings::default(),
                    },
                    SqlPlanningEnvironment::Distributed,
                    &EmptyCatalog,
                    builtin_sql_function_catalog(),
                    novarocks_sql::compiler::noop_constant_evaluator(),
                    None,
                    crate::completed_plan_fixture::test_constant_policy(),
                    SqlCompileControl::unbounded(),
                );
                let pending = SqlCompiler::analyze(request)
                    .expect("analyze recursive UNION")
                    .into_pending()
                    .expect("recursive UNION requires optimization");
                SqlCompiler::optimize(SqlOptimizeRequest::new(
                    pending,
                    &DmlStatisticsSnapshot::empty(),
                    SqlCompileControl::unbounded(),
                ))
                .expect("optimize recursive UNION on the production planning worker");
            })
            .await
            .expect("planning worker returns without aborting the process");
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("planning worker actually exits");
    }

    #[test]
    fn shutdown_retains_a_start_until_its_real_worker_join_is_published_and_observed() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let mut owner = executor(Duration::from_secs(1));
        let pool = Arc::clone(&owner.pool);
        let main_thread = std::thread::current().id();
        std::thread::scope(|scope| {
            // Senders live INSIDE this scope closure. Assertion unwind closes
            // the receivers before scoped joins; every wait is also bounded.
            let (at_spawn, spawn_reached) = std::sync::mpsc::sync_channel(1);
            let (allow_spawn, spawn_allowed) = std::sync::mpsc::sync_channel(0);
            let (at_work, work_reached) = std::sync::mpsc::sync_channel(1);
            let (allow_exit, exit_allowed) = std::sync::mpsc::sync_channel(0);
            let (at_publication, publication_reached) = std::sync::mpsc::sync_channel(1);
            let (exited, worker_exited) = std::sync::mpsc::sync_channel(1);
            let (allow_publication, publication_allowed) = std::sync::mpsc::sync_channel(0);
            let submitter = scope.spawn(move || {
                pool.submit_with_spawn_gates(
                    move || {
                        at_work.send(()).unwrap();
                        exit_allowed.recv_timeout(Duration::from_secs(5)).unwrap();
                        Box::new(std::thread::current().id()) as super::CpuResult
                    },
                    move || {
                        at_spawn.send(()).unwrap();
                        spawn_allowed.recv_timeout(Duration::from_secs(5)).unwrap();
                    },
                    move |join| {
                        at_publication.send(()).unwrap();
                        // Observe the SAME real thread's public exit, rather
                        // than treating closure return or an empty worker map
                        // as its actual exit. It is still not joined here.
                        let deadline = Instant::now() + Duration::from_secs(5);
                        while !join.is_finished() {
                            assert!(Instant::now() < deadline, "original worker did not exit");
                            std::thread::yield_now();
                        }
                        exited.send(()).unwrap();
                        publication_allowed
                            .recv_timeout(Duration::from_secs(5))
                            .unwrap();
                    },
                )
            });
            spawn_reached.recv_timeout(Duration::from_secs(5)).unwrap();
            let before = owner.pool.snapshot();
            assert_eq!(before.unjoined_starts, 1);
            assert_eq!(before.joins, 0);
            let first =
                runtime.block_on(owner.shutdown_until(Instant::now() + Duration::from_millis(20)));
            assert!(
                first.is_err(),
                "accepted unpublished start is not a joined worker"
            );
            assert_eq!(owner.pool.snapshot().unjoined_starts, 1);

            allow_spawn.send(()).unwrap();
            publication_reached
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            work_reached.recv_timeout(Duration::from_secs(5)).unwrap();
            let running = owner.pool.snapshot();
            assert_eq!(running.unjoined_starts, 1);
            assert_eq!(running.joins, 0);
            assert_eq!(running.running, 1);
            let held =
                runtime.block_on(owner.shutdown_until(Instant::now() + Duration::from_millis(20)));
            assert!(
                held.is_err(),
                "the same actual worker is still inside its call"
            );

            allow_exit.send(()).unwrap();
            worker_exited.recv_timeout(Duration::from_secs(5)).unwrap();
            let unpublished_exit = owner.pool.snapshot();
            assert_eq!(unpublished_exit.workers, 0);
            assert_eq!(unpublished_exit.joins, 0);
            assert_eq!(unpublished_exit.unjoined_starts, 1);
            let exited_but_unjoined =
                runtime.block_on(owner.shutdown_until(Instant::now() + Duration::from_millis(20)));
            assert!(
                exited_but_unjoined.is_err(),
                "actual exit still owes its original join"
            );

            allow_publication.send(()).unwrap();
            let receipt = submitter.join().unwrap().unwrap();
            let worker_thread = runtime
                .block_on(receipt)
                .unwrap()
                .unwrap()
                .downcast::<std::thread::ThreadId>()
                .unwrap();
            assert_ne!(*worker_thread, main_thread);
            let (at_join, join_reached) = std::sync::mpsc::sync_channel(1);
            let (allow_join, join_allowed) = std::sync::mpsc::sync_channel(0);
            let joining_pool = Arc::clone(&owner.pool);
            let reaper = scope.spawn(move || {
                joining_pool.reap_finished_with_join_gate(|join| {
                    assert!(join.is_finished());
                    at_join.send(()).unwrap();
                    join_allowed.recv_timeout(Duration::from_secs(5)).unwrap();
                });
            });
            join_reached.recv_timeout(Duration::from_secs(5)).unwrap();
            let joining = owner.pool.snapshot();
            assert_eq!(joining.joins, 0);
            assert_eq!(joining.workers, 0);
            assert_eq!(joining.unjoined_starts, 1);
            let taken_but_unjoined =
                runtime.block_on(owner.shutdown_until(Instant::now() + Duration::from_millis(20)));
            assert!(
                taken_but_unjoined.is_err(),
                "the original join is still in progress"
            );
            allow_join.send(()).unwrap();
            reaper.join().unwrap();
            runtime
                .block_on(owner.shutdown_until(Instant::now() + Duration::from_secs(5)))
                .unwrap();
            let joined = owner.pool.snapshot();
            assert_eq!(joined.unjoined_starts, 0);
            assert_eq!(joined.workers, 0);
            assert_eq!(joined.joins, 0);
        });
    }

    #[tokio::test]
    async fn elastic_cpu_workers_expand_for_every_admitted_job() {
        let mut owner = executor(Duration::from_secs(1));
        let executor = owner.executor();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let mut work = Vec::new();
        for value in 0..4 {
            let gate = Arc::clone(&gate);
            let active = Arc::clone(&active);
            let executor = executor.clone();
            work.push(tokio::spawn(async move {
                executor
                    .run(move || {
                        active.fetch_add(1, Ordering::SeqCst);
                        let (lock, ready) = &*gate;
                        let mut open = lock.lock().expect("gate lock");
                        while !*open {
                            open = ready.wait(open).expect("gate wait");
                        }
                        value
                    })
                    .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all admitted planning jobs started without a fixed queue");
        assert_eq!(executor.pool.snapshot().running, 4);

        let (lock, ready) = &*gate;
        *lock.lock().expect("gate lock") = true;
        ready.notify_all();
        for (expected, work) in work.into_iter().enumerate() {
            assert_eq!(work.await.expect("join").expect("run"), expected);
        }
        drop(executor);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down CPU workers");
    }

    #[tokio::test]
    async fn elastic_cpu_worker_reuses_idle_thread_and_reclaims_it_after_keepalive() {
        let mut owner = executor(Duration::from_millis(20));
        let executor = owner.executor();
        let first = executor.run(|| std::thread::current().id()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.pool.snapshot().idle != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first worker did not become idle");
        let second = executor.run(|| std::thread::current().id()).await.unwrap();
        assert_eq!(first, second);
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.pool.snapshot().workers != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("idle worker was not reclaimed");
        drop(executor);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down reclaimed CPU workers");
    }

    #[tokio::test]
    async fn panicked_planning_worker_reports_failure_then_retires() {
        let mut owner = executor(Duration::from_secs(1));
        let executor = owner.executor();
        let error = executor
            .run::<(), _>(|| panic!("planning test panic"))
            .await
            .expect_err("panic must reach the submitting query");
        assert!(error.contains("panicked"));
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.pool.snapshot().workers != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicked worker did not retire");
        assert_eq!(executor.run(|| 7_u8).await, Ok(7));
        drop(executor);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down replacement CPU worker");
    }

    #[tokio::test]
    async fn cancellation_releases_the_waiter_without_claiming_running_work_stopped() {
        let mut owner = executor(Duration::from_secs(1));
        let executor = owner.executor();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let cancellation = QueryCancellationSource::new();
        let ran = Arc::new(AtomicUsize::new(0));
        let running = tokio::spawn({
            let executor = executor.clone();
            let gate = Arc::clone(&gate);
            let cancellation = cancellation.view();
            let ran = Arc::clone(&ran);
            async move {
                executor
                    .run_cancellable(cancellation, move || {
                        ran.fetch_add(1, Ordering::SeqCst);
                        let (lock, ready) = &*gate;
                        let mut open = lock.lock().expect("gate lock");
                        while !*open {
                            open = ready.wait(open).expect("gate wait");
                        }
                    })
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while ran.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("planning closure did not start");
        assert_eq!(
            cancellation.request(QueryCancellationReason::ClientDisconnected),
            crate::cancellation::QueryCancellationRequestResult::Requested
        );
        assert_eq!(
            running.await.expect("planning task"),
            Err(QueryCpuRunError::Cancelled(
                QueryCancellationReason::ClientDisconnected
            ))
        );
        assert_eq!(executor.pool.snapshot().running, 1);

        let (lock, ready) = &*gate;
        *lock.lock().expect("gate lock") = true;
        ready.notify_all();
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.pool.snapshot().running != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual planning work did not exit");
        drop(executor);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down CPU workers");
    }

    #[tokio::test]
    async fn fixed_blocking_workers_bound_concurrent_work() {
        let owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).expect("nonzero workers"),
            NonZeroUsize::new(2).expect("nonzero queue"),
        ))
        .expect("open blocking executor");
        let executor = owner.executor();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut work = Vec::new();
        for value in 0..2 {
            let gate = Arc::clone(&gate);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let executor = executor.clone();
            work.push(tokio::spawn(async move {
                executor
                    .execute(move || {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        let (lock, ready) = &*gate;
                        let mut open = lock.lock().expect("gate lock");
                        while !*open {
                            open = ready.wait(open).expect("gate wait");
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        value
                    })
                    .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one fixed blocking worker started");
        let (lock, ready) = &*gate;
        *lock.lock().expect("gate lock") = true;
        ready.notify_all();
        for (expected, work) in work.into_iter().enumerate() {
            assert_eq!(work.await.expect("join").expect("run"), expected);
        }
        assert_eq!(peak.load(Ordering::SeqCst), 1);
        drop(executor);
        let mut owner = owner;
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down blocking workers");
    }
}

#[cfg(test)]
mod joint_queue_tests {
    use super::*;
    use novarocks_resource_contract::SharedDependency;
    use novarocks_workload_control::joint_admission::*;
    use novarocks_workload_control::{
        ControlStorageConfig, WorkClass, WorkOwner, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::{
        future::{Future, poll_fn},
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };

    #[tokio::test(flavor = "current_thread")]
    async fn closed_factory_queue_wait_wakes_on_original_capacity_and_withdraws_on_cancel() {
        struct QueueWake(AtomicBool);
        impl Wake for QueueWake {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let mut owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .unwrap();
        let descriptor = owner
            .bind_internal_result_closed_factory_for_composition(fixture_closed_work)
            .unwrap();
        let executor = owner.executor();
        assert!(matches!(
            executor.reserve().await,
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));
        let mut control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let binding = control
            .bind_internal_result_closed_factory_for_composition(descriptor)
            .unwrap();
        let held = executor
            .try_reserve_internal_result_closed_factory(&binding)
            .unwrap();
        let wake = Arc::new(QueueWake(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);
        let mut cancelled = Box::pin(executor.reserve_internal_result_closed_factory(&binding));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);
        let mut waiting = Box::pin(executor.reserve_internal_result_closed_factory(&binding));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert_eq!(executor.handle.snapshot().queued, 0);
        assert_eq!(executor.handle.snapshot().running, 0);
        drop(held);
        assert!(wake.0.load(Ordering::SeqCst));
        let Poll::Ready(Ok(acquired)) = waiting.as_mut().poll(&mut cx) else {
            panic!("original queue capacity must wake its surviving waiter")
        };
        drop(waiting);
        drop(acquired);

        let held = executor
            .try_reserve_internal_result_closed_factory(&binding)
            .unwrap();
        let mut waiting = Box::pin(executor.reserve_internal_result_closed_factory(&binding));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        owner.request_shutdown_for_process_exit();
        assert!(matches!(
            waiting.as_mut().poll(&mut cx),
            Poll::Ready(Err(QueryBlockingReserveRefusal::Closed))
        ));
        drop(waiting);
        drop(held);
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_queue_wait_preserves_input_and_withdraws_before_original_capacity_reuse() {
        struct QueueWake(AtomicBool);
        impl Wake for QueueWake {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let (mut owner, executor) = worker();
        let mut held: Vec<_> = (0..3).map(|_| executor.try_reserve().unwrap()).collect();
        assert!(matches!(
            executor.try_reserve(),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        let drops = Arc::new(AtomicUsize::new(0));
        let input = Some(Box::new(DropCount(Arc::clone(&drops))));
        let input_pointer = input.as_deref().unwrap() as *const DropCount;
        let wake = Arc::new(QueueWake(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);
        let mut cancelled = Box::pin(executor.reserve());
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);
        drop(held.pop().unwrap());
        // A cancelled semaphore waiter must not consume newly available space.
        held.push(executor.try_reserve().unwrap());
        wake.0.store(false, Ordering::SeqCst);
        let mut waiting = Box::pin(executor.reserve());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert!(!wake.0.load(Ordering::SeqCst));
        assert_eq!(input.as_deref().unwrap() as *const DropCount, input_pointer);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(executor.handle.snapshot().queued, 0);
        assert_eq!(executor.handle.snapshot().running, 0);
        drop(held.pop().unwrap());
        assert!(wake.0.load(Ordering::SeqCst));
        let Poll::Ready(Ok(acquired)) = waiting.as_mut().poll(&mut cx) else {
            panic!("the same ordinary waiter must acquire the original released slot")
        };
        drop(waiting);
        assert!(matches!(
            executor.try_reserve(),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        assert_eq!(input.as_deref().unwrap() as *const DropCount, input_pointer);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(executor.handle.snapshot().queued, 0);
        assert_eq!(executor.handle.snapshot().running, 0);
        drop(acquired);
        drop(held);
        drop(input);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_queue_close_wakes_pending_reservation_without_creating_work() {
        struct QueueWake(AtomicBool);
        impl Wake for QueueWake {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let (mut owner, executor) = worker();
        let held: Vec<_> = (0..3).map(|_| executor.try_reserve().unwrap()).collect();
        let wake = Arc::new(QueueWake(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);
        let mut waiting = Box::pin(executor.reserve());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert!(!wake.0.load(Ordering::SeqCst));
        owner.request_shutdown_for_process_exit();
        assert!(wake.0.load(Ordering::SeqCst));
        assert!(matches!(
            waiting.as_mut().poll(&mut cx),
            Poll::Ready(Err(QueryBlockingReserveRefusal::Closed))
        ));
        assert_eq!(executor.handle.snapshot().queued, 0);
        assert_eq!(executor.handle.snapshot().running, 0);
        drop(waiting);
        drop(held);
        shutdown(&mut owner).await;
    }

    struct OrdinaryOriginal {
        input: String,
        address: usize,
        runs: AtomicUsize,
        output: Mutex<Option<usize>>,
    }
    impl OrdinaryOriginal {
        fn new() -> Arc<Self> {
            let input = String::from("same private opening input");
            let address = input.as_ptr() as usize;
            Arc::new(Self {
                input,
                address,
                runs: AtomicUsize::new(0),
                output: Mutex::new(None),
            })
        }
    }
    impl QueryBlockingOriginal for OrdinaryOriginal {
        fn run_original(&self) {
            assert_eq!(self.input.as_ptr() as usize, self.address);
            assert_eq!(self.runs.fetch_add(1, Ordering::SeqCst), 0);
            *self.output.lock().unwrap() = Some(self.input.len());
        }
    }
    struct OtherOrdinaryOriginal;
    impl QueryBlockingOriginal for OtherOrdinaryOriginal {
        fn run_original(&self) {
            panic!("foreign purpose must not execute");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_full_wait_cancels_without_moving_input_then_same_queue_runs_once() {
        let (mut owner, executor) = worker();
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        let original = OrdinaryOriginal::new(); // stable original receiver, outside each waiting F
        let pointer = Arc::as_ptr(&original);
        let mut held: Vec<_> = (0..3).map(|_| executor.try_reserve().unwrap()).collect();
        let mut waiting = Box::pin(port.reserve());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(waiting); // withdraw only this queue waiter, not the original input
        assert_eq!(Arc::as_ptr(&original), pointer);
        assert_eq!(original.runs.load(Ordering::SeqCst), 0);
        drop(held.pop());
        let mut reservation = Some(port.reserve().await.unwrap());
        assert!(matches!(
            executor.try_reserve(),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        let mut receipt = None;
        let mut prepared = Some(
            port.prepare_in(reservation.as_ref().unwrap(), &original, &mut receipt)
                .unwrap(),
        );
        assert!(receipt.is_some());
        assert_eq!(executor.handle.snapshot().queued, 0);
        assert!(original.output.lock().unwrap().is_none());
        port.publish_in(&mut reservation, &mut prepared)
            .unwrap()
            .notify();
        assert!(reservation.is_none() && prepared.is_none());
        let mut receipt = receipt.unwrap();
        result(&mut receipt).await.unwrap();
        exited(&receipt).await.unwrap();
        assert_eq!(original.runs.load(Ordering::SeqCst), 1);
        assert_eq!(*original.output.lock().unwrap(), Some(original.input.len()));
        assert_eq!(Arc::as_ptr(&original), pointer);
        drop(held);
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_wait_uses_callers_absolute_deadline_without_publication() {
        let (mut owner, executor) = worker();
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        let original = OrdinaryOriginal::new();
        let held: Vec<_> = (0..3).map(|_| port.try_reserve().unwrap()).collect();
        let deadline = Instant::now() + Duration::from_millis(20);
        assert!(
            tokio::time::timeout_at(deadline.into(), port.reserve())
                .await
                .is_err()
        );
        assert_eq!(original.runs.load(Ordering::SeqCst), 0);
        assert!(original.output.lock().unwrap().is_none());
        assert_eq!(executor.handle.snapshot().queued, 0);
        drop(held);
        // The elapsed caller's wait did not retain a queue position.
        drop(port.try_reserve().unwrap());
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_stable_result_precedes_actual_exit_and_shutdown_keeps_same_worker() {
        let (mut owner, executor) = worker();
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        let original = OrdinaryOriginal::new();
        let mut reservation = Some(port.try_reserve().unwrap());
        let mut receipt = None;
        let mut prepared = Some(
            port.prepare_in(reservation.as_ref().unwrap(), &original, &mut receipt)
                .unwrap(),
        );
        let mut receipt = receipt.unwrap();
        let (entered, did_enter) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let wake = Waker::from(Arc::new(PausedWake {
            entered,
            release: Mutex::new(released),
            once: AtomicBool::new(false),
        }));
        assert!(
            receipt
                .poll_result(&mut Context::from_waker(&wake))
                .is_pending()
        );
        port.publish_in(&mut reservation, &mut prepared)
            .unwrap()
            .notify();
        receive(&did_enter).await;
        assert_eq!(*original.output.lock().unwrap(), Some(original.input.len()));
        result(&mut receipt).await.unwrap();
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(executor.handle.snapshot().running, 1);
        // Pop has returned the original queue position, independently of exit.
        let held: Vec<_> = (0..3).map(|_| executor.try_reserve().unwrap()).collect();
        assert!(
            owner
                .shutdown_until(Instant::now() + Duration::from_millis(20))
                .await
                .is_err()
        );
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(original.runs.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        exited(&receipt).await.unwrap();
        drop(held);
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_foreign_pool_purpose_and_unpublished_close_preserve_receivers() {
        let (mut owner, executor) = worker();
        let (mut foreign_owner, foreign) = worker();
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        let wrong = executor
            .original_executor::<OtherOrdinaryOriginal>()
            .unwrap();
        let foreign = foreign.original_executor::<OrdinaryOriginal>().unwrap();
        let original = OrdinaryOriginal::new();
        let mut reservation = Some(port.try_reserve().unwrap());
        let mut receipt = None;
        assert_eq!(
            foreign
                .prepare_in(reservation.as_ref().unwrap(), &original, &mut receipt)
                .err(),
            Some(QueryBlockingPublishRefusal::ForeignPool)
        );
        assert!(receipt.is_none());
        let mut prepared = Some(
            port.prepare_in(reservation.as_ref().unwrap(), &original, &mut receipt)
                .unwrap(),
        );
        assert_eq!(
            foreign.publish_in(&mut reservation, &mut prepared).err(),
            Some(QueryBlockingPublishRefusal::ForeignPool)
        );
        assert_eq!(
            wrong.publish_in(&mut reservation, &mut prepared).err(),
            Some(QueryBlockingPublishRefusal::FactoryMismatch)
        );
        assert!(reservation.is_some() && prepared.is_some() && receipt.is_some());
        owner.request_shutdown_for_process_exit();
        assert_eq!(
            port.publish_in(&mut reservation, &mut prepared).err(),
            Some(QueryBlockingPublishRefusal::Closed)
        );
        assert!(reservation.is_some() && prepared.is_some());
        drop(prepared.take()); // normal never-published return, not worker actual exit
        drop(reservation.take());
        assert_eq!(original.runs.load(Ordering::SeqCst), 0);
        assert!(
            receipt
                .as_ref()
                .unwrap()
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        shutdown(&mut owner).await;
        shutdown(&mut foreign_owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_accepted_before_close_is_destroyed_on_same_worker_without_running() {
        let (mut owner, executor) = worker();
        let parked = park_all_legacy_workers(&executor).await;
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        let original = OrdinaryOriginal::new();
        let mut reservation = Some(port.try_reserve().unwrap());
        let mut receipt = None;
        let mut prepared = Some(
            port.prepare_in(reservation.as_ref().unwrap(), &original, &mut receipt)
                .unwrap(),
        );
        port.publish_in(&mut reservation, &mut prepared)
            .unwrap()
            .notify();
        assert_eq!(executor.handle.snapshot().queued, 1);
        owner.request_shutdown_for_process_exit();
        let mut receipt = receipt.unwrap();
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        for release in parked {
            release.send(()).unwrap();
        }
        assert!(matches!(
            result(&mut receipt).await,
            Err(QueryBlockingFailure::ExecutorClosed)
        ));
        exited(&receipt).await.unwrap();
        assert_eq!(original.runs.load(Ordering::SeqCst), 0);
        assert!(original.output.lock().unwrap().is_none());
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_view_cannot_borrow_later_finite_factory_registration() {
        let (mut owner, executor) = worker();
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        owner
            .bind_internal_result_closed_factory_for_composition(fixture_closed_work)
            .unwrap();
        assert!(matches!(
            port.try_reserve(),
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));
        assert!(matches!(
            port.reserve().await,
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));
        assert!(matches!(
            executor.original_executor::<OrdinaryOriginal>(),
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));
        shutdown(&mut owner).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_original_notification_panic_preserves_raw_cause_and_unproven_exit() {
        let (mut owner, executor) = worker();
        let parked = park_all_legacy_workers(&executor).await;
        let port = executor.original_executor::<OrdinaryOriginal>().unwrap();
        let original = OrdinaryOriginal::new();
        let mut reservation = Some(port.try_reserve().unwrap());
        let mut receipt = None;
        let mut prepared = Some(
            port.prepare_in(reservation.as_ref().unwrap(), &original, &mut receipt)
                .unwrap(),
        );
        let mut receipt = receipt.unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let wake = Waker::from(Arc::new(PanicOnceWake {
            armed: AtomicBool::new(true),
            dropped: Arc::clone(&dropped),
        }));
        assert!(
            receipt
                .poll_result(&mut Context::from_waker(&wake))
                .is_pending()
        );
        port.publish_in(&mut reservation, &mut prepared)
            .unwrap()
            .notify();
        for release in parked {
            release.send(()).unwrap();
        }
        assert_eq!(
            exited(&receipt).await,
            Err(QueryBlockingActualExitFailure::FailedUnproven)
        );
        result(&mut receipt).await.unwrap(); // stable business result does not erase notification failure
        let cause = receipt.actual_exit_failure().unwrap();
        assert!(cause.original_payload_is::<PanicTag>());
        assert!(
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
        let failure = owner.take_worker_failure().unwrap();
        assert!(failure.cause().unwrap().shares_original_with(&cause));
        assert_eq!(original.runs.load(Ordering::SeqCst), 1);
        drop(failure);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(receipt);
        drop(cause);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    fn joint_fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
        let fixture = joint_fixture_before_ready();
        fixture.0.mark_ready().unwrap();
        fixture
    }
    fn joint_fixture_before_ready() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver)
    {
        let count = 8;
        let storage = ControlStorageConfig::new(count, &[]);
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (handle, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        (control, handle, driver)
    }

    fn drive(driver: &mut JointProgressDriver) {
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("original proof driver failed to reach idle");
    }
    fn root(
        control: &WorkloadControl,
        driver: &mut JointProgressDriver,
    ) -> novarocks_workload_control::RootWork {
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let mut proposal = scope.begin_control_binding().unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive(driver);
        let Poll::Ready(Ok(mut batch)) =
            Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("control FirstBind");
        };
        batch
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop(batch);
        drive(driver);
        root
    }
    fn request(handle: &JointAdmissionHandle, owner: &WorkOwner) -> JointAdmission {
        let b = bounded_worker::queue_record_layout::<CpuResult>().size() as u64;
        let mut proposal = handle.begin_paid_proposal(owner).unwrap();
        proposal
            .begin_new(
                JointLane::Foreground,
                JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(JointResource::BlockingJobs, 1)
                    .with(JointResource::PhysicalQueueRecords, 1)
                    .with(JointResource::PhysicalQueueBytes, b),
            )
            .unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        proposal.submit().unwrap()
    }
    fn accept(
        executor: &QueryBlockingExecutor,
        request: &mut JointAdmission,
        driver: &mut JointProgressDriver,
    ) -> JointLocalGrant<QueryBlockingReservation> {
        drive(driver);
        let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("expected original finite offer"),
        };
        let mut original = Some(executor.try_reserve().unwrap());
        let mut stable = None;
        offer
            .accept_in(&mut original, &mut stable)
            .unwrap()
            .notify();
        stable.unwrap()
    }
    fn worker() -> (QueryBlockingExecutorOwner, QueryBlockingExecutor) {
        let owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::new(3).unwrap(),
        ))
        .unwrap();
        let executor = owner.executor();
        (owner, executor)
    }
    async fn result<T: Send + 'static>(
        receipt: &mut QueryBlockingReceipt<T>,
    ) -> Result<T, QueryBlockingFailure> {
        tokio::time::timeout(
            Duration::from_secs(5),
            poll_fn(|cx| receipt.poll_result(cx)),
        )
        .await
        .unwrap()
    }
    async fn exited<T: Send + 'static>(
        receipt: &QueryBlockingReceipt<T>,
    ) -> Result<(), QueryBlockingActualExitFailure> {
        tokio::time::timeout(
            Duration::from_secs(5),
            poll_fn(|cx| receipt.poll_actual_exit(cx)),
        )
        .await
        .unwrap()
    }
    async fn receive<T>(receiver: &std::sync::mpsc::Receiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match receiver.try_recv() {
                    Ok(value) => return value,
                    Err(std::sync::mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("observation channel closed: {error}"),
                }
            }
        })
        .await
        .unwrap()
    }
    async fn shutdown(owner: &mut QueryBlockingExecutorOwner) {
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ungranted_reservations_execute_nothing_and_foreign_publish_preserves_originals() {
        let (control, handle, mut driver) = joint_fixture();
        let root = root(&control, &mut driver);
        let (mut a_owner, a) = worker();
        let (mut b_owner, b) = worker();
        let reservation = a.try_reserve().unwrap();
        assert_eq!(a.handle.snapshot().queued, 0);
        drop(reservation);
        let mut request = request(&handle, &root.owner);
        let mut physical = Some(accept(&a, &mut request, &mut driver));
        let runs = Arc::new(AtomicUsize::new(0));
        let original_runs = Arc::clone(&runs);
        let mut receipt = None;
        let text = String::from("original move-only input");
        let address = text.as_ptr() as usize;
        let work = move || {
            assert_eq!(text.as_ptr() as usize, address);
            assert_eq!(text, "original move-only input");
            original_runs.fetch_add(1, Ordering::SeqCst);
            17usize
        };
        fn quote<T: Send + 'static, F: FnOnce() -> T + Send + 'static>(
            _: &F,
        ) -> std::alloc::Layout {
            QueryBlockingExecutor::preserving_call_layout::<T, F>()
        }
        let quoted = quote(&work);
        let mut prepared = Some(
            a.prepare_preserving_in(physical.as_mut().unwrap(), work, &mut receipt)
                .unwrap(),
        );
        assert_eq!(prepared.as_ref().unwrap().call_layout(), quoted);
        assert_eq!(
            b.publish_joint_in(&mut physical, &mut prepared).err(),
            Some(QueryBlockingPublishRefusal::ForeignPool)
        );
        assert!(physical.is_some() && prepared.is_some());
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        a.publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        assert!(physical.is_none() && prepared.is_none());
        let mut receipt = receipt.unwrap();
        assert_eq!(result(&mut receipt).await.unwrap(), 17);
        exited(&receipt).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        shutdown(&mut a_owner).await;
        shutdown(&mut b_owner).await;
    }

    struct PausedWake {
        entered: std::sync::mpsc::SyncSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        once: AtomicBool,
    }
    impl Wake for PausedWake {
        fn wake(self: Arc<Self>) {
            if !self.once.swap(true, Ordering::SeqCst) {
                let _ = self.entered.send(());
                // Bounded even if the controller assertion unwinds.
                let _ = self
                    .release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5));
            }
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn early_result_does_not_release_running_physical_or_publish_actual_exit() {
        let (control, handle, mut driver) = joint_fixture();
        let first = root(&control, &mut driver);
        let second = root(&control, &mut driver);
        let third = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let mut a = request(&handle, &first.owner);
        let mut physical = Some(accept(&executor, &mut a, &mut driver));
        let mut b = request(&handle, &second.owner);
        let held = accept(&executor, &mut b, &mut driver);
        let mut blocked = request(&handle, &third.owner);
        drive(&mut driver);
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(physical.as_mut().unwrap(), || 23usize, &mut receipt)
                .unwrap(),
        );
        let mut receipt = receipt.unwrap();
        let (entered, did_enter) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let wake = Waker::from(Arc::new(PausedWake {
            entered,
            release: Mutex::new(released),
            once: AtomicBool::new(false),
        }));
        assert!(
            receipt
                .poll_result(&mut Context::from_waker(&wake))
                .is_pending()
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        receive(&did_enter).await;
        assert!(matches!(
            receipt.poll_result(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(23))
        ));
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(executor.handle.snapshot().running, 1);
        drive(&mut driver);
        let mut offer = match blocked.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("responsibility registration does not impose a running-job quota"),
        };
        let mut no_physical: Option<QueryBlockingReservation> = None;
        offer
            .reobserve_after_drop(&mut no_physical)
            .unwrap()
            .notify();
        drop(offer);
        // A different request's withdrawal cannot end the original running
        // call or manufacture the actual-exit observer's completion.
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(blocked);
        release.send(()).unwrap();
        exited(&receipt).await.unwrap();
        let mut blocked = request(&handle, &third.owner);
        drive(&mut driver);
        let mut offer = match blocked.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("a new valid request remains observable after original actual exit"),
        };
        let mut no_physical: Option<QueryBlockingReservation> = None;
        offer
            .reobserve_after_drop(&mut no_physical)
            .unwrap()
            .notify();
        drop(offer);
        drop(blocked);
        drop(held);
        shutdown(&mut owner).await;
    }

    struct PanicTag(Arc<AtomicUsize>);
    impl Drop for PanicTag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn caught_call_panic_returns_physical_but_keeps_the_original_payload() {
        let (control, handle, mut driver) = joint_fixture();
        let first = root(&control, &mut driver);
        let second = root(&control, &mut driver);
        let third = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let mut held_request = request(&handle, &second.owner);
        let held = accept(&executor, &mut held_request, &mut driver);
        let mut pending = request(&handle, &first.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let dropped = Arc::new(AtomicUsize::new(0));
        let raw = Arc::clone(&dropped);
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || -> usize {
                        std::panic::panic_any(PanicTag(raw));
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        let mut receipt = receipt.unwrap();
        let cause = result(&mut receipt).await.unwrap_err();
        assert!(cause.original_payload_is::<PanicTag>());
        exited(&receipt).await.unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        let mut next = request(&handle, &third.owner);
        drive(&mut driver);
        let mut next_offer = match next.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("CPU returned while same panic payload remains alive"),
        };
        let mut no_physical: Option<QueryBlockingReservation> = None;
        next_offer
            .reobserve_after_drop(&mut no_physical)
            .unwrap()
            .notify();
        drop(next_offer);
        drop(next);
        drop(held);
        let reservations = [
            executor.try_reserve().unwrap(),
            executor.try_reserve().unwrap(),
            executor.try_reserve().unwrap(),
        ];
        assert_eq!(
            executor.try_reserve().err(),
            Some(QueryBlockingReserveRefusal::Full)
        );
        drop(reservations);
        shutdown(&mut owner).await;
        assert!(owner.take_worker_failure().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(cause);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    struct DropGate {
        started: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
        finished: Arc<AtomicBool>,
    }
    impl Drop for DropGate {
        fn drop(&mut self) {
            let _ = self.started.send(());
            let _ = self.release.recv_timeout(Duration::from_secs(5));
            self.finished.store(true, Ordering::SeqCst);
        }
    }
    async fn park_all_legacy_workers(
        executor: &QueryBlockingExecutor,
    ) -> Vec<std::sync::mpsc::SyncSender<()>> {
        let mut releases = Vec::new();
        for _ in 0..3 {
            let (start, started) = std::sync::mpsc::sync_channel(1);
            let (release, released) = std::sync::mpsc::sync_channel(1);
            let receipt = executor
                .handle
                .try_submit(BoundedWorkerJob::new(move || {
                    let _ = start.send(());
                    let _ = released.recv_timeout(Duration::from_secs(5));
                    Box::new(()) as CpuResult
                }))
                .unwrap();
            receive(&started).await;
            drop(receipt);
            releases.push(release);
        }
        releases
    }
    #[tokio::test(flavor = "current_thread")]
    async fn close_destroys_the_unexecuted_joint_input_before_physical_exit() {
        let (control, handle, mut driver) = joint_fixture();
        let root = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        // Legacy blockers only force this interoperability close path. They
        // are deliberately not part of a host-finite/profile qualification.
        let parked = park_all_legacy_workers(&executor).await;
        let mut pending = request(&handle, &root.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let (start, started) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let input = DropGate {
            started: start,
            release: released,
            finished: Arc::clone(&finished),
        };
        let runs = Arc::new(AtomicUsize::new(0));
        let actual_runs = Arc::clone(&runs);
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || {
                        actual_runs.fetch_add(1, Ordering::SeqCst);
                        drop(input);
                        29usize
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        owner.request_shutdown_for_process_exit();
        for release in parked {
            let _ = release.send(());
        }
        receive(&started).await;
        let mut receipt = receipt.unwrap();
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(!finished.load(Ordering::SeqCst));
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        assert!(matches!(
            result(&mut receipt).await,
            Err(QueryBlockingFailure::ExecutorClosed)
        ));
        exited(&receipt).await.unwrap();
        assert!(finished.load(Ordering::SeqCst));
        shutdown(&mut owner).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    struct PanicOnDrop(Arc<AtomicUsize>);
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            std::panic::panic_any(PanicTag(Arc::clone(&self.0)));
        }
    }
    #[tokio::test]
    async fn cancelled_input_drop_panic_is_unproven_and_retained_by_the_original_worker_entry() {
        let (control, handle, mut driver) = joint_fixture();
        let root = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let parked = park_all_legacy_workers(&executor).await;
        let mut pending = request(&handle, &root.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let dropped = Arc::new(AtomicUsize::new(0));
        let input = PanicOnDrop(Arc::clone(&dropped));
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || {
                        drop(input);
                        31usize
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        owner.request_shutdown_for_process_exit();
        for release in parked {
            let _ = release.send(());
        }
        let mut receipt = receipt.unwrap();
        let cause = result(&mut receipt).await.unwrap_err();
        assert_eq!(
            exited(&receipt).await,
            Err(QueryBlockingActualExitFailure::FailedUnproven)
        );
        assert!(cause.original_payload_is::<PanicTag>());
        assert!(
            receipt
                .actual_exit_failure()
                .unwrap()
                .shares_original_with(&cause)
        );
        assert!(
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
        let failure = owner
            .take_worker_failure()
            .expect("same fixed joined entry");
        assert!(failure.cause().unwrap().shares_original_with(&cause));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        // Actual disposition of the opaque entry is not an observer success.
        drop(failure);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(cause);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(receipt);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn undelivered_original_panic_drop_keeps_the_actual_successor_in_the_worker_entry() {
        struct Successor(Arc<AtomicUsize>);
        impl Drop for Successor {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Original {
            dropped: Arc<AtomicUsize>,
            successor: Option<Successor>,
        }
        impl Drop for Original {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                let successor = self.successor.take().unwrap();
                if std::thread::panicking() {
                    // A failed prerequisite must not abort the test binary.
                    drop(successor);
                } else {
                    std::panic::panic_any(successor);
                }
            }
        }
        let (control, handle, mut driver) = joint_fixture();
        let root = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let mut pending = request(&handle, &root.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let original_drops = Arc::new(AtomicUsize::new(0));
        let successor_drops = Arc::new(AtomicUsize::new(0));
        let input = Original {
            dropped: Arc::clone(&original_drops),
            successor: Some(Successor(Arc::clone(&successor_drops))),
        };
        let (started, did_start) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || -> usize {
                        let _ = started.send(());
                        let _ = released.recv_timeout(Duration::from_secs(5));
                        std::panic::panic_any(input);
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        receive(&did_start).await;
        let mut receipt = receipt.unwrap();
        // Withdraw this actual result receiver before the original call returns.
        // The same worker must destroy the undelivered original panic payload.
        drop(receipt.receipt.take());
        release.send(()).unwrap();
        assert_eq!(
            exited(&receipt).await,
            Err(QueryBlockingActualExitFailure::FailedUnproven)
        );
        let cause = receipt.actual_exit_failure().unwrap();
        assert!(cause.original_payload_is::<Successor>());
        assert_eq!(original_drops.load(Ordering::SeqCst), 1);
        assert_eq!(successor_drops.load(Ordering::SeqCst), 0);
        assert!(
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
        let failure = owner.take_worker_failure().unwrap();
        assert!(failure.cause().unwrap().shares_original_with(&cause));
        let alias = cause.clone();
        // Explicit failed-entry disposition and physical guard release are
        // separate from the last raw payload alias. Neither reports source success.
        drop(failure);
        drop(cause);
        drop(receipt);
        assert_eq!(successor_drops.load(Ordering::SeqCst), 0);
        drop(alias);
        assert_eq!(successor_drops.load(Ordering::SeqCst), 1);
    }

    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn close_between_reserve_and_publish_returns_both_original_slots_without_execution() {
        let (control, handle, mut driver) = joint_fixture();
        let root = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let mut pending = request(&handle, &root.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let dropped = Arc::new(AtomicUsize::new(0));
        let input = DropCount(Arc::clone(&dropped));
        let runs = Arc::new(AtomicUsize::new(0));
        let original_runs = Arc::clone(&runs);
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || {
                        original_runs.fetch_add(1, Ordering::SeqCst);
                        drop(input);
                        37usize
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        owner.request_shutdown_for_process_exit();
        assert_eq!(
            executor
                .publish_joint_in(&mut physical, &mut prepared)
                .err(),
            Some(QueryBlockingPublishRefusal::Closed)
        );
        assert!(physical.is_some() && prepared.is_some());
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        let mut receipt = receipt.unwrap();
        assert!(
            receipt
                .poll_result(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        // The source never entered the row. Explicit original-caller cleanup,
        // not the receipt observer, retires these unpublished values in order.
        drop(prepared.take());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        drop(physical.take());
        shutdown(&mut owner).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert!(
            receipt
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn undelivered_output_drop_finishes_before_guard_refund_and_actual_exit() {
        let (control, handle, mut driver) = joint_fixture();
        let first = root(&control, &mut driver);
        let second = root(&control, &mut driver);
        let third = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let mut held_request = request(&handle, &second.owner);
        let held = accept(&executor, &mut held_request, &mut driver);
        let mut pending = request(&handle, &first.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let (started, did_start) = std::sync::mpsc::sync_channel(1);
        let (release_work, released_work) = std::sync::mpsc::sync_channel(1);
        let (dropping, did_drop) = std::sync::mpsc::sync_channel(1);
        let (release_drop, released_drop) = std::sync::mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let output = DropGate {
            started: dropping,
            release: released_drop,
            finished: Arc::clone(&finished),
        };
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || {
                        let _ = started.send(());
                        let _ = released_work.recv_timeout(Duration::from_secs(5));
                        output
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        receive(&did_start).await;
        let receipt = receipt.unwrap();
        // A test-only alias of this same header observes the receipt-drop
        // negative path; production callers keep their original opaque receipt.
        let exit = Arc::clone(&receipt.exit);
        drop(receipt);
        release_work.send(()).unwrap();
        receive(&did_drop).await;
        assert!(!finished.load(Ordering::SeqCst));
        assert!(
            exit.poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(executor.handle.snapshot().running, 1);
        let mut blocked = request(&handle, &third.owner);
        drive(&mut driver);
        let mut offer = match blocked.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("responsibility registration does not impose a running-job quota"),
        };
        let mut no_physical: Option<QueryBlockingReservation> = None;
        offer
            .reobserve_after_drop(&mut no_physical)
            .unwrap()
            .notify();
        drop(offer);
        drop(blocked);
        release_drop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), poll_fn(|cx| exit.poll(cx)))
            .await
            .unwrap()
            .unwrap();
        assert!(finished.load(Ordering::SeqCst));
        let mut blocked = request(&handle, &third.owner);
        drive(&mut driver);
        let mut offer = match blocked.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("a new valid request remains observable after actual output Drop"),
        };
        let mut no_physical: Option<QueryBlockingReservation> = None;
        offer
            .reobserve_after_drop(&mut no_physical)
            .unwrap()
            .notify();
        drop(offer);
        drop(blocked);
        drop(held);
        shutdown(&mut owner).await;
    }
    struct PanicOnceWake {
        armed: AtomicBool,
        dropped: Arc<AtomicUsize>,
    }
    impl Wake for PanicOnceWake {
        fn wake(self: Arc<Self>) {
            if self.armed.swap(false, Ordering::SeqCst) {
                std::panic::panic_any(PanicTag(Arc::clone(&self.dropped)));
            }
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn successful_result_and_failed_physical_cleanup_share_the_actual_original_cause() {
        let (control, handle, mut driver) = joint_fixture();
        let root = root(&control, &mut driver);
        let (mut owner, executor) = worker();
        let mut pending = request(&handle, &root.owner);
        let mut physical = Some(accept(&executor, &mut pending, &mut driver));
        let (started, did_start) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let mut receipt = None;
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    move || {
                        let _ = started.send(());
                        let _ = released.recv_timeout(Duration::from_secs(5));
                        41usize
                    },
                    &mut receipt,
                )
                .unwrap(),
        );
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        receive(&did_start).await;
        let dropped = Arc::new(AtomicUsize::new(0));
        let wake = Waker::from(Arc::new(PanicOnceWake {
            armed: AtomicBool::new(true),
            dropped: Arc::clone(&dropped),
        }));
        let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(&wake))
                .is_pending()
        );
        release.send(()).unwrap();
        let mut receipt = receipt.unwrap();
        assert_eq!(result(&mut receipt).await.unwrap(), 41);
        assert_eq!(
            exited(&receipt).await,
            Err(QueryBlockingActualExitFailure::FailedUnproven)
        );
        let cause = receipt
            .actual_exit_failure()
            .expect("successful result is not the physical failure");
        assert!(cause.original_payload_is::<PanicTag>());
        assert!(
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
        let retained = owner.take_worker_failure().unwrap();
        assert!(retained.cause().unwrap().shares_original_with(&cause));
        drop(progress);
        drop(retained);
        drop(cause);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(receipt);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    // A real finite test factory; production registers only FA's private
    // make_cpu_work after reviewing its four closed operation paths.
    fn fixture_closed_work(
        runs: Arc<AtomicUsize>,
        value: usize,
    ) -> impl FnOnce() -> usize + Send + 'static {
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            value
        }
    }
    fn positive_computation(
        handle: &JointAdmissionHandle,
        owner: &WorkOwner,
        driver: &mut JointProgressDriver,
    ) -> InternalComputationGrant {
        let mut proposal = handle.begin_paid_proposal(owner).unwrap();
        proposal
            .begin_new(
                JointLane::Foreground,
                JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(JointResource::Computations, 1),
            )
            .unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive(driver);
        let mut batch = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(batch)) => batch,
            _ => panic!("original positive computation"),
        };
        let mut raw = batch.new_claims.pop();
        let mut grant = None;
        InternalComputationGrant::adopt_in(&owner.scope(), &mut raw, &mut grant)
            .unwrap()
            .notify();
        drop(batch);
        drop(admission);
        grant.unwrap()
    }
    #[tokio::test(flavor = "current_thread")]
    async fn closed_factory_binds_original_wc_pool_and_call_through_real_worker_exit() {
        use novarocks_workload_control::WorkError;
        let (mut control, handle, mut driver) = joint_fixture_before_ready();
        let (mut owner, executor) = worker();
        let old_raw_view = executor.clone();
        let descriptor = owner
            .bind_internal_result_closed_factory_for_composition(fixture_closed_work)
            .unwrap();
        let binding = control
            .bind_internal_result_closed_factory_for_composition(descriptor)
            .unwrap();
        control.mark_ready().unwrap();
        let root = root(&control, &mut driver);
        let computation = positive_computation(&handle, &root.owner, &mut driver);
        // Pure capture remains available, but cannot create an E CPU request
        // in this registered WC. The later bound request reuses the same Q=1
        // original proposal cell and completes on the real worker below.
        let unbound = computation.cpu_action_source(&root.owner.scope()).unwrap();
        assert!(matches!(
            unbound.begin(
                std::num::NonZeroU64::new(1).unwrap(),
                std::num::NonZeroU64::new(1).unwrap()
            ),
            Err(JointAdmissionFailure {
                error: WorkError::Conflict
            })
        ));
        let runs = Arc::new(AtomicUsize::new(0));
        let wrong_drops = Arc::new(AtomicUsize::new(0));
        let wrong = DropCount(Arc::clone(&wrong_drops));
        assert!(matches!(
            old_raw_view
                .execute_preserving(move || -> usize {
                    drop(wrong);
                    panic!("an old raw view cannot execute an unregistered factory");
                })
                .await,
            Err(QueryBlockingFailure::SubmissionClosed)
        ));
        assert_eq!(wrong_drops.load(Ordering::SeqCst), 1);
        assert_eq!(executor.handle.snapshot().queued, 0);
        assert_eq!(executor.handle.snapshot().running, 0);
        assert!(matches!(
            executor.try_reserve(),
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));

        let (mut foreign_owner, foreign_executor) = worker();
        let foreign_descriptor = foreign_owner
            .bind_internal_result_closed_factory_for_composition(fixture_closed_work)
            .unwrap();
        let mut foreign_control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let foreign_binding = foreign_control
            .bind_internal_result_closed_factory_for_composition(foreign_descriptor)
            .unwrap();
        assert!(matches!(
            foreign_binding
                .bind_source(computation.cpu_action_source(&root.owner.scope()).unwrap()),
            Err(WorkError::ForeignAuthority)
        ));
        assert!(matches!(
            foreign_executor.try_reserve_internal_result_closed_factory(&binding),
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));
        assert!(matches!(
            executor.try_reserve_internal_result_closed_factory(&foreign_binding),
            Err(QueryBlockingReserveRefusal::FactoryMismatch)
        ));

        fn call_layout<I, C, F: FnOnce() -> usize + Send + 'static>(
            _: fn(I, C) -> F,
        ) -> std::alloc::Layout {
            QueryBlockingExecutor::preserving_call_layout::<usize, F>()
        }
        let bytes = bounded_worker::queue_record_layout::<CpuResult>()
            .size()
            .checked_add(call_layout(fixture_closed_work).size())
            .unwrap() as u64;
        let underquoted = binding
            .bind_source(computation.cpu_action_source(&root.owner.scope()).unwrap())
            .unwrap()
            .begin(
                std::num::NonZeroU64::new(1).unwrap(),
                std::num::NonZeroU64::new(1).unwrap(),
            );
        assert!(matches!(
            underquoted,
            Err(JointAdmissionFailure {
                error: WorkError::Conflict
            })
        ));
        let source = binding
            .bind_source(computation.cpu_action_source(&root.owner.scope()).unwrap())
            .unwrap();
        let mut action = source
            .begin(
                std::num::NonZeroU64::new(1).unwrap(),
                std::num::NonZeroU64::new(bytes).unwrap(),
            )
            .unwrap();
        drive(&mut driver);
        let mut physical = None;
        let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("same paid closed action"),
        };
        let mut original = Some(
            executor
                .try_reserve_internal_result_closed_factory(&binding)
                .unwrap(),
        );
        offer
            .accept_in(&mut original, &mut physical)
            .unwrap()
            .notify();
        drop(offer);
        assert!(
            physical
                .as_ref()
                .unwrap()
                .is_internal_result_closed_factory()
        );
        assert!(matches!(
            physical.as_ref().unwrap().claims()[0].view(),
            Err(WorkError::Conflict)
        ));
        let wrong = DropCount(Arc::clone(&wrong_drops));
        let mut receipt = None;
        let refused = executor
            .prepare_preserving_in(
                physical.as_mut().unwrap(),
                move || {
                    drop(wrong);
                    0usize
                },
                &mut receipt,
            )
            .unwrap_err();
        assert_eq!(
            refused.reason(),
            QueryBlockingPublishRefusal::FactoryMismatch
        );
        assert_eq!(wrong_drops.load(Ordering::SeqCst), 1);
        assert!(receipt.is_none() && physical.is_some());
        drop(refused.into_work());
        assert_eq!(wrong_drops.load(Ordering::SeqCst), 2);
        let mut prepared = Some(
            executor
                .prepare_preserving_in(
                    physical.as_mut().unwrap(),
                    fixture_closed_work(Arc::clone(&runs), 42),
                    &mut receipt,
                )
                .unwrap(),
        );
        assert_eq!(
            prepared.as_ref().unwrap().call_layout(),
            call_layout(fixture_closed_work)
        );
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(
            foreign_executor
                .publish_joint_in(&mut physical, &mut prepared)
                .err()
                .unwrap(),
            QueryBlockingPublishRefusal::ForeignPool
        );
        assert!(physical.is_some() && prepared.is_some() && receipt.is_some());
        executor
            .publish_joint_in(&mut physical, &mut prepared)
            .unwrap()
            .notify();
        let mut receipt = receipt.unwrap();
        assert_eq!(result(&mut receipt).await.unwrap(), 42);
        exited(&receipt).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        // Same registered call on the old non-Root path remains ordinary E:
        // no joint grant is minted by the generic execute API.
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                old_raw_view.execute_preserving(fixture_closed_work(Arc::clone(&runs), 43))
            )
            .await
            .unwrap()
            .unwrap(),
            43
        );
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        drop(receipt);
        drop(action);
        // A real unpublished original reservation can also be disposed by its
        // original caller. Extracting its raw claim cannot turn this finite
        // action into generic growth, a payload grant or an early refund.
        let source = binding
            .bind_source(computation.cpu_action_source(&root.owner.scope()).unwrap())
            .unwrap();
        let mut action = source
            .begin(
                std::num::NonZeroU64::new(1).unwrap(),
                std::num::NonZeroU64::new(bytes).unwrap(),
            )
            .unwrap();
        drive(&mut driver);
        let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("same original physical reservation for explicit refusal cleanup"),
        };
        let mut original = Some(
            executor
                .try_reserve_internal_result_closed_factory(&binding)
                .unwrap(),
        );
        let mut physical = None;
        offer
            .accept_in(&mut original, &mut physical)
            .unwrap()
            .notify();
        drop(offer);
        let mut original_physical = None;
        let mut original_claims = None;
        assert!(
            physical
                .take()
                .unwrap()
                .transfer_in(&mut original_physical, &mut original_claims)
                .is_ok()
        );
        let mut claims = original_claims.take().unwrap().into_iter();
        let mut raw = Some(claims.next().unwrap());
        assert!(claims.next().is_none());
        drop(claims);
        assert!(matches!(
            raw.as_ref().unwrap().view(),
            Err(WorkError::Conflict)
        ));
        assert!(matches!(
            raw.as_mut()
                .unwrap()
                .release_part(JointResources::ZERO.with(JointResource::InternalCpuJobs, 1)),
            Err(WorkError::Conflict)
        ));
        let mut payload = None;
        assert!(matches!(
            RetainedPayloadGrant::adopt_in(&root.owner.scope(), &mut raw, &mut payload),
            Err(WorkError::Conflict)
        ));
        assert!(raw.is_some() && payload.is_none());
        drop(original_physical.take());
        drop(raw);
        drop(action);
        drop(computation);
        let novarocks_workload_control::RootWork {
            owner: work_owner,
            business,
        } = root;
        drop(business);
        work_owner.complete();
        drive(&mut driver);
        shutdown(&mut owner).await;
        shutdown(&mut foreign_owner).await;
    }
}
