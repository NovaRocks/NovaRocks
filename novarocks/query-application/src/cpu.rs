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

use crate::{
    cancellation::{QueryCancellationReason, QueryCancellationView},
    coordination::{
        BoundedResultDecodeHandle, BoundedResultDecodeOwner, ResultDecodeExecutorConfig,
        ResultDecodeJob,
    },
};

type CpuResult = Box<dyn Any + Send>;

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

struct ElasticPlanningJob {
    run: Box<dyn FnOnce() -> CpuResult + Send>,
    result: oneshot::Sender<Result<CpuResult, String>>,
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
}

struct ElasticPlanningPoolState {
    accepting: bool,
    next_worker_id: WorkerId,
    workers: BTreeMap<WorkerId, ElasticWorkerState>,
    joins: BTreeMap<WorkerId, JoinHandle<()>>,
}

impl Default for ElasticPlanningPoolState {
    fn default() -> Self {
        Self {
            accepting: true,
            next_worker_id: 0,
            workers: BTreeMap::new(),
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
        self.reap_finished();
        let (result, receipt) = oneshot::channel();
        let job = ElasticPlanningJob {
            run: Box::new(run),
            result,
        };
        let assignment = {
            let mut state = self.state.lock().unwrap();
            if !state.accepting {
                return Err("query CPU executor closed before admitting work".to_owned());
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
                state.next_worker_id = state
                    .next_worker_id
                    .checked_add(1)
                    .ok_or_else(|| "query CPU worker id overflow".to_owned())?;
                let id = state.next_worker_id;
                state.workers.insert(id, ElasticWorkerState::Starting);
                ElasticAssignment::Start { id }
            }
        };
        match assignment {
            ElasticAssignment::Idle { id, sender } => match sender.send(WorkerCommand::Run(job)) {
                Ok(()) => Ok(receipt),
                Err(error) => {
                    self.state.lock().unwrap().workers.remove(&id);
                    let WorkerCommand::Run(job) = error.0 else {
                        unreachable!("only a job is assigned to an idle worker")
                    };
                    let _ = job.result.send(Err(
                        "query CPU worker exited before accepting work".to_owned()
                    ));
                    Ok(receipt)
                }
            },
            ElasticAssignment::Start { id } => {
                let (sender, receiver) = mpsc::channel();
                let pool = Arc::clone(self);
                match std::thread::Builder::new()
                    .name(format!("novarocks-query-planning-{id}"))
                    .spawn(move || worker_loop(pool, id, sender, receiver, job))
                {
                    Ok(join) => {
                        self.state.lock().unwrap().joins.insert(id, join);
                        Ok(receipt)
                    }
                    Err(error) => {
                        self.state.lock().unwrap().workers.remove(&id);
                        Err(format!("create query CPU worker: {error}"))
                    }
                }
            }
        }
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
            if self.state.lock().unwrap().joins.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("query CPU workers did not exit before shutdown deadline".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn reap_finished(&self) {
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
            let _ = join.join();
        }
    }

    #[cfg(test)]
    fn snapshot(&self) -> ElasticPlanningPoolSnapshot {
        let state = self.state.lock().unwrap();
        ElasticPlanningPoolSnapshot {
            workers: state.workers.len(),
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
        let output = catch_unwind(AssertUnwindSafe(job.run));
        let panicked = output.is_err();
        let output = output.map_err(|_| "query CPU worker panicked".to_owned());
        let _ = job.result.send(output);

        if panicked {
            pool.state.lock().unwrap().workers.remove(&id);
            return;
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
    owner: BoundedResultDecodeOwner<CpuResult>,
    executor: QueryBlockingExecutor,
}

#[derive(Clone)]
pub struct QueryBlockingExecutor {
    handle: BoundedResultDecodeHandle<CpuResult>,
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
        let owner = BoundedResultDecodeOwner::try_new(ResultDecodeExecutorConfig::new(
            config.worker_threads(),
            config.queue_capacity(),
        ))
        .map_err(|error| format!("open query blocking executor: {error}"))?;
        let executor = QueryBlockingExecutor {
            handle: owner.handle(),
        };
        Ok(Self { owner, executor })
    }

    pub fn executor(&self) -> QueryBlockingExecutor {
        self.executor.clone()
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

impl QueryBlockingExecutor {
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
        let receipt = self
            .handle
            .submit(ResultDecodeJob::new(move || Box::new(work()) as CpuResult))
            .await
            .map_err(|_| "query blocking executor closed before admitting work".to_owned())?;
        receipt
            .complete()
            .await
            .map_err(|error| format!("query blocking worker failed: {error}"))?
            .downcast::<T>()
            .map(|result| *result)
            .map_err(|_| "query blocking worker returned an invalid result type".to_owned())
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
