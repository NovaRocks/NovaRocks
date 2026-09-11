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

//! Bounded process executor for synchronous result decoding.
//!
//! The executor knows nothing about Native packets or Arrow. A move-only job
//! closure may capture both the raw packet and its result credit, keeping that
//! ownership together from admission through synchronous execution.

use std::{
    collections::VecDeque,
    fmt,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, OnceLock, mpsc},
    thread::{self, JoinHandle},
    time::Instant,
};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

type DecodeCall<R> = Box<dyn FnOnce() -> R + Send + 'static>;

/// One move-only synchronous decode operation.
pub(crate) struct ResultDecodeJob<R> {
    call: Option<DecodeCall<R>>,
}

impl<R> ResultDecodeJob<R> {
    pub(crate) fn new(call: impl FnOnce() -> R + Send + 'static) -> Self {
        Self {
            call: Some(Box::new(call)),
        }
    }

    fn run(mut self) -> R {
        self.call
            .take()
            .expect("a result decode job runs at most once")()
    }
}

impl<R> fmt::Debug for ResultDecodeJob<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResultDecodeJob")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResultDecodeExecutorConfig {
    worker_threads: NonZeroUsize,
    queue_capacity: NonZeroUsize,
}

impl ResultDecodeExecutorConfig {
    pub(crate) const fn new(worker_threads: NonZeroUsize, queue_capacity: NonZeroUsize) -> Self {
        Self {
            worker_threads,
            queue_capacity,
        }
    }

    pub(crate) const fn worker_threads(self) -> NonZeroUsize {
        self.worker_threads
    }

    pub(crate) const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResultDecodeExecutorOpenError {
    detail: Arc<str>,
}

impl fmt::Display for ResultDecodeExecutorOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ResultDecodeExecutorOpenError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultDecodeWorkerError {
    Panicked,
    ExecutorClosed,
}

impl fmt::Display for ResultDecodeWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Panicked => "result decode worker panicked",
            Self::ExecutorClosed => "result decode executor closed before completing the job",
        })
    }
}

impl std::error::Error for ResultDecodeWorkerError {}

/// A refused submission returns the still-unexecuted move-only job.
pub(crate) struct ResultDecodeSubmitError<R> {
    job: ResultDecodeJob<R>,
}

impl<R> ResultDecodeSubmitError<R> {
    pub(crate) fn into_job(self) -> ResultDecodeJob<R> {
        self.job
    }
}

impl<R> fmt::Debug for ResultDecodeSubmitError<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResultDecodeSubmitError")
            .finish_non_exhaustive()
    }
}

/// Completion ownership is independent of job execution. Dropping this value
/// never cancels a queued or running job.
pub(crate) struct ResultDecodeReceipt<R> {
    receiver: oneshot::Receiver<Result<R, ResultDecodeWorkerError>>,
}

impl<R> ResultDecodeReceipt<R> {
    pub(crate) async fn complete(self) -> Result<R, ResultDecodeWorkerError> {
        self.receiver
            .await
            .unwrap_or(Err(ResultDecodeWorkerError::ExecutorClosed))
    }
}

impl<R> fmt::Debug for ResultDecodeReceipt<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResultDecodeReceipt")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResultDecodeExecutorSnapshot {
    pub(crate) queued: usize,
    pub(crate) running: usize,
    pub(crate) worker_threads: usize,
    pub(crate) queue_capacity: usize,
}

struct QueuedJob<R> {
    job: ResultDecodeJob<R>,
    completion: oneshot::Sender<Result<R, ResultDecodeWorkerError>>,
    queue_slot: OwnedSemaphorePermit,
}

struct QueueState<R> {
    closed: bool,
    jobs: VecDeque<QueuedJob<R>>,
    running: usize,
    live_workers: usize,
}

struct Shared<R> {
    state: Mutex<QueueState<R>>,
    ready: Condvar,
    worker_exit: Notify,
    queue_slots: Arc<Semaphore>,
    worker_threads: usize,
    queue_capacity: usize,
}

/// Unique process-lifetime owner of the result decode worker threads.
///
/// Query work receives only [`BoundedResultDecodeHandle`]. Keeping the worker
/// join handles here prevents a query-scoped handle drop from closing or
/// joining the process executor.
pub(crate) struct BoundedResultDecodeOwner<R> {
    shared: Arc<Shared<R>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

/// Cloneable submission handle for the process-owned result decode executor.
///
/// This handle deliberately has no edge to the owner or its join handles.
/// Dropping any or all handles therefore never closes the executor or blocks
/// while worker threads converge.
pub(crate) struct BoundedResultDecodeHandle<R> {
    shared: Arc<Shared<R>>,
}

impl<R> Clone for BoundedResultDecodeHandle<R> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<R: Send + 'static> fmt::Debug for BoundedResultDecodeHandle<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedResultDecodeHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl<R: Send + 'static> BoundedResultDecodeOwner<R> {
    pub(crate) fn try_new(
        config: ResultDecodeExecutorConfig,
    ) -> Result<Self, ResultDecodeExecutorOpenError> {
        let worker_threads = config.worker_threads().get();
        let queue_capacity = config.queue_capacity().get();
        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                closed: false,
                jobs: VecDeque::with_capacity(queue_capacity),
                running: 0,
                live_workers: 0,
            }),
            ready: Condvar::new(),
            worker_exit: Notify::new(),
            queue_slots: Arc::new(Semaphore::new(queue_capacity)),
            worker_threads,
            queue_capacity,
        });
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker_shared = Arc::clone(&shared);
            match thread::Builder::new()
                .name(format!("result-decode-{index}"))
                .spawn(move || run_worker(worker_shared))
            {
                Ok(worker) => {
                    shared
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .live_workers += 1;
                    workers.push(worker);
                }
                Err(error) => {
                    close_queue(&shared);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(ResultDecodeExecutorOpenError {
                        detail: format!("spawn result decode worker failed: {error}").into(),
                    });
                }
            }
        }
        Ok(Self {
            shared,
            workers: Mutex::new(workers),
        })
    }
}

impl<R> BoundedResultDecodeOwner<R> {
    pub(crate) fn handle(&self) -> BoundedResultDecodeHandle<R> {
        BoundedResultDecodeHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn request_close(&self) {
        close_queue(&self.shared);
    }

    /// Closes admission and synchronously joins the fixed worker set.
    ///
    /// Process composition must call this from its blocking shutdown path, not
    /// from a Tokio coordinator worker. Drop only closes admission and hands
    /// the workers to the process reaper; it never joins on the dropping
    /// thread.
    pub(crate) fn shutdown_and_join(mut self) -> Result<(), ResultDecodeShutdownError> {
        self.shutdown_and_join_inner()
    }

    /// Closes admission and waits for the same fixed worker set under one
    /// absolute deadline.
    ///
    /// The unique owner and every unjoined worker handle remain in place when
    /// the wait is cancelled or the deadline elapses, so process composition
    /// can resume the exact shutdown instead of detaching a blocking join.
    pub(crate) async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), ResultDecodeShutdownError> {
        close_queue(&self.shared);
        let wait = wait_for_worker_exit(&self.shared);
        if tokio::time::timeout_at(deadline.into(), wait)
            .await
            .is_err()
        {
            let live_workers = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .live_workers;
            return Err(ResultDecodeShutdownError::DeadlineExceeded { live_workers });
        }
        self.join_finished_workers()
    }

    fn shutdown_and_join_inner(&mut self) -> Result<(), ResultDecodeShutdownError> {
        close_queue(&self.shared);
        self.join_finished_workers()
    }

    fn join_finished_workers(&mut self) -> Result<(), ResultDecodeShutdownError> {
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        let mut panicked_workers = 0;
        for worker in workers.drain(..) {
            if worker.join().is_err() {
                panicked_workers += 1;
            }
        }
        if panicked_workers == 0 {
            Ok(())
        } else {
            Err(ResultDecodeShutdownError::WorkerPanicked { panicked_workers })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultDecodeShutdownError {
    DeadlineExceeded { live_workers: usize },
    WorkerPanicked { panicked_workers: usize },
}

impl fmt::Display for ResultDecodeShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded { live_workers } => write!(
                formatter,
                "result decode shutdown deadline exceeded with {live_workers} live worker(s)"
            ),
            Self::WorkerPanicked { panicked_workers } => write!(
                formatter,
                "{panicked_workers} result decode worker(s) panicked during shutdown"
            ),
        }
    }
}

impl std::error::Error for ResultDecodeShutdownError {}

impl<R: Send + 'static> BoundedResultDecodeHandle<R> {
    /// Waits only for bounded queue admission. Once this returns a receipt,
    /// the worker queue owns the job and dropping the caller future or receipt
    /// cannot cancel it.
    pub(crate) async fn submit(
        &self,
        job: ResultDecodeJob<R>,
    ) -> Result<ResultDecodeReceipt<R>, ResultDecodeSubmitError<R>> {
        let queue_slot = match Arc::clone(&self.shared.queue_slots).acquire_owned().await {
            Ok(slot) => slot,
            Err(_) => return Err(ResultDecodeSubmitError { job }),
        };
        let (completion, receiver) = oneshot::channel();
        let queued = QueuedJob {
            job,
            completion,
            queue_slot,
        };
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.closed {
            return Err(ResultDecodeSubmitError { job: queued.job });
        }
        debug_assert!(state.jobs.len() < self.shared.queue_capacity);
        state.jobs.push_back(queued);
        drop(state);
        self.shared.ready.notify_one();
        Ok(ResultDecodeReceipt { receiver })
    }

    pub(crate) fn snapshot(&self) -> ResultDecodeExecutorSnapshot {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        ResultDecodeExecutorSnapshot {
            queued: state.jobs.len(),
            running: state.running,
            worker_threads: self.shared.worker_threads,
            queue_capacity: self.shared.queue_capacity,
        }
    }
}

fn run_worker<R: Send + 'static>(shared: Arc<Shared<R>>) {
    struct Exit<'a, R>(&'a Shared<R>);

    impl<R> Drop for Exit<'_, R> {
        fn drop(&mut self) {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.live_workers -= 1;
            drop(state);
            self.0.worker_exit.notify_waiters();
        }
    }

    let _exit = Exit(shared.as_ref());
    loop {
        let queued = {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            loop {
                if let Some(queued) = state.jobs.pop_front() {
                    state.running += 1;
                    break queued;
                }
                if state.closed {
                    return;
                }
                state = shared
                    .ready
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
            }
        };
        let QueuedJob {
            job,
            completion,
            queue_slot,
        } = queued;
        // Admission protects the bounded waiting queue only. Once this worker
        // owns the job, its fixed thread is the running bound.
        drop(queue_slot);
        let outcome = catch_unwind(AssertUnwindSafe(|| job.run()))
            .map_err(|_| ResultDecodeWorkerError::Panicked);
        if let Err(undelivered) = completion.send(outcome) {
            // The result is destroyed here, on the worker, before this job is
            // removed from the running set.
            drop(undelivered);
        }
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.running -= 1;
    }
}

async fn wait_for_worker_exit<R>(shared: &Shared<R>) {
    loop {
        let notified = shared.worker_exit.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .live_workers
            == 0
        {
            return;
        }
        notified.await;
    }
}

fn close_queue<R>(shared: &Shared<R>) {
    let queued = {
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        shared.queue_slots.close();
        state.jobs.drain(..).collect::<Vec<_>>()
    };
    drop(queued);
    shared.ready.notify_all();
}

impl<R> Drop for BoundedResultDecodeOwner<R> {
    fn drop(&mut self) {
        close_queue(&self.shared);
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        reap_workers(std::mem::take(workers));
    }
}

type ResultDecodeWorkerSet = Vec<JoinHandle<()>>;

/// The fallback path cannot report worker panics, but it must still keep thread
/// convergence away from arbitrary async/runtime destruction paths. A single
/// process reaper owns every fallback join. If the reaper itself cannot be
/// created, dropping the join handles safely detaches the already-closing
/// workers instead of blocking the owner destructor.
fn reap_workers(workers: ResultDecodeWorkerSet) {
    if workers.is_empty() {
        return;
    }
    static REAPER: OnceLock<Option<mpsc::Sender<ResultDecodeWorkerSet>>> = OnceLock::new();
    let reaper = REAPER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<ResultDecodeWorkerSet>();
        thread::Builder::new()
            .name("result-decode-reaper".to_string())
            .spawn(move || {
                while let Ok(workers) = receiver.recv() {
                    for worker in workers {
                        let _ = worker.join();
                    }
                }
            })
            .ok()
            .map(|reaper| {
                // The process owns the reaper lifetime. Its channel sender is
                // process-static, so joining it during ordinary teardown would
                // require another blocking destructor path.
                drop(reaper);
                sender
            })
    });
    if let Some(reaper) = reaper {
        if let Err(mpsc::SendError(workers)) = reaper.send(workers) {
            // Dropping JoinHandle detaches a thread. Intake is already closed,
            // so each worker still exits after its current job finishes.
            drop(workers);
        }
    } else {
        drop(workers);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread::ThreadId,
        time::{Duration, Instant},
    };

    use super::*;

    fn executor<R: Send + 'static>(
        workers: usize,
        queue: usize,
    ) -> (BoundedResultDecodeOwner<R>, BoundedResultDecodeHandle<R>) {
        let owner = BoundedResultDecodeOwner::try_new(ResultDecodeExecutorConfig::new(
            NonZeroUsize::new(workers).unwrap(),
            NonZeroUsize::new(queue).unwrap(),
        ))
        .unwrap();
        let handle = owner.handle();
        (owner, handle)
    }

    async fn wait_for(
        supervisor: &BoundedResultDecodeHandle<usize>,
        queued: usize,
        running: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = supervisor.snapshot();
                if snapshot.queued == queued && snapshot.running == running {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("executor state did not converge");
    }

    async fn receive<T>(receiver: &mpsc::Receiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match receiver.try_recv() {
                    Ok(value) => return value,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("worker observation channel disconnected")
                    }
                }
            }
        })
        .await
        .expect("worker observation timed out")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_job_does_not_block_a_single_thread_tokio_runtime() {
        let (_owner, supervisor) = executor::<usize>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, did_start) = mpsc::channel();
        let receipt = supervisor
            .submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                7
            }))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while did_start.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(tokio::spawn(async { 11 }).await.unwrap(), 11);
        release.send(()).unwrap();
        assert_eq!(receipt.complete().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn fixed_workers_and_queue_never_exceed_their_bounds() {
        let (_owner, supervisor) = executor::<usize>(2, 2);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut receipts = Vec::new();
        for value in 0..4 {
            let gate = Arc::clone(&gate);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            receipts.push(
                supervisor
                    .submit(ResultDecodeJob::new(move || {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        let (lock, ready) = &*gate;
                        let mut open = lock.lock().unwrap();
                        while !*open {
                            open = ready.wait(open).unwrap();
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        value
                    }))
                    .await
                    .unwrap(),
            );
        }
        wait_for(&supervisor, 2, 2).await;
        let snapshot = supervisor.snapshot();
        assert_eq!((snapshot.worker_threads, snapshot.queue_capacity), (2, 2));
        assert!(snapshot.running <= 2);
        assert!(snapshot.queued <= 2);
        let mut fifth = Box::pin(supervisor.submit(ResultDecodeJob::new(|| 4)));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut fifth)
                .await
                .is_err()
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for (expected, receipt) in receipts.into_iter().enumerate() {
            assert_eq!(receipt.complete().await.unwrap(), expected);
        }
        assert_eq!(fifth.await.unwrap().complete().await.unwrap(), 4);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    struct DropNotice {
        dropped: Option<mpsc::Sender<ThreadId>>,
    }

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(std::thread::current().id());
            }
        }
    }

    #[tokio::test]
    async fn dropped_waiter_releases_its_job_and_queue_admission() {
        let (_owner, supervisor) = executor::<usize>(1, 1);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let running_gate = Arc::clone(&gate);
        let running = supervisor
            .submit(ResultDecodeJob::new(move || {
                let (lock, ready) = &*running_gate;
                let mut open = lock.lock().unwrap();
                while !*open {
                    open = ready.wait(open).unwrap();
                }
                1
            }))
            .await
            .unwrap();
        wait_for(&supervisor, 0, 1).await;
        let queued = supervisor.submit(ResultDecodeJob::new(|| 2)).await.unwrap();
        wait_for(&supervisor, 1, 1).await;
        let (dropped, observed_drop) = mpsc::channel();
        let notice = DropNotice {
            dropped: Some(dropped),
        };
        let mut waiting = Box::pin(supervisor.submit(ResultDecodeJob::new(move || {
            drop(notice);
            3
        })));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        drop(waiting);
        observed_drop
            .recv_timeout(Duration::from_secs(1))
            .expect("a cancelled admission must drop its unsubmitted job");

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        assert_eq!(running.complete().await.unwrap(), 1);
        assert_eq!(queued.complete().await.unwrap(), 2);
        assert_eq!(supervisor.snapshot().queued, 0);
        let replacement = tokio::time::timeout(
            Duration::from_secs(1),
            supervisor.submit(ResultDecodeJob::new(|| 4)),
        )
        .await
        .expect("a cancelled waiter must not leak queue admission")
        .unwrap();
        assert_eq!(replacement.complete().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn dropped_completion_is_destroyed_on_the_worker_and_job_still_runs() {
        let (_owner, supervisor) = executor::<DropNotice>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, did_start) = mpsc::channel();
        let (dropped, observed_drop) = mpsc::channel();
        let receipt = supervisor
            .submit(ResultDecodeJob::new(move || {
                started.send(std::thread::current().id()).unwrap();
                released.recv().unwrap();
                DropNotice {
                    dropped: Some(dropped),
                }
            }))
            .await
            .unwrap();
        let worker = receive(&did_start).await;
        drop(receipt);
        release.send(()).unwrap();
        let dropped_on = receive(&observed_drop).await;
        assert_eq!(dropped_on, worker);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_the_last_handle_does_not_close_or_join_the_executor() {
        let (owner, handle) = executor::<usize>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, did_start) = mpsc::channel();
        let running = handle
            .submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                1
            }))
            .await
            .unwrap();
        receive(&did_start).await;

        // This is the last submission handle, while its worker is blocked.
        // Drop must return before the worker is released.
        drop(handle);
        let replacement = owner.handle();
        let queued = tokio::time::timeout(
            Duration::from_secs(1),
            replacement.submit(ResultDecodeJob::new(|| 2)),
        )
        .await
        .expect("dropping query handles must leave process admission open")
        .unwrap();

        release.send(()).unwrap();
        assert_eq!(running.complete().await.unwrap(), 1);
        assert_eq!(queued.complete().await.unwrap(), 2);
        drop(replacement);
        owner.shutdown_and_join().unwrap();
    }

    #[tokio::test]
    async fn a_job_may_capture_a_handle_without_an_owner_cycle_or_self_join() {
        let (owner, handle) = executor::<usize>(1, 1);
        let captured = handle.clone();
        let receipt = handle
            .submit(ResultDecodeJob::new(move || {
                assert_eq!(captured.snapshot().worker_threads, 1);
                drop(captured);
                7
            }))
            .await
            .unwrap();
        drop(handle);
        assert_eq!(receipt.complete().await.unwrap(), 7);
        owner.shutdown_and_join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fallback_owner_drop_closes_intake_without_joining_on_the_runtime_thread() {
        let (owner, handle) = executor::<usize>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, observed_start) = mpsc::channel();
        let running = handle
            .submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                1
            }))
            .await
            .unwrap();
        receive(&observed_start).await;
        let queued = handle.submit(ResultDecodeJob::new(|| 2)).await.unwrap();
        wait_for(&handle, 1, 1).await;

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            release.send(()).unwrap();
        });
        let drop_started = Instant::now();
        drop(owner);
        assert!(
            drop_started.elapsed() < Duration::from_millis(300),
            "fallback owner Drop synchronously waited for a running decode job"
        );
        assert_eq!(tokio::spawn(async { 7 }).await.unwrap(), 7);
        assert_eq!(
            queued.complete().await,
            Err(ResultDecodeWorkerError::ExecutorClosed)
        );
        match tokio::time::timeout(
            Duration::from_secs(1),
            handle.submit(ResultDecodeJob::new(|| 3)),
        )
        .await
        .expect("closed intake must reject without waiting")
        {
            Ok(_) => panic!("fallback owner Drop must close future admission"),
            Err(error) => drop(error.into_job()),
        }
        assert_eq!(running.complete().await.unwrap(), 1);
        releaser.join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_deadline_retains_the_same_decode_workers_for_retry() {
        let (mut owner, handle) = executor::<usize>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, observed_start) = mpsc::channel();
        let running = handle
            .submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                1
            }))
            .await
            .unwrap();
        receive(&observed_start).await;

        assert_eq!(
            owner.shutdown_until(Instant::now()).await,
            Err(ResultDecodeShutdownError::DeadlineExceeded { live_workers: 1 })
        );
        match handle.submit(ResultDecodeJob::new(|| 2)).await {
            Ok(_) => panic!("decode shutdown must keep intake closed across retry"),
            Err(error) => drop(error.into_job()),
        }

        release.send(()).unwrap();
        assert_eq!(running.complete().await.unwrap(), 1);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("retry joins the original worker set");
    }

    #[test]
    fn shutdown_breaks_queued_handle_cycles_and_running_handles_do_not_self_join() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (owner, handle) = executor::<usize>(1, 1);
        let running_handle = handle.clone();
        let queued_handle = handle.clone();
        let (started, observed_start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let running = runtime
            .block_on(handle.submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                drop(running_handle);
                1
            })))
            .unwrap();
        observed_start.recv_timeout(Duration::from_secs(1)).unwrap();
        let queued = runtime
            .block_on(handle.submit(ResultDecodeJob::new(move || {
                drop(queued_handle);
                2
            })))
            .unwrap();
        drop(handle);
        drop(running);
        drop(queued);

        let releaser = std::thread::spawn(move || release.send(()).unwrap());
        owner.shutdown_and_join().unwrap();
        releaser.join().unwrap();
    }

    #[test]
    fn process_owner_shutdown_closes_the_queue_and_joins_workers() {
        let finished = Arc::new(AtomicUsize::new(0));
        let worker_finished = Arc::clone(&finished);
        let drop_started = Arc::new(AtomicUsize::new(0));
        let drop_returned = Arc::new(AtomicUsize::new(0));
        let (started, observed_start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (resumed, observed_resume) = mpsc::channel();
        let (finish, may_finish) = mpsc::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (owner, supervisor) = executor::<usize>(1, 1);
        let receipt = runtime
            .block_on(supervisor.submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                resumed.send(()).unwrap();
                may_finish.recv().unwrap();
                worker_finished.store(1, Ordering::SeqCst);
                1
            })))
            .unwrap();
        observed_start.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(receipt);
        let drop_started_for_releaser = Arc::clone(&drop_started);
        let drop_returned_for_releaser = Arc::clone(&drop_returned);
        let releaser = std::thread::spawn(move || {
            while drop_started_for_releaser.load(Ordering::Acquire) == 0 {
                std::thread::yield_now();
            }
            release.send(()).unwrap();
            observed_resume
                .recv_timeout(Duration::from_secs(1))
                .unwrap();
            assert_eq!(drop_returned_for_releaser.load(Ordering::Acquire), 0);
            finish.send(()).unwrap();
        });
        drop_started.store(1, Ordering::Release);
        owner.shutdown_and_join().unwrap();
        drop_returned.store(1, Ordering::Release);
        releaser.join().unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 1);
    }
}
