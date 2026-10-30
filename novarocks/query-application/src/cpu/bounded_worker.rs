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

//! Bounded process executor for synchronous application work.
//!
//! The executor knows nothing about Native packets or Arrow. A move-only job
//! closure retains its application input and capacity owner, keeping that
//! ownership together from admission through synchronous execution.

use std::{
    any::TypeId,
    collections::VecDeque,
    fmt,
    mem::ManuallyDrop,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, OnceLock, Weak,
        atomic::{AtomicU8, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::Instant,
};

use novarocks_workload_control::joint_admission::JointLocalGrant;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

type WorkerCall<R> = Box<dyn FnOnce() -> R + Send + 'static>;

/// One move-only synchronous application operation.
pub(crate) struct BoundedWorkerJob<R> {
    call: Option<WorkerCall<R>>,
    call_layout: std::alloc::Layout,
    call_type: TypeId,
}

impl<R> BoundedWorkerJob<R> {
    pub(crate) fn new<F: FnOnce() -> R + Send + 'static>(call: F) -> Self {
        Self {
            call: Some(Box::new(call)),
            call_layout: std::alloc::Layout::new::<F>(),
            call_type: TypeId::of::<F>(),
        }
    }

    fn run(mut self) -> R {
        self.call
            .take()
            .expect("a bounded worker job runs at most once")()
    }
}

impl<R> fmt::Debug for BoundedWorkerJob<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedWorkerJob")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundedWorkerConfig {
    worker_threads: NonZeroUsize,
    queue_capacity: NonZeroUsize,
}

impl BoundedWorkerConfig {
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
pub(crate) struct BoundedWorkerOpenError {
    detail: Arc<str>,
}

impl fmt::Display for BoundedWorkerOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for BoundedWorkerOpenError {}

// The same receipt carries the original opaque panic to preserving callers.
pub(crate) use super::QueryBlockingFailure as BoundedWorkerError;

/// A refused submission retains the same move-only job for its original caller.
/// The queue lock never runs the rejected input's destructor.
pub(crate) struct BoundedWorkerSubmitError<R> {
    job: BoundedWorkerJob<R>,
}

impl<R> BoundedWorkerSubmitError<R> {
    fn rejected(job: BoundedWorkerJob<R>) -> Self {
        Self { job }
    }

    pub(crate) fn into_job(self) -> BoundedWorkerJob<R> {
        self.job
    }
}

impl<R> fmt::Debug for BoundedWorkerSubmitError<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedWorkerSubmitError")
            .finish_non_exhaustive()
    }
}

/// Completion ownership is independent of job execution. Dropping this value
/// never cancels a queued or running job.
pub(crate) struct BoundedWorkerReceipt<R> {
    pub(super) receiver: oneshot::Receiver<Result<R, BoundedWorkerError>>,
}

impl<R> BoundedWorkerReceipt<R> {
    pub(crate) async fn complete(self) -> Result<R, BoundedWorkerError> {
        self.receiver
            .await
            .unwrap_or(Err(BoundedWorkerError::ExecutorClosed))
    }
}

impl<R> fmt::Debug for BoundedWorkerReceipt<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedWorkerReceipt")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundedWorkerSnapshot {
    pub(crate) queued: usize,
    pub(crate) running: usize,
    pub(crate) worker_threads: usize,
    pub(crate) queue_capacity: usize,
}

/// This source identity is non-owning: a queued row must not point strongly
/// back to Shared through its reservation. The semaphore is the original one.
pub(crate) struct BoundedWorkerReservation {
    source: Weak<dyn Send + Sync>,
    queue_slot: Option<OwnedSemaphorePermit>,
}
impl fmt::Debug for BoundedWorkerReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedWorkerReservation")
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueReserveRefusal {
    Full,
    Closed,
    FactoryMismatch,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueuePublishRefusal {
    Empty,
    ForeignPool,
    Closed,
    ReceiptClosed,
    FactoryMismatch,
}

/// One fixed observer header for a received job, not another completion queue.
/// The platform Mutex's external backing and Waker pointees are not quoted here.
pub(crate) struct BoundedWorkerActualExit {
    phase: AtomicU8,
    observer: Mutex<ActualExitObserver>,
}
struct ActualExitObserver {
    reader: Option<Waker>,
    failure: Option<BoundedWorkerError>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerActualExitFailure {
    FailedUnproven,
}
impl BoundedWorkerActualExit {
    pub(super) fn new() -> Self {
        Self {
            phase: AtomicU8::new(0),
            observer: Mutex::new(ActualExitObserver {
                reader: None,
                failure: None,
            }),
        }
    }
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<(), WorkerActualExitFailure>> {
        let mut incoming = Some(cx.waker().clone());
        let mut outgoing = None;
        {
            let mut observer = self.observer.lock().unwrap_or_else(|e| e.into_inner());
            if self.phase.load(Ordering::Acquire) == 0
                && observer
                    .reader
                    .as_ref()
                    .is_none_or(|old| !old.will_wake(incoming.as_ref().unwrap()))
            {
                outgoing = observer.reader.replace(incoming.take().unwrap());
            }
        }
        drop(outgoing);
        drop(incoming);
        match self.phase.load(Ordering::Acquire) {
            0 => Poll::Pending,
            1 => Poll::Ready(Ok(())),
            _ => Poll::Ready(Err(WorkerActualExitFailure::FailedUnproven)),
        }
    }
    // Returns the actual raw notification panic to the original worker entry.
    // No clone, destructor or wake executes under the observer/queue mutex.
    pub(crate) fn failure(&self) -> Option<BoundedWorkerError> {
        self.observer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failure
            .clone()
    }
    pub(super) fn publish(
        &self,
        phase: u8,
        failure: Option<&BoundedWorkerError>,
    ) -> Option<Box<dyn std::any::Any + Send>> {
        let mut incoming = failure.cloned();
        let reader = {
            let mut observer = self.observer.lock().unwrap_or_else(|e| e.into_inner());
            if observer.failure.is_none() {
                observer.failure = incoming.take();
            }
            let _ = self
                .phase
                .compare_exchange(0, phase, Ordering::AcqRel, Ordering::Acquire);
            observer.reader.take()
        };
        drop(incoming);
        catch_unwind(AssertUnwindSafe(|| {
            if let Some(reader) = reader {
                reader.wake();
            }
        }))
        .err()
    }
}

/// Prepared under the caller's original reservation/source. Receipt custody
/// precedes publication; constructing this value does not enqueue or execute F.
pub(crate) struct BoundedWorkerPrepared<R> {
    source: Weak<dyn Send + Sync>,
    job: BoundedWorkerJob<R>,
    completion: oneshot::Sender<Result<R, BoundedWorkerError>>,
    exit: Arc<BoundedWorkerActualExit>,
}
impl<R> BoundedWorkerPrepared<R> {
    pub(crate) fn call_layout(&self) -> std::alloc::Layout {
        self.job.call_layout
    }
}
impl<R> fmt::Debug for BoundedWorkerPrepared<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedWorkerPrepared")
            .finish_non_exhaustive()
    }
}

pub(crate) struct BoundedWorkerPublication<R> {
    shared: Arc<Shared<R>>,
}
impl<R> BoundedWorkerPublication<R> {
    pub(crate) fn notify(self) {}
}
impl<R> Drop for BoundedWorkerPublication<R> {
    fn drop(&mut self) {
        self.shared.ready.notify_one();
    }
}

struct LegacyQueuedJob<R> {
    job: BoundedWorkerJob<R>,
    completion: oneshot::Sender<Result<R, BoundedWorkerError>>,
    queue_slot: OwnedSemaphorePermit,
}
struct JointQueuedJob<R> {
    prepared: ManuallyDrop<BoundedWorkerPrepared<R>>,
    physical: ManuallyDrop<JointLocalGrant<super::QueryBlockingReservation>>,
}
impl<R> Drop for JointQueuedJob<R> {
    fn drop(&mut self) {
        // Unexpected destruction cannot let ordinary field cleanup refund the
        // physical guard if destruction of its original call unwinds.
        // SAFETY: these fields are initialized once and never independently
        // moved on this destructor path. A prepared-field unwind deliberately
        // leaves the physical field undropped and charged.
        unsafe {
            ManuallyDrop::drop(&mut self.prepared);
            ManuallyDrop::drop(&mut self.physical);
        }
    }
}
/// An ordinary queue reservation does not carry a workload grant. The stable
/// original receiver outside this row remains responsible for source custody.
struct OrdinaryQueuedJob<R> {
    prepared: BoundedWorkerPrepared<R>,
    reservation: BoundedWorkerReservation,
}
enum QueuedJob<R> {
    Legacy(LegacyQueuedJob<R>),
    Ordinary(OrdinaryQueuedJob<R>),
    Joint(JointQueuedJob<R>),
}
enum PendingRecord<'a, R> {
    Legacy(&'a mut Option<LegacyQueuedJob<R>>),
    Ordinary {
        prepared: &'a mut Option<BoundedWorkerPrepared<R>>,
        reservation: &'a mut Option<super::QueryBlockingReservation>,
    },
    Joint {
        prepared: &'a mut Option<BoundedWorkerPrepared<R>>,
        physical: &'a mut Option<JointLocalGrant<super::QueryBlockingReservation>>,
    },
}
impl<R> PendingRecord<'_, R> {
    fn call_type(&self) -> TypeId {
        match self {
            Self::Legacy(row) => row.as_ref().unwrap().job.call_type,
            Self::Joint { prepared, .. } | Self::Ordinary { prepared, .. } => {
                prepared.as_ref().unwrap().job.call_type
            }
        }
    }
    fn take(&mut self) -> QueuedJob<R> {
        match self {
            Self::Legacy(row) => QueuedJob::Legacy(row.take().unwrap()),
            Self::Ordinary {
                prepared,
                reservation,
            } => QueuedJob::Ordinary(OrdinaryQueuedJob {
                prepared: prepared.take().unwrap(),
                reservation: reservation.take().unwrap().inner,
            }),
            Self::Joint { prepared, physical } => QueuedJob::Joint(JointQueuedJob {
                prepared: ManuallyDrop::new(prepared.take().unwrap()),
                physical: ManuallyDrop::new(physical.take().unwrap()),
            }),
        }
    }
}

/// A teardown panic carries the same guard into this worker's original join
/// entry. This is only the existing worker failure, not a new failure registry.
pub(crate) struct WorkerRetirementFailure {
    cause: ManuallyDrop<BoundedWorkerError>,
    notification: ManuallyDrop<[Option<BoundedWorkerError>; 2]>,
    physical: ManuallyDrop<Option<JointLocalGrant<super::QueryBlockingReservation>>>,
}
impl WorkerRetirementFailure {
    pub(crate) fn cause(&self) -> &BoundedWorkerError {
        &self.cause
    }
    pub(crate) fn notification(&self, index: usize) -> Option<&BoundedWorkerError> {
        self.notification.get(index).and_then(Option::as_ref)
    }
}
impl Drop for WorkerRetirementFailure {
    fn drop(&mut self) {
        // SAFETY: the original join owns each field exactly once. If any
        // failure payload destructor unwinds, later manual fields stay charged.
        unsafe {
            ManuallyDrop::drop(&mut self.cause);
            ManuallyDrop::drop(&mut self.notification);
            ManuallyDrop::drop(&mut self.physical);
        }
    }
}
struct WorkerEntry {
    join: Option<JoinHandle<()>>,
    failure: Option<super::QueryBlockingWorkerFailure>,
}

pub(crate) fn queue_record_layout<R>() -> std::alloc::Layout {
    std::alloc::Layout::new::<QueuedJob<R>>()
}
pub(crate) fn worker_entry_layout() -> std::alloc::Layout {
    std::alloc::Layout::new::<WorkerEntry>()
}

struct QueueState<R> {
    closed_factory: Option<TypeId>,
    closed_factory_queue_bytes: Option<std::num::NonZeroU64>,
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

/// Unique process-lifetime owner of the bounded worker worker threads.
///
/// Query work receives only [`BoundedWorkerHandle`]. Keeping the worker
/// join handles here prevents a query-scoped handle drop from closing or
/// joining the process executor.
pub(crate) struct BoundedWorkerOwner<R> {
    shared: Arc<Shared<R>>,
    workers: Mutex<Vec<WorkerEntry>>,
}

/// Cloneable submission handle for the process-owned bounded worker executor.
///
/// This handle deliberately has no edge to the owner or its join handles.
/// Dropping any or all handles therefore never closes the executor or blocks
/// while worker threads converge.
pub(crate) struct BoundedWorkerHandle<R> {
    shared: Arc<Shared<R>>,
}

impl<R> Clone for BoundedWorkerHandle<R> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<R: Send + 'static> fmt::Debug for BoundedWorkerHandle<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedWorkerHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl<R: Send + 'static> BoundedWorkerOwner<R> {
    pub(crate) fn try_new(config: BoundedWorkerConfig) -> Result<Self, BoundedWorkerOpenError> {
        let worker_threads = config.worker_threads().get();
        let queue_capacity = config.queue_capacity().get();
        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                closed_factory: None,
                closed_factory_queue_bytes: None,
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
                .name(format!("bounded-worker-{index}"))
                // Decoding a result walks a nested value as deep as the type
                // is, and these threads are workers like any other: the
                // process sizes its runtime workers deliberately, and one
                // spawned outside that runtime has to be sized the same way or
                // it aborts where the others would not.
                .stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                .spawn(move || run_worker(worker_shared))
            {
                Ok(worker) => {
                    shared
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .live_workers += 1;
                    workers.push(WorkerEntry {
                        join: Some(worker),
                        failure: None,
                    });
                }
                Err(error) => {
                    close_queue(&shared);
                    for mut worker in workers {
                        if let Some(join) = worker.join.take() {
                            let _ = join.join();
                        }
                    }
                    return Err(BoundedWorkerOpenError {
                        detail: format!("spawn bounded worker worker failed: {error}").into(),
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

impl<R> BoundedWorkerOwner<R> {
    pub(crate) fn handle(&self) -> BoundedWorkerHandle<R> {
        BoundedWorkerHandle {
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
    #[cfg(test)]
    pub(crate) fn shutdown_and_join(mut self) -> Result<(), BoundedWorkerShutdownError> {
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
    ) -> Result<(), BoundedWorkerShutdownError> {
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
            return Err(BoundedWorkerShutdownError::DeadlineExceeded { live_workers });
        }
        self.join_finished_workers()
    }

    #[cfg(test)]
    fn shutdown_and_join_inner(&mut self) -> Result<(), BoundedWorkerShutdownError> {
        close_queue(&self.shared);
        self.join_finished_workers()
    }

    pub(crate) fn take_worker_failure(&mut self) -> Option<super::QueryBlockingWorkerFailure> {
        let workers = self.workers.get_mut().unwrap_or_else(|e| e.into_inner());
        let failure = workers.iter_mut().find_map(|worker| worker.failure.take());
        workers.retain(|worker| worker.join.is_some() || worker.failure.is_some());
        failure
    }

    fn join_finished_workers(&mut self) -> Result<(), BoundedWorkerShutdownError> {
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        let mut panicked_workers = 0;
        for worker in workers.iter_mut() {
            if let Some(join) = worker.join.take()
                && let Err(original) = join.join()
            {
                worker.failure = Some(super::QueryBlockingWorkerFailure::new(original));
            }
            panicked_workers += usize::from(worker.failure.is_some());
        }
        // Successful joins leave no source. Failed entries retain the original
        // opaque payload/guard until the process owner explicitly takes them.
        workers.retain(|worker| worker.failure.is_some());
        if panicked_workers == 0 {
            Ok(())
        } else {
            Err(BoundedWorkerShutdownError::WorkerPanicked { panicked_workers })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundedWorkerShutdownError {
    DeadlineExceeded { live_workers: usize },
    WorkerPanicked { panicked_workers: usize },
}

impl fmt::Display for BoundedWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded { live_workers } => write!(
                formatter,
                "bounded worker shutdown deadline exceeded with {live_workers} live worker(s)"
            ),
            Self::WorkerPanicked { panicked_workers } => write!(
                formatter,
                "{panicked_workers} bounded worker worker(s) panicked during shutdown"
            ),
        }
    }
}

impl std::error::Error for BoundedWorkerShutdownError {}

impl<R: Send + 'static> BoundedWorkerHandle<R> {
    /// Waits only for bounded queue admission. Once this returns a receipt,
    /// the worker queue owns the job and dropping the caller future or receipt
    /// cannot cancel it.
    pub(crate) async fn submit(
        &self,
        job: BoundedWorkerJob<R>,
    ) -> Result<BoundedWorkerReceipt<R>, BoundedWorkerSubmitError<R>> {
        if !self.accepts_call_type(job.call_type) {
            return Err(BoundedWorkerSubmitError::rejected(job));
        }
        let queue_slot = match Arc::clone(&self.shared.queue_slots).acquire_owned().await {
            Ok(slot) => slot,
            Err(_) => return Err(BoundedWorkerSubmitError::rejected(job)),
        };
        self.enqueue(job, queue_slot)
    }

    /// Attempts bounded queue admission without waiting for a queue position.
    /// A full or closed executor returns the same unexecuted move-only job.
    /// This is only the physical queue seam; it grants no workload capacity.
    #[cfg(test)]
    pub(crate) fn try_submit(
        &self,
        job: BoundedWorkerJob<R>,
    ) -> Result<BoundedWorkerReceipt<R>, BoundedWorkerSubmitError<R>> {
        if !self.accepts_call_type(job.call_type) {
            return Err(BoundedWorkerSubmitError::rejected(job));
        }
        let queue_slot = match Arc::clone(&self.shared.queue_slots).try_acquire_owned() {
            Ok(slot) => slot,
            Err(_) => return Err(BoundedWorkerSubmitError { job }),
        };
        self.enqueue(job, queue_slot)
    }

    pub(crate) fn bind_internal_result_closed_factory_for_composition(
        &self,
        call: TypeId,
        physical_queue_bytes: std::num::NonZeroU64,
    ) -> Result<
        novarocks_workload_control::joint_admission::InternalResultClosedFactoryDescriptor,
        novarocks_workload_control::WorkError,
    > {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed
            || state.closed_factory.is_some()
            || state.running != 0
            || !state.jobs.is_empty()
            || self.shared.queue_slots.available_permits() != self.shared.queue_capacity
        {
            return Err(novarocks_workload_control::WorkError::Conflict);
        }
        state.closed_factory = Some(call);
        state.closed_factory_queue_bytes = Some(physical_queue_bytes);
        Ok(novarocks_workload_control::joint_admission::InternalResultClosedFactoryDescriptor::for_composition(self.source(), call, physical_queue_bytes))
    }
    pub(crate) fn closed_factory_descriptor(
        &self,
    ) -> Option<novarocks_workload_control::joint_admission::InternalResultClosedFactoryDescriptor>
    {
        let state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        let call = state.closed_factory?;
        let physical_queue_bytes = state.closed_factory_queue_bytes?;
        Some(novarocks_workload_control::joint_admission::InternalResultClosedFactoryDescriptor::for_composition(self.source(), call, physical_queue_bytes))
    }
    pub(crate) fn accepts_call_type(&self, call: TypeId) -> bool {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed_factory
            .is_none_or(|registered| registered == call)
    }
    fn source(&self) -> Weak<dyn Send + Sync> {
        let source: Arc<dyn Send + Sync> = self.shared.clone();
        Arc::downgrade(&source)
    }
    pub(crate) fn try_reserve(&self) -> Result<BoundedWorkerReservation, QueueReserveRefusal> {
        let queue_slot = Arc::clone(&self.shared.queue_slots)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => QueueReserveRefusal::Closed,
                tokio::sync::TryAcquireError::NoPermits => QueueReserveRefusal::Full,
            })?;
        let closed = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed;
        if closed {
            return Err(QueueReserveRefusal::Closed);
        }
        Ok(BoundedWorkerReservation {
            source: self.source(),
            queue_slot: Some(queue_slot),
        })
    }

    /// Wait in the same physical queue used by ordinary submissions. Dropping
    /// this future withdraws its semaphore waiter; it does not create a job.
    pub(crate) async fn reserve(&self) -> Result<BoundedWorkerReservation, QueueReserveRefusal> {
        let queue_slot = Arc::clone(&self.shared.queue_slots)
            .acquire_owned()
            .await
            .map_err(|_| QueueReserveRefusal::Closed)?;
        if self
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed
        {
            return Err(QueueReserveRefusal::Closed);
        }
        Ok(BoundedWorkerReservation {
            source: self.source(),
            queue_slot: Some(queue_slot),
        })
    }
    pub(crate) fn same_pool(&self, reservation: &BoundedWorkerReservation) -> bool {
        Weak::ptr_eq(&reservation.source, &self.source())
    }
    pub(crate) fn prepare(
        &self,
        job: BoundedWorkerJob<R>,
    ) -> (
        BoundedWorkerPrepared<R>,
        BoundedWorkerReceipt<R>,
        Arc<BoundedWorkerActualExit>,
    ) {
        let (completion, receiver) = oneshot::channel();
        let exit = Arc::new(BoundedWorkerActualExit::new());
        (
            BoundedWorkerPrepared {
                source: self.source(),
                job,
                completion,
                exit: Arc::clone(&exit),
            },
            BoundedWorkerReceipt { receiver },
            exit,
        )
    }
    pub(crate) fn publish_joint_in(
        &self,
        prepared: &mut Option<BoundedWorkerPrepared<R>>,
        physical: &mut Option<JointLocalGrant<super::QueryBlockingReservation>>,
    ) -> Result<BoundedWorkerPublication<R>, QueuePublishRefusal> {
        let Some(job) = prepared.as_ref() else {
            return Err(QueuePublishRefusal::Empty);
        };
        let Some(guard) = physical.as_mut() else {
            return Err(QueuePublishRefusal::Empty);
        };
        let source = self.source();
        if !Weak::ptr_eq(&job.source, &source)
            || !Weak::ptr_eq(&guard.physical_mut().inner.source, &source)
        {
            return Err(QueuePublishRefusal::ForeignPool);
        }
        match self.closed_factory_descriptor() {
            Some(descriptor) if !guard.matches_internal_result_closed_factory(&descriptor) => {
                return Err(QueuePublishRefusal::FactoryMismatch);
            }
            None if guard.is_internal_result_closed_factory() => {
                return Err(QueuePublishRefusal::FactoryMismatch);
            }
            _ => {}
        }
        if job.completion.is_closed() {
            return Err(QueuePublishRefusal::ReceiptClosed);
        }
        self.publish_record(&mut PendingRecord::Joint { prepared, physical })
    }
    pub(crate) fn publish_ordinary_in(
        &self,
        prepared: &mut Option<BoundedWorkerPrepared<R>>,
        reservation: &mut Option<super::QueryBlockingReservation>,
        call: TypeId,
    ) -> Result<BoundedWorkerPublication<R>, QueuePublishRefusal> {
        let job = prepared.as_ref().ok_or(QueuePublishRefusal::Empty)?;
        let reserved = reservation.as_ref().ok_or(QueuePublishRefusal::Empty)?;
        let source = self.source();
        if !Weak::ptr_eq(&job.source, &source) || !Weak::ptr_eq(&reserved.inner.source, &source) {
            return Err(QueuePublishRefusal::ForeignPool);
        }
        if reserved.inner.queue_slot.is_none() {
            return Err(QueuePublishRefusal::Empty);
        }
        if job.job.call_type != call {
            return Err(QueuePublishRefusal::FactoryMismatch);
        }
        if job.completion.is_closed() {
            return Err(QueuePublishRefusal::ReceiptClosed);
        }
        self.publish_record(&mut PendingRecord::Ordinary {
            prepared,
            reservation,
        })
    }
    /// The only FIFO publication author. Taking these closed enum fields runs
    /// no callback, allocation or destructor under QueueState.
    fn publish_record(
        &self,
        pending: &mut PendingRecord<'_, R>,
    ) -> Result<BoundedWorkerPublication<R>, QueuePublishRefusal> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(QueuePublishRefusal::Closed);
        }
        // Ordinary publication cannot borrow a finite factory's stamp, even
        // when its erased call happens to have the same Rust type.
        if matches!(pending, PendingRecord::Ordinary { .. }) && state.closed_factory.is_some() {
            return Err(QueuePublishRefusal::FactoryMismatch);
        }
        if state
            .closed_factory
            .is_some_and(|call| call != pending.call_type())
        {
            return Err(QueuePublishRefusal::FactoryMismatch);
        }
        debug_assert!(state.jobs.len() < self.shared.queue_capacity);
        state.jobs.push_back(pending.take());
        drop(state);
        Ok(BoundedWorkerPublication {
            shared: Arc::clone(&self.shared),
        })
    }
    /// Legacy admission retains its original result and dequeue-release timing.
    fn enqueue(
        &self,
        job: BoundedWorkerJob<R>,
        queue_slot: OwnedSemaphorePermit,
    ) -> Result<BoundedWorkerReceipt<R>, BoundedWorkerSubmitError<R>> {
        let (completion, receiver) = oneshot::channel();
        let mut row = Some(LegacyQueuedJob {
            job,
            completion,
            queue_slot,
        });
        match self.publish_record(&mut PendingRecord::Legacy(&mut row)) {
            Ok(publication) => {
                publication.notify();
                Ok(BoundedWorkerReceipt { receiver })
            }
            Err(_) => {
                let LegacyQueuedJob { job, .. } = row.take().unwrap();
                Err(BoundedWorkerSubmitError::rejected(job))
            }
        }
    }

    pub(crate) fn snapshot(&self) -> BoundedWorkerSnapshot {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        BoundedWorkerSnapshot {
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
        let (queued, cancelling) = {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(queued) = state.jobs.pop_front() {
                    state.running += 1;
                    break (queued, state.closed);
                }
                if state.closed {
                    return;
                }
                state = shared.ready.wait(state).unwrap_or_else(|e| e.into_inner());
            }
        };
        let (job, completion, physical, exit) = match queued {
            QueuedJob::Legacy(LegacyQueuedJob {
                job,
                completion,
                queue_slot,
            }) => {
                // Preserve the legacy queue-only reservation semantics.
                drop(queue_slot);
                (job, completion, None, None)
            }
            QueuedJob::Ordinary(OrdinaryQueuedJob {
                prepared,
                mut reservation,
            }) => {
                let BoundedWorkerPrepared {
                    job,
                    completion,
                    exit,
                    ..
                } = prepared;
                // Same queue-only lifetime as the ordinary executor. The
                // independent exit header still covers the actual call and
                // result/destructor path on this original worker.
                drop(reservation.queue_slot.take());
                (job, completion, None, Some(exit))
            }
            QueuedJob::Joint(row) => {
                let mut row = ManuallyDrop::new(row);
                // SAFETY: suppress the row destructor and move each initialized
                // original field exactly once into this same worker's custody.
                let (prepared, mut guard) = unsafe {
                    (
                        ManuallyDrop::take(&mut row.prepared),
                        ManuallyDrop::take(&mut row.physical),
                    )
                };
                let BoundedWorkerPrepared {
                    job,
                    completion,
                    exit,
                    ..
                } = prepared;
                // The original FIFO no longer retains this queue position.
                // Its separate execution/source guard remains through actual
                // call/result teardown; a running job is bounded by workers.
                drop(guard.physical_mut().inner.queue_slot.take());
                (job, completion, Some(guard), Some(exit))
            }
        };
        let mut physical = physical;
        let cleanup = if cancelling && exit.is_some() {
            // The original worker disposes an unexecuted received call. Any
            // joint guard stays outside this catch and cannot unwind away.
            match catch_unwind(AssertUnwindSafe(|| drop(job))) {
                Ok(()) => catch_unwind(AssertUnwindSafe(|| {
                    if let Err(original) = completion.send(Err(BoundedWorkerError::ExecutorClosed))
                    {
                        drop(original);
                    }
                })),
                Err(original) => {
                    let cause = BoundedWorkerError::panicked(original);
                    let notification = catch_unwind(AssertUnwindSafe(|| {
                        let _ = completion.send(Err(cause.clone()));
                    }))
                    .err()
                    .map(BoundedWorkerError::panicked);
                    retire_failed_worker(
                        &shared,
                        physical.take(),
                        exit.as_deref(),
                        cause,
                        notification,
                    );
                }
            }
        } else {
            let outcome =
                catch_unwind(AssertUnwindSafe(|| job.run())).map_err(BoundedWorkerError::panicked);
            catch_unwind(AssertUnwindSafe(|| {
                if let Err(undelivered) = completion.send(outcome) {
                    drop(undelivered);
                }
            }))
        };
        if let Err(original) = cleanup {
            if exit.is_some() {
                retire_failed_worker(
                    &shared,
                    physical.take(),
                    exit.as_deref(),
                    BoundedWorkerError::panicked(original),
                    None,
                );
            }
            // Legacy teardown panic keeps its original raw thread payload.
            std::panic::resume_unwind(original);
        }
        {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.running -= 1;
        }
        // The call, result-send and any undelivered destructor have returned,
        // and the original running record is gone. No owner lock is held.
        if let Err(original) = catch_unwind(AssertUnwindSafe(|| drop(physical.take()))) {
            retire_failed_worker_after_running(
                &shared,
                None,
                exit.as_deref(),
                BoundedWorkerError::panicked(original),
                None,
            );
        }
        if let Some(exit) = exit
            && let Some(original) = exit.publish(1, None)
        {
            retire_failed_worker_after_running(
                &shared,
                None,
                Some(&exit),
                BoundedWorkerError::panicked(original),
                None,
            );
        }
    }
}
fn retire_failed_worker<R>(
    shared: &Shared<R>,
    physical: Option<JointLocalGrant<super::QueryBlockingReservation>>,
    exit: Option<&BoundedWorkerActualExit>,
    cause: BoundedWorkerError,
    notification: Option<BoundedWorkerError>,
) -> ! {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.running -= 1;
    }
    retire_failed_worker_after_running(shared, physical, exit, cause, notification)
}
fn retire_failed_worker_after_running<R>(
    shared: &Shared<R>,
    physical: Option<JointLocalGrant<super::QueryBlockingReservation>>,
    exit: Option<&BoundedWorkerActualExit>,
    cause: BoundedWorkerError,
    notification: Option<BoundedWorkerError>,
) -> ! {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
    }
    shared.queue_slots.close();
    shared.ready.notify_all();
    let secondary = exit
        .and_then(|exit| exit.publish(2, Some(&cause)))
        .map(BoundedWorkerError::panicked);
    // Three fixed opaque carriers at most: this cause, the original result
    // notification, and the independent actual-exit observer notification.
    let failure = WorkerRetirementFailure {
        cause: ManuallyDrop::new(cause),
        notification: ManuallyDrop::new([notification, secondary]),
        physical: ManuallyDrop::new(physical),
    };
    std::panic::resume_unwind(Box::new(failure));
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
    let discarded = {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        let count = state.jobs.len();
        let legacy_count = state
            .jobs
            .iter()
            .filter(|row| matches!(row, QueuedJob::Legacy(_)))
            .count();
        let mut legacy = Vec::with_capacity(legacy_count);
        for _ in 0..count {
            let row = state.jobs.pop_front().unwrap();
            match row {
                QueuedJob::Legacy(row) => legacy.push(row),
                // Same original FIFO/storage: workers perform the cancelled
                // joint call's real destruction under its physical guard.
                QueuedJob::Joint(row) => state.jobs.push_back(QueuedJob::Joint(row)),
                QueuedJob::Ordinary(row) => state.jobs.push_back(QueuedJob::Ordinary(row)),
            }
        }
        legacy
    };
    // Closing the original semaphore can wake foreign producer Wakers.
    // Both that notification and actual input destruction are lock-free.
    shared.queue_slots.close();
    shared.ready.notify_all();
    drop(discarded);
}

impl<R> Drop for BoundedWorkerOwner<R> {
    fn drop(&mut self) {
        close_queue(&self.shared);
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        reap_workers(std::mem::take(workers));
    }
}

type WorkerSet = Vec<WorkerEntry>;

/// The fallback path cannot report worker panics, but it must still keep thread
/// convergence away from arbitrary async/runtime destruction paths. A single
/// process reaper owns every fallback join. If the reaper itself cannot be
/// created, dropping the join handles safely detaches the already-closing
/// workers instead of blocking the owner destructor.
fn reap_workers(workers: WorkerSet) {
    if workers.is_empty() {
        return;
    }
    static REAPER: OnceLock<Option<mpsc::Sender<WorkerSet>>> = OnceLock::new();
    let reaper = REAPER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<WorkerSet>();
        thread::Builder::new()
            .name("bounded-worker-reaper".to_string())
            .spawn(move || {
                while let Ok(workers) = receiver.recv() {
                    for mut worker in workers {
                        if let Some(join) = worker.join.take() {
                            let _ = join.join();
                        }
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
    ) -> (BoundedWorkerOwner<R>, BoundedWorkerHandle<R>) {
        let owner = BoundedWorkerOwner::try_new(BoundedWorkerConfig::new(
            NonZeroUsize::new(workers).unwrap(),
            NonZeroUsize::new(queue).unwrap(),
        ))
        .unwrap();
        let handle = owner.handle();
        (owner, handle)
    }

    async fn wait_for(supervisor: &BoundedWorkerHandle<usize>, queued: usize, running: usize) {
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
            .submit(BoundedWorkerJob::new(move || {
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
                    .submit(BoundedWorkerJob::new(move || {
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
        let mut fifth = Box::pin(supervisor.submit(BoundedWorkerJob::new(|| 4)));
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
            .submit(BoundedWorkerJob::new(move || {
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
        let queued = supervisor
            .submit(BoundedWorkerJob::new(|| 2))
            .await
            .unwrap();
        wait_for(&supervisor, 1, 1).await;
        let (dropped, observed_drop) = mpsc::channel();
        let notice = DropNotice {
            dropped: Some(dropped),
        };
        let mut waiting = Box::pin(supervisor.submit(BoundedWorkerJob::new(move || {
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
            supervisor.submit(BoundedWorkerJob::new(|| 4)),
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
            .submit(BoundedWorkerJob::new(move || {
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
            .submit(BoundedWorkerJob::new(move || {
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
            replacement.submit(BoundedWorkerJob::new(|| 2)),
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
            .submit(BoundedWorkerJob::new(move || {
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
            .submit(BoundedWorkerJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                1
            }))
            .await
            .unwrap();
        receive(&observed_start).await;
        let queued = handle.submit(BoundedWorkerJob::new(|| 2)).await.unwrap();
        wait_for(&handle, 1, 1).await;

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            release.send(()).unwrap();
        });
        let drop_started = Instant::now();
        drop(owner);
        assert!(
            drop_started.elapsed() < Duration::from_millis(300),
            "fallback owner Drop synchronously waited for a running worker job"
        );
        assert_eq!(tokio::spawn(async { 7 }).await.unwrap(), 7);
        assert_eq!(
            queued.complete().await,
            Err(BoundedWorkerError::ExecutorClosed)
        );
        match tokio::time::timeout(
            Duration::from_secs(1),
            handle.submit(BoundedWorkerJob::new(|| 3)),
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
    async fn shutdown_deadline_retains_the_same_workers_for_retry() {
        let (mut owner, handle) = executor::<usize>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, observed_start) = mpsc::channel();
        let running = handle
            .submit(BoundedWorkerJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                1
            }))
            .await
            .unwrap();
        receive(&observed_start).await;

        assert_eq!(
            owner.shutdown_until(Instant::now()).await,
            Err(BoundedWorkerShutdownError::DeadlineExceeded { live_workers: 1 })
        );
        match handle.submit(BoundedWorkerJob::new(|| 2)).await {
            Ok(_) => panic!("worker shutdown must keep intake closed across retry"),
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
            .block_on(handle.submit(BoundedWorkerJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                drop(running_handle);
                1
            })))
            .unwrap();
        observed_start.recv_timeout(Duration::from_secs(1)).unwrap();
        let queued = runtime
            .block_on(handle.submit(BoundedWorkerJob::new(move || {
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
            .block_on(supervisor.submit(BoundedWorkerJob::new(move || {
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

    #[tokio::test(flavor = "current_thread")]
    async fn try_submit_full_returns_the_original_job_without_running_or_dropping_it() {
        let (mut owner, handle) = executor::<usize>(1, 1);
        let (release, released) = mpsc::channel();
        let (started, observed_start) = mpsc::channel();
        let running = handle
            .try_submit(BoundedWorkerJob::new(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                1
            }))
            .unwrap();
        receive(&observed_start).await;
        // The asynchronous entry must consume the very same physical queue.
        let queued = handle.submit(BoundedWorkerJob::new(|| 2)).await.unwrap();
        let runs = Arc::new(AtomicUsize::new(0));
        let job_runs = Arc::clone(&runs);
        let (dropped, observed_drop) = mpsc::channel();
        let notice = DropNotice {
            dropped: Some(dropped),
        };
        let payload = String::from("same move-only application input");
        let refused = handle
            .try_submit(BoundedWorkerJob::new(move || {
                assert_eq!(payload, "same move-only application input");
                job_runs.fetch_add(1, Ordering::SeqCst);
                drop(notice);
                3
            }))
            .unwrap_err();
        // This call returned synchronously while both existing jobs remain
        // gated. No timeout or scheduler turn creates room for the refusal.
        assert_eq!(
            (handle.snapshot().queued, handle.snapshot().running),
            (1, 1)
        );
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(observed_drop.try_recv(), Err(mpsc::TryRecvError::Empty));
        let original = refused.into_job();
        release.send(()).unwrap();
        assert_eq!(running.complete().await.unwrap(), 1);
        assert_eq!(queued.complete().await.unwrap(), 2);
        let retried = handle.try_submit(original).unwrap();
        assert_eq!(retried.complete().await.unwrap(), 3);
        receive(&observed_drop).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn try_submit_closed_returns_untouched_job_and_drop_stays_with_the_caller() {
        let (mut owner, handle) = executor::<usize>(1, 1);
        owner.request_close();
        let runs = Arc::new(AtomicUsize::new(0));
        let job_runs = Arc::clone(&runs);
        let (dropped, observed_drop) = mpsc::channel();
        let notice = DropNotice {
            dropped: Some(dropped),
        };
        let error = handle
            .try_submit(BoundedWorkerJob::new(move || {
                job_runs.fetch_add(1, Ordering::SeqCst);
                drop(notice);
                7
            }))
            .unwrap_err();
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(observed_drop.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(handle.snapshot().queued, 0);
        drop(error.into_job());
        assert_eq!(receive(&observed_drop).await, thread::current().id());
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_between_original_position_acquisition_and_enqueue_returns_the_job() {
        let (mut owner, handle) = executor::<usize>(1, 1);
        // This is the actual semaphore position used by both public entry
        // bodies, not a second queue or an injected queue-state flag.
        let slot = Arc::clone(&handle.shared.queue_slots)
            .try_acquire_owned()
            .unwrap();
        owner.request_close();
        let runs = Arc::new(AtomicUsize::new(0));
        let job_runs = Arc::clone(&runs);
        let (dropped, observed_drop) = mpsc::channel();
        let notice = DropNotice {
            dropped: Some(dropped),
        };
        let error = handle
            .enqueue(
                BoundedWorkerJob::new(move || {
                    job_runs.fetch_add(1, Ordering::SeqCst);
                    drop(notice);
                    7
                }),
                slot,
            )
            .unwrap_err();
        assert_eq!(handle.snapshot().queued, 0);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(observed_drop.try_recv(), Err(mpsc::TryRecvError::Empty));
        drop(error.into_job());
        assert_eq!(receive(&observed_drop).await, thread::current().id());
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(owner.shared.state.lock().unwrap().live_workers, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn original_try_submit_and_close_race_have_one_job_owner_and_one_execution_verdict() {
        for _ in 0..16 {
            let (mut owner, handle) = executor::<usize>(1, 1);
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let submit_barrier = Arc::clone(&barrier);
            let runs = Arc::new(AtomicUsize::new(0));
            let job_runs = Arc::clone(&runs);
            let (dropped, observed_drop) = mpsc::channel();
            let notice = DropNotice {
                dropped: Some(dropped),
            };
            let submitter = thread::spawn(move || {
                submit_barrier.wait();
                handle.try_submit(BoundedWorkerJob::new(move || {
                    job_runs.fetch_add(1, Ordering::SeqCst);
                    drop(notice);
                    7
                }))
            });
            barrier.wait();
            owner.request_close();
            match submitter.join().unwrap() {
                Ok(receipt) => match receipt.complete().await {
                    Ok(value) => {
                        assert_eq!(value, 7);
                        assert_eq!(runs.load(Ordering::SeqCst), 1);
                    }
                    Err(BoundedWorkerError::ExecutorClosed) => {
                        assert_eq!(runs.load(Ordering::SeqCst), 0);
                    }
                    Err(other) => panic!("unexpected worker verdict: {other}"),
                },
                Err(error) => {
                    assert_eq!(runs.load(Ordering::SeqCst), 0);
                    assert_eq!(observed_drop.try_recv(), Err(mpsc::TryRecvError::Empty));
                    drop(error.into_job());
                }
            }
            receive(&observed_drop).await;
            assert_eq!(
                observed_drop.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            );
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(owner.shared.state.lock().unwrap().live_workers, 0);
            assert!(owner.workers.lock().unwrap().is_empty());
        }
    }
}
