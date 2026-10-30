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

//! Bounded, single-owner execution for connector writers.
//!
//! The driver owns only this mailbox handle. The provider writer is opened and
//! then exclusively owned by one `sink_io` task for its complete lifecycle, so
//! no provider future is polled on a driver thread and no writer mutex exists.

#[cfg(test)]
use crate::exec::pipeline::operator::ExitProgress;
use crate::exec::pipeline::operator::{ExitFailure, ExitObservation, FinishWatch};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution,
};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::exec::chunk::{ChunkBatchProjection, ChunkMemoryLease, TransferredChunkBytes};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::writer_failure::{WriterCommandOperation, WriterFailure};
use crate::runtime::execution_services::IoExecutor;
use crate::runtime::fragment::connector_failure::{ConnectorCallFailure, ConnectorOperation};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_execution_contract::status::SafeDetail;
use novarocks_failure_contract::BoundedDetail;
use novarocks_failure_contract::ResourceDimension;
use novarocks_type_contract::ValueTypeStorageLifetime;

/// Limits one writer's retained input. A single input page must fit both
/// per-page limits. `need_input` reserves against the worst legal next page, so
/// it remains an exact admission predicate even though the pipeline contract
/// does not pass that next page to it for inspection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AsyncWriterQueueConfig {
    pub(crate) max_batches: usize,
    pub(crate) max_rows: usize,
    pub(crate) max_bytes: usize,
    pub(crate) max_batch_rows: usize,
    pub(crate) max_batch_bytes: usize,
    pub(crate) abort_timeout: Duration,
}

impl Default for AsyncWriterQueueConfig {
    fn default() -> Self {
        Self {
            max_batches: 4,
            max_rows: 4 * 1_048_576,
            max_bytes: 64 * 1024 * 1024,
            max_batch_rows: 1_048_576,
            max_batch_bytes: 16 * 1024 * 1024,
            abort_timeout: Duration::from_secs(5),
        }
    }
}

/// The queued provider copy always travels with its original typed projection.
/// It retains only immutable batch/type loans and this copy's finite storage.
pub(crate) struct WriterAppendBatch {
    batch: RecordBatch,
    projection: ChunkBatchProjection,
    construction: ValueTypeStorageLifetime,
}
impl WriterAppendBatch {
    pub(crate) fn try_new(
        projection: ChunkBatchProjection,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, ConstructionFailure> {
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                units: projection.columns().len(),
            },
        )?;
        // RecordBatch::clone shares Schema/Array Arcs but clones its actual
        // Vec<ArrayRef> backing. Quote that same operation before the clone.
        owner.request(std::alloc::Layout::array::<arrow::array::ArrayRef>(
            projection.columns().len(),
        ).map_err(|_| novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?)?;
        let batch = projection.record_batch().clone();
        Ok(Self {
            batch,
            projection,
            construction: owner.storage_lifetime(),
        })
    }

    // Original accounting tests explicitly author expansion fixtures. This is
    // never a production alternative to the actual constructor above.
    #[cfg(test)]
    pub(crate) fn from_fixture_projection(projection: ChunkBatchProjection) -> Self {
        Self {
            batch: projection.record_batch().clone(),
            projection,
            construction: ValueTypeStorageLifetime::pure(),
        }
    }
}

struct AppendCommand {
    batch: WriterAppendBatch,
    rows: usize,
    bytes: usize,
    _transferred_accounting: Option<TransferredChunkBytes>,
    _additional_accounting: Option<TrackedBytes>,
}

enum WriterCommand {
    Append(Box<AppendCommand>),
    Finish,
}

type FinishMapper<O> =
    Box<dyn FnMut(u64, &[ConnectorCommitFragment]) -> ExecutionResult<O> + Send + 'static>;

struct WriterStart<O> {
    execution: Arc<dyn ConnectorWriteExecution>,
    request: ConnectorOpenWriterRequest,
    receiver: mpsc::Receiver<WriterCommand>,
    finish_mapper: FinishMapper<O>,
}

struct WriterShared<O> {
    observable: Arc<Observable>,
    abort_notify: Notify,
    queue_usage: Mutex<QueueUsage>,
    finish_requested: AtomicBool,
    abort_requested: AtomicBool,
    done: AtomicBool,
    terminal_result_produced: AtomicBool,
    result: Mutex<Option<O>>,
    error: Mutex<Option<ExecutionFailure>>,
    cleanup_diagnostics: Mutex<Vec<SafeDetail>>,
    // One returned typed abort failure; separate from the execution origin.
    abort_failure: Mutex<Option<ExecutionFailure>>,
    queue_tracker: Mutex<Option<Arc<MemTracker>>>,
    queue_peak_batches: AtomicUsize,
    queue_peak_rows: AtomicUsize,
    queue_peak_bytes: AtomicUsize,
    queue_blocked_checks: AtomicUsize,
    abort_requests: AtomicUsize,
    phase: AtomicUsize,
    unresolved: AtomicBool,
    frame_exited: AtomicBool,
    panic_primary: Mutex<[Option<std::mem::ManuallyDrop<WriterPanicPayload>>; WRITER_PANIC_CELLS]>,
    panic_payloads: Mutex<[Option<std::mem::ManuallyDrop<WriterPanicPayload>>; WRITER_PANIC_CELLS]>,
    notification_failed: [AtomicBool; WRITER_NOTIFICATIONS],
    notification_primary:
        Mutex<[Option<std::mem::ManuallyDrop<WriterPanicPayload>>; WRITER_NOTIFICATIONS]>,
    notification_secondary:
        Mutex<[Option<std::mem::ManuallyDrop<WriterPanicPayload>>; WRITER_NOTIFICATIONS]>,
    unknown_actor: Mutex<Option<std::mem::ManuallyDrop<WriterActorFuture<O>>>>,
    unknown_output: Mutex<Option<std::mem::ManuallyDrop<Option<O>>>>,
    unknown_self: Mutex<Option<Arc<WriterShared<O>>>>,
    #[cfg(test)]
    empty_result_observed: std::sync::OnceLock<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct QueueUsage {
    batches: usize,
    rows: usize,
    bytes: usize,
}

impl<O> WriterShared<O> {
    fn new() -> Self {
        Self {
            observable: Arc::new(Observable::new()),
            abort_notify: Notify::new(),
            queue_usage: Mutex::new(QueueUsage::default()),
            finish_requested: AtomicBool::new(false),
            abort_requested: AtomicBool::new(false),
            done: AtomicBool::new(false),
            terminal_result_produced: AtomicBool::new(false),
            result: Mutex::new(None),
            error: Mutex::new(None),
            cleanup_diagnostics: Mutex::new(Vec::new()),
            abort_failure: Mutex::new(None),
            queue_tracker: Mutex::new(None),
            queue_peak_batches: AtomicUsize::new(0),
            queue_peak_rows: AtomicUsize::new(0),
            queue_peak_bytes: AtomicUsize::new(0),
            queue_blocked_checks: AtomicUsize::new(0),
            abort_requests: AtomicUsize::new(0),
            phase: AtomicUsize::new(WriterPhase::Open as usize),
            unresolved: AtomicBool::new(false),
            frame_exited: AtomicBool::new(false),
            panic_primary: Mutex::new(std::array::from_fn(|_| None)),
            panic_payloads: Mutex::new(std::array::from_fn(|_| None)),
            notification_failed: std::array::from_fn(|_| AtomicBool::new(false)),
            notification_primary: Mutex::new(std::array::from_fn(|_| None)),
            notification_secondary: Mutex::new(std::array::from_fn(|_| None)),
            unknown_actor: Mutex::new(None),
            unknown_output: Mutex::new(None),
            unknown_self: Mutex::new(None),
            #[cfg(test)]
            empty_result_observed: std::sync::OnceLock::new(),
        }
    }

    fn wake(&self) {
        self.observable.defer_notify().arm();
    }

    fn release_append(&self, rows: usize, bytes: usize) {
        let mut usage = self
            .queue_usage
            .lock()
            .expect("async writer queue usage lock");
        usage.batches = usage.batches.saturating_sub(1);
        usage.rows = usage.rows.saturating_sub(rows);
        usage.bytes = usage.bytes.saturating_sub(bytes);
        drop(usage);
        self.wake();
    }

    fn observe_queue_usage(&self, usage: QueueUsage) {
        self.queue_peak_batches
            .fetch_max(usage.batches, Ordering::Relaxed);
        self.queue_peak_rows
            .fetch_max(usage.rows, Ordering::Relaxed);
        self.queue_peak_bytes
            .fetch_max(usage.bytes, Ordering::Relaxed);
    }

    fn clear_queue_usage(&self) {
        *self
            .queue_usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = QueueUsage::default();
    }

    fn set_error(&self, error: ExecutionFailure, runtime_error: &RuntimeErrorState) {
        let first = {
            let mut guard = self.error.lock().expect("async writer error lock");
            if guard.is_none() {
                *guard = Some(error.clone());
                true
            } else {
                false
            }
        };
        // Both notifications can run synchronous observers. Never retain the
        // private writer lock across a runtime call or an observable callback.
        if first {
            self.notify(WriterNotification::Publication, runtime_error, &mut || {
                runtime_error.set_error(error.clone())
            });
        } else if self
            .error
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|original| !original.same_occurrence(&error))
        {
            self.record_cleanup_error(&error);
        }
        self.notify(WriterNotification::Readiness, runtime_error, &mut || {
            self.wake()
        });
    }

    fn record_cleanup_error(&self, error: &ExecutionFailure) {
        let mut diagnostics = self
            .cleanup_diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if diagnostics.len() < 4 {
            diagnostics.push(error.safe_detail());
        }
    }

    fn set_done(&self) {
        self.done.store(true, Ordering::Release);
        self.wake();
    }

    fn publish_terminal_success(&self, output: O) {
        let mut result = self.result.lock().expect("async writer result lock");
        debug_assert!(
            result.is_none(),
            "async writer publishes one terminal result"
        );
        *result = Some(output);
        // Result and semantic completion form one monotonic terminal
        // publication. The result slot is consumer-owned afterwards and can
        // legitimately be empty again while `done` remains true.
        self.terminal_result_produced.store(true, Ordering::Release);
        self.done.store(true, Ordering::Release);
        drop(result);
        self.wake();
    }

    fn ensure_terminal_outcome(&self, runtime_error: &RuntimeErrorState) {
        let has_error = self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some();
        if !self.abort_requested.load(Ordering::Acquire)
            && !self.terminal_result_produced.load(Ordering::Acquire)
            && !has_error
        {
            self.set_error(WriterFailure::MissingTerminalOutcome.into(), runtime_error);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AsyncWriterMetrics {
    pub(crate) queue_peak_batches: usize,
    pub(crate) queue_peak_rows: usize,
    pub(crate) queue_peak_bytes: usize,
    pub(crate) queue_blocked_checks: usize,
    pub(crate) abort_requests: usize,
}

/// Capacity, channel admission, and retained-memory ownership reserved before
/// a composite operator mutates any other child state.
pub(crate) struct AsyncWriterReservation<O: Send + 'static> {
    permit: Option<mpsc::OwnedPermit<WriterCommand>>,
    shared: Arc<WriterShared<O>>,
    rows: usize,
    bytes: usize,
    transferred_accounting: Option<TransferredChunkBytes>,
    additional_accounting: Option<TrackedBytes>,
    committed: bool,
}

impl<O: Send + 'static> AsyncWriterReservation<O> {
    pub(crate) fn send(mut self, batch: WriterAppendBatch) -> ExecutionResult<()> {
        if let Some(accounting) = self.transferred_accounting.as_mut() {
            accounting.release_unshared_connector_bytes()?;
        }
        let command = WriterCommand::Append(Box::new(AppendCommand {
            batch,
            rows: self.rows,
            bytes: self.bytes,
            _transferred_accounting: self.transferred_accounting.take(),
            _additional_accounting: self.additional_accounting.take(),
        }));
        self.permit
            .take()
            .expect("async writer reservation permit")
            .send(command);
        self.committed = true;
        Ok(())
    }
}

impl<O: Send + 'static> Drop for AsyncWriterReservation<O> {
    fn drop(&mut self) {
        if !self.committed {
            self.shared.release_append(self.rows, self.bytes);
        }
    }
}

/// Driver-side handle for one asynchronously owned connector writer.
pub(crate) struct AsyncWriterOwner<O: Send + 'static> {
    config: AsyncWriterQueueConfig,
    sender: Option<mpsc::Sender<WriterCommand>>,
    start: Option<WriterStart<O>>,
    shared: Arc<WriterShared<O>>,
    join: Option<JoinHandle<()>>,
    actor_exit: bool,
    runtime_error: Option<Arc<RuntimeErrorState>>,
}

impl<O: Send + 'static> AsyncWriterOwner<O> {
    pub(crate) fn new(
        execution: Arc<dyn ConnectorWriteExecution>,
        request: ConnectorOpenWriterRequest,
        config: AsyncWriterQueueConfig,
        finish_mapper: FinishMapper<O>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.max_batches.max(1).saturating_add(1));
        Self {
            config,
            sender: Some(sender),
            start: Some(WriterStart {
                execution,
                request,
                receiver,
                finish_mapper,
            }),
            shared: Arc::new(WriterShared::new()),
            join: None,
            actor_exit: false,
            runtime_error: None,
        }
    }

    pub(crate) fn set_mem_tracker(&self, tracker: Arc<MemTracker>) {
        *self
            .shared
            .queue_tracker
            .lock()
            .expect("async writer queue tracker lock") =
            Some(MemTracker::new_child("ConnectorWriterQueue", &tracker));
    }

    pub(crate) fn bind(
        &mut self,
        executor: IoExecutor,
        runtime_error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<()> {
        if self.join.is_some() {
            return Ok(());
        }
        let start = self.start.take().ok_or(WriterFailure::AlreadyBound)?;
        let shared = Arc::clone(&self.shared);
        let config = self.config;
        self.runtime_error = Some(runtime_error.clone());
        // Install the original guard before the executor can cancel an unpolled
        // task. Its receiver already owns every accepted queued command.
        let frame = WriterActorFrame::new(start, shared, config, runtime_error);
        self.join = Some(executor.spawn(supervise_actor(frame)));
        Ok(())
    }

    pub(crate) fn can_accept(&self) -> bool {
        if self.shared.unresolved.load(Ordering::Acquire)
            || self.shared.finish_requested.load(Ordering::Acquire)
            || self.shared.abort_requested.load(Ordering::Acquire)
            || self.shared.done.load(Ordering::Acquire)
        {
            return false;
        }
        let usage = self
            .shared
            .queue_usage
            .lock()
            .expect("async writer queue usage lock");
        let can_accept = usage.batches < self.config.max_batches
            && usage
                .rows
                .checked_add(self.config.max_batch_rows)
                .is_some_and(|rows| rows <= self.config.max_rows)
            && usage
                .bytes
                .checked_add(self.config.max_batch_bytes)
                .is_some_and(|bytes| bytes <= self.config.max_bytes);
        if !can_accept {
            self.shared
                .queue_blocked_checks
                .fetch_add(1, Ordering::Relaxed);
        }
        can_accept
    }

    #[cfg(test)]
    pub(crate) fn enqueue(&self, batch: RecordBatch, retained_bytes: usize) -> ExecutionResult<()> {
        self.enqueue_with_accounting(batch, retained_bytes, None, 0, retained_bytes)
    }

    #[cfg(test)]
    pub(crate) fn enqueue_with_accounting(
        &self,
        batch: RecordBatch,
        retained_bytes: usize,
        inherited_accounting: Option<ChunkMemoryLease>,
        connector_retained_bytes: usize,
        additional_bytes: usize,
    ) -> ExecutionResult<()> {
        let reservation = self.try_reserve_input(
            batch.num_rows(),
            retained_bytes,
            inherited_accounting,
            connector_retained_bytes,
            additional_bytes,
        )?;
        reservation.send(tests::fixture_append_batch(batch))
    }

    pub(crate) fn try_reserve_input(
        &self,
        rows: usize,
        retained_bytes: usize,
        inherited_accounting: Option<ChunkMemoryLease>,
        connector_retained_bytes: usize,
        additional_bytes: usize,
    ) -> ExecutionResult<AsyncWriterReservation<O>> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        if self.shared.unresolved.load(Ordering::Acquire) {
            return Err(WriterFailure::ExitUnresolved.into());
        }
        if rows > self.config.max_batch_rows {
            return Err(WriterFailure::InputBatchRows {
                rows,
                limit: self.config.max_batch_rows,
            }
            .into());
        }
        if retained_bytes > self.config.max_batch_bytes {
            return Err(WriterFailure::InputBatchBytes {
                bytes: retained_bytes,
                limit: self.config.max_batch_bytes,
            }
            .into());
        }
        {
            let mut usage = self
                .shared
                .queue_usage
                .lock()
                .expect("async writer queue usage lock");
            let refusal = if usage.batches >= self.config.max_batches {
                Some((
                    ResourceDimension::Items,
                    1,
                    usage.batches,
                    self.config.max_batches,
                ))
            } else if usage
                .rows
                .checked_add(rows)
                .is_none_or(|rows| rows > self.config.max_rows)
            {
                Some((
                    ResourceDimension::Positions,
                    rows,
                    usage.rows,
                    self.config.max_rows,
                ))
            } else if usage
                .bytes
                .checked_add(retained_bytes)
                .is_none_or(|bytes| bytes > self.config.max_bytes)
            {
                Some((
                    ResourceDimension::Bytes,
                    retained_bytes,
                    usage.bytes,
                    self.config.max_bytes,
                ))
            } else {
                None
            };
            if let Some((dimension, incoming, live, limit)) = refusal {
                return Err(WriterFailure::QueueCapacity {
                    dimension,
                    incoming,
                    live,
                    limit,
                }
                .into());
            }
            usage.batches += 1;
            usage.rows += rows;
            usage.bytes += retained_bytes;
            self.shared.observe_queue_usage(*usage);
        }
        if additional_bytes > retained_bytes {
            self.shared.release_append(rows, retained_bytes);
            return Err(WriterFailure::AdditionalBytes {
                additional: additional_bytes,
                retained: retained_bytes,
            }
            .into());
        }
        let queue_tracker = self
            .shared
            .queue_tracker
            .lock()
            .expect("async writer queue tracker lock")
            .clone();
        let tracker = queue_tracker.or_else(|| {
            inherited_accounting
                .as_ref()
                .and_then(ChunkMemoryLease::tracker)
        });
        let shared_projected_bytes = retained_bytes - additional_bytes;
        let transferred_accounting = match inherited_accounting.as_ref() {
            Some(accounting) => {
                match accounting.try_split_to(
                    tracker.as_ref(),
                    shared_projected_bytes,
                    connector_retained_bytes,
                ) {
                    Ok(accounting) => accounting,
                    Err(error) => {
                        self.shared.release_append(rows, retained_bytes);
                        return Err(error);
                    }
                }
            }
            None => None,
        };
        // An exclusive source lease transfers only buffers shared with the
        // projection. A non-exclusive lease falls back to per-batch Scheme S:
        // charge the complete retained projection because another live owner
        // still owns the source charge.
        let bytes_to_charge = if transferred_accounting.is_some() {
            additional_bytes
        } else {
            retained_bytes
        };
        let additional_accounting = match tracker {
            Some(tracker) => match TrackedBytes::try_new(bytes_to_charge, tracker) {
                Ok(accounting) => Some(accounting),
                Err(error) => {
                    self.shared.release_append(rows, retained_bytes);
                    return Err(error.into());
                }
            },
            None => None,
        };
        let Some(sender) = self.sender.as_ref().cloned() else {
            self.shared.release_append(rows, retained_bytes);
            return Err(self.error().unwrap_or_else(|| {
                WriterFailure::AfterTerminal {
                    operation: WriterCommandOperation::Append,
                }
                .into()
            }));
        };
        let permit = match sender.try_reserve_owned() {
            Ok(permit) => permit,
            Err(error) => {
                self.shared.release_append(rows, retained_bytes);
                return Err(self.command_error(WriterCommandOperation::Append, error));
            }
        };
        Ok(AsyncWriterReservation {
            permit: Some(permit),
            shared: Arc::clone(&self.shared),
            rows,
            bytes: retained_bytes,
            transferred_accounting,
            additional_accounting,
            committed: false,
        })
    }

    fn command_error<T>(
        &self,
        operation: WriterCommandOperation,
        error: mpsc::error::TrySendError<T>,
    ) -> ExecutionFailure {
        let observed = match error {
            mpsc::error::TrySendError::Full(_) => WriterFailure::MailboxFull { operation },
            mpsc::error::TrySendError::Closed(_) => WriterFailure::MailboxClosed { operation },
        };
        self.error().unwrap_or_else(|| observed.into())
    }
    pub(crate) fn request_finish(&mut self) -> ExecutionResult<()> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        if self.shared.finish_requested.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let Some(sender) = self.sender.take() else {
            return Err(WriterFailure::AfterTerminal {
                operation: WriterCommandOperation::Finish,
            }
            .into());
        };
        sender
            .try_send(WriterCommand::Finish)
            .map_err(|error| self.command_error(WriterCommandOperation::Finish, error))
    }

    pub(crate) fn request_abort(&mut self) {
        if self.shared.done.load(Ordering::Acquire)
            || self.shared.abort_requested.swap(true, Ordering::AcqRel)
        {
            return;
        }
        self.shared.abort_requests.fetch_add(1, Ordering::Relaxed);
        self.sender = None;
        if self.start.take().is_some() {
            // No actor was admitted. Dropping the unopened provider request is
            // already a complete abort and must not strand PendingFinish.
            self.shared.set_done();
            return;
        }
        self.shared.abort_notify.notify_one();
        self.shared.wake();
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.shared.observable)
    }

    pub(crate) fn is_done(&self) -> bool {
        self.shared.done.load(Ordering::Acquire)
    }

    /// A terminal result can be published before the sink-I/O task returns.
    /// Keep the task owned by the driver until its wrapper has actually exited.
    pub(crate) fn actor_exited(&self) -> bool {
        !self.shared.unresolved.load(Ordering::Acquire)
            && self.join.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Only the worker holding this owner consumes the task's actual outcome.
    /// A finished flag or an abort request cannot substitute for this join.
    pub(crate) fn observe_actor_exit(&mut self) -> ExitObservation {
        use std::{
            future::Future,
            task::{Context, Poll},
        };
        if self.shared.unresolved.load(Ordering::Acquire) {
            let mut custody = self
                .shared
                .unknown_self
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            custody.get_or_insert_with(|| self.shared.clone());
        }
        let watch = || FinishWatch::Notify(self.observable());
        if self.actor_exit {
            return if self.shared.unresolved.load(Ordering::Acquire) {
                ExitObservation::unresolved(watch())
            } else {
                ExitObservation::exited()
            };
        }
        let Some(join) = self.join.as_mut() else {
            return ExitObservation::exited();
        };
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        match std::pin::Pin::new(join).poll(&mut context) {
            Poll::Pending => {
                let watch = if self.is_done() {
                    FinishWatch::RecheckAfter(Duration::from_millis(1))
                } else {
                    FinishWatch::Notify(self.observable())
                };
                if self.shared.unresolved.load(Ordering::Acquire) {
                    ExitObservation::unresolved(watch)
                } else {
                    ExitObservation::pending(watch)
                }
            }
            Poll::Ready(result) => {
                self.join = None;
                let signal = self
                    .runtime_error
                    .clone()
                    .expect("a bound actor owns its exact original failure port");
                if let Err(error) = result {
                    let phase = self.shared.current_phase();
                    if self.shared.unresolved.load(Ordering::Acquire) {
                        if error.is_panic() {
                            self.shared
                                .save_panic(WriterPhase::Join, false, error.into_panic());
                            self.shared.dispose_panic(WriterPhase::Join, false, &signal);
                        }
                        self.actor_exit = true;
                        return ExitObservation::unresolved(FinishWatch::Notify(self.observable()));
                    }
                    if error.is_cancelled() && self.shared.frame_exited.load(Ordering::Acquire) {
                        self.actor_exit = true;
                        self.runtime_error = None;
                        return ExitObservation::exited().with_failure(ExitFailure::Cleanup(
                            WriterFailure::ActorInterrupted.into(),
                        ));
                    }
                    let failure = if error.is_panic() {
                        let failure =
                            self.shared
                                .save_panic(WriterPhase::Join, false, error.into_panic());
                        let physical_frame_exited =
                            self.shared.frame_exited.load(Ordering::Acquire);
                        self.shared.dispose_panic(WriterPhase::Join, false, &signal);
                        if physical_frame_exited && !self.shared.unresolved.load(Ordering::Acquire)
                        {
                            self.actor_exit = true;
                            self.runtime_error = None;
                            return ExitObservation::exited()
                                .with_failure(ExitFailure::Cleanup(failure));
                        }
                        self.shared.unknown(&failure, &signal);
                        failure
                    } else {
                        let failure: ExecutionFailure = WriterFailure::ActorInterrupted.into();
                        self.shared.unknown(&failure, &signal);
                        failure
                    };
                    let role = if phase.required() {
                        ExitFailure::Required(failure)
                    } else {
                        ExitFailure::Cleanup(failure)
                    };
                    self.actor_exit = true;
                    self.shared
                        .unknown_self
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get_or_insert_with(|| self.shared.clone());
                    // This fallback was not already reported by the actor.
                    return ExitObservation::unresolved(FinishWatch::Notify(self.observable()))
                        .with_failure(role);
                }
                self.actor_exit = true;
                if self.shared.unresolved.load(Ordering::Acquire) {
                    ExitObservation::unresolved(FinishWatch::Notify(self.observable()))
                } else {
                    self.runtime_error = None;
                    ExitObservation::exited()
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn install_terminal_actor_for_test(&mut self, handle: JoinHandle<()>) {
        assert!(
            self.join.is_none() && !self.actor_exit,
            "test actor must be installed only once"
        );
        self.join = Some(handle);
        self.runtime_error = Some(Arc::new(RuntimeErrorState::default()));
        self.shared.set_done();
    }

    pub(crate) fn has_output(&self) -> bool {
        self.shared
            .result
            .lock()
            .expect("async writer result lock")
            .is_some()
    }

    /// Observe and consume one terminal output at the publisher's result cut.
    /// An empty slot observed before publication is Pending, even when the
    /// actor publishes immediately after this method releases the result lock.
    pub(crate) fn poll_output(&self) -> ExecutionResult<std::task::Poll<O>> {
        let (output, done) = {
            let mut result = self.shared.result.lock().expect("async writer result lock");
            let output = result.take();
            #[cfg(test)]
            if output.is_none()
                && let Some(observed) = self.shared.empty_result_observed.get()
            {
                observed();
            }
            (output, self.shared.done.load(Ordering::Acquire))
        };
        // The result lock never covers error projection, a callback, or the
        // caller's output destruction. Preserve a cause published after the
        // driver's earlier error check rather than manufacturing a new one.
        match output {
            Some(output) => Ok(std::task::Poll::Ready(output)),
            None if done => Err(self
                .error()
                .unwrap_or_else(|| WriterFailure::MissingTerminalOutcome.into())),
            None => Ok(std::task::Poll::Pending),
        }
    }

    #[cfg(test)]
    pub(crate) fn take_output(&self) -> Option<O> {
        self.shared
            .result
            .lock()
            .expect("async writer result lock")
            .take()
    }

    pub(crate) fn error(&self) -> Option<ExecutionFailure> {
        self.shared
            .error
            .lock()
            .expect("async writer error lock")
            .clone()
    }

    pub(crate) fn metrics(&self) -> AsyncWriterMetrics {
        AsyncWriterMetrics {
            queue_peak_batches: self.shared.queue_peak_batches.load(Ordering::Relaxed),
            queue_peak_rows: self.shared.queue_peak_rows.load(Ordering::Relaxed),
            queue_peak_bytes: self.shared.queue_peak_bytes.load(Ordering::Relaxed),
            queue_blocked_checks: self.shared.queue_blocked_checks.load(Ordering::Relaxed),
            abort_requests: self.shared.abort_requests.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn queued_usage(&self) -> (usize, usize) {
        let usage = self
            .shared
            .queue_usage
            .lock()
            .expect("async writer queue usage lock");
        (usage.rows, usage.bytes)
    }
}

impl<O: Send + 'static> Drop for AsyncWriterOwner<O> {
    fn drop(&mut self) {
        self.request_abort();
        let signal = self.runtime_error.clone();
        let observation = self.observe_actor_exit();
        if let (Some(signal), Some(failure)) = (signal, observation.failure) {
            match failure {
                ExitFailure::Required(cause) => signal.set_error(cause),
                ExitFailure::Cleanup(cause) => signal.report_cleanup(&cause),
            }
        }
    }
}

enum AwaitResult<T> {
    Completed(T),
    Aborted,
    Panicked(ExecutionFailure),
}

type WriterPanicPayload = Box<dyn std::any::Any + Send + 'static>;
type WriterActorFuture<O> = std::pin::Pin<Box<dyn std::future::Future<Output = Option<O>> + Send>>;
#[derive(Clone, Copy)]
#[repr(usize)]
enum WriterPhase {
    Open,
    Append,
    Finish,
    Map,
    Abort,
    Retire,
    Publish,
    Join,
    MapOwner,
    MapInput,
}
impl WriterPhase {
    fn required(self) -> bool {
        matches!(self, Self::Open | Self::Append | Self::Finish | Self::Map)
    }
    fn operation(self) -> Option<ConnectorOperation> {
        match self {
            Self::Open => Some(ConnectorOperation::OpenWriter),
            Self::Append => Some(ConnectorOperation::AppendWriter),
            Self::Finish => Some(ConnectorOperation::FinishWriter),
            Self::Abort => Some(ConnectorOperation::AbortWriter),
            _ => None,
        }
    }
}
// Closed author phases, with independent poll/construction and Drop disposal.
// MapOwner/MapInput are cleanup-only destructors, separate from Required Map.
const WRITER_PANIC_CELLS: usize = 20;
#[derive(Clone, Copy)]
#[repr(usize)]
enum WriterNotification {
    Publication,
    Cleanup,
    Health,
    Readiness,
}
const WRITER_NOTIFICATIONS: usize = 4;
fn panic_detail(payload: &(dyn std::any::Any + Send)) -> BoundedDetail {
    BoundedDetail::truncating(
        payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown panic payload"),
    )
}
impl<O> WriterShared<O> {
    fn phase(&self, phase: WriterPhase) {
        self.phase.store(phase as usize, Ordering::Release);
    }
    fn current_phase(&self) -> WriterPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => WriterPhase::Open,
            1 => WriterPhase::Append,
            2 => WriterPhase::Finish,
            3 => WriterPhase::Map,
            4 => WriterPhase::Abort,
            5 => WriterPhase::Retire,
            6 => WriterPhase::Publish,
            7 => WriterPhase::Join,
            8 => WriterPhase::MapOwner,
            _ => WriterPhase::MapInput,
        }
    }
    fn cleanup(&self, failure: &ExecutionFailure, signal: &RuntimeErrorState) {
        self.record_cleanup_error(failure);
        self.notify(WriterNotification::Cleanup, signal, &mut || {
            signal.report_cleanup(failure)
        });
    }
    fn unknown(&self, failure: &ExecutionFailure, signal: &RuntimeErrorState) {
        self.unresolved.store(true, Ordering::Release);
        self.notify(WriterNotification::Health, signal, &mut || {
            signal.report_unresolved_exit(failure)
        });
        self.notify(WriterNotification::Readiness, signal, &mut || self.wake());
    }
    fn save_panic(
        &self,
        phase: WriterPhase,
        dropping: bool,
        payload: WriterPanicPayload,
    ) -> ExecutionFailure {
        let index = phase as usize * 2 + usize::from(dropping);
        let mut primary = self.panic_primary.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            primary[index].is_none(),
            "a saved writer panic phase is never overwritten"
        );
        // The original Any is addressable in the exact original shared owner
        // before constructing/publishing a typed cause or notifying anybody.
        primary[index] = Some(std::mem::ManuallyDrop::new(payload));
        let detail = panic_detail(primary[index].as_ref().unwrap().as_ref());
        drop(primary);
        WriterFailure::Panicked {
            operation: match phase {
                WriterPhase::Join => self.current_phase().operation(),
                _ => phase.operation(),
            },
            detail,
        }
        .into()
    }
    fn dispose_panic(&self, phase: WriterPhase, dropping: bool, signal: &RuntimeErrorState) {
        let index = phase as usize * 2 + usize::from(dropping);
        let Some(mut payload) =
            self.panic_primary.lock().unwrap_or_else(|p| p.into_inner())[index].take()
        else {
            return;
        };
        if let Err(secondary) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            std::mem::ManuallyDrop::drop(&mut payload)
        })) {
            let mut faults = self
                .panic_payloads
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                faults[index].is_none(),
                "the same unwound writer author phase is never retried"
            );
            // Save secondary custody before Health or any synchronous wake.
            faults[index] = Some(std::mem::ManuallyDrop::new(secondary));
            let detail = panic_detail(faults[index].as_ref().unwrap().as_ref());
            drop(faults);
            let failure: ExecutionFailure = WriterFailure::Panicked {
                operation: phase.operation(),
                detail,
            }
            .into();
            self.unknown(&failure, signal);
        }
    }
    fn caught_panic(
        &self,
        phase: WriterPhase,
        dropping: bool,
        payload: WriterPanicPayload,
        signal: &RuntimeErrorState,
    ) -> ExecutionFailure {
        let failure = self.save_panic(phase, dropping, payload);
        if phase.required() {
            self.set_error(failure.clone(), signal);
        } else {
            self.cleanup(&failure, signal);
        }
        failure
    }

    /// Each original notification endpoint is independently supervised. A
    /// callback unwind is Cleanup/Health, never another originating failure.
    /// The failed endpoint is not re-entered; other endpoints can still run.
    fn notify(
        &self,
        kind: WriterNotification,
        signal: &RuntimeErrorState,
        action: &mut dyn FnMut(),
    ) {
        let index = kind as usize;
        if self.notification_failed[index].load(Ordering::Acquire) {
            return;
        }
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
            self.notification_failed[index].store(true, Ordering::Release);
            let mut primary = self
                .notification_primary
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                primary[index].is_none(),
                "failed writer notification is never retried"
            );
            primary[index] = Some(std::mem::ManuallyDrop::new(payload));
            let detail = panic_detail(primary[index].as_ref().unwrap().as_ref());
            drop(primary);
            let failure: ExecutionFailure = WriterFailure::Panicked {
                operation: None,
                detail,
            }
            .into();
            self.unresolved.store(true, Ordering::Release);
            self.notify(WriterNotification::Health, signal, &mut || {
                signal.report_unresolved_exit(&failure)
            });
            self.cleanup(&failure, signal);
            self.notify(WriterNotification::Readiness, signal, &mut || self.wake());
            // All reporting finished (or was independently caught) with the
            // primary still in this scope. Only then attempt its one Drop.
            let mut payload = self
                .notification_primary
                .lock()
                .unwrap_or_else(|p| p.into_inner())[index]
                .take()
                .unwrap();
            if let Err(secondary) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                    std::mem::ManuallyDrop::drop(&mut payload)
                }))
            {
                // No notification occurs while this secondary is still raw.
                self.notification_secondary
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())[index] =
                    Some(std::mem::ManuallyDrop::new(secondary));
            }
        }
    }
}
// The original admitted frame has a named owner even if the executor cancels
// its wrapper. An unresolved frame is moved into its own original shared
// custody, never repolled or destroyed by implicit async-field Drop glue.
struct WriterActorFrame<O> {
    future: Option<std::mem::ManuallyDrop<WriterActorFuture<O>>>,
    shared: Arc<WriterShared<O>>,
    signal: Arc<RuntimeErrorState>,
    completed: bool,
}
impl<O> Drop for WriterActorFrame<O> {
    fn drop(&mut self) {
        let Some(mut future) = self.future.take() else {
            return;
        };
        if !self.completed && !self.shared.unresolved.load(Ordering::Acquire) {
            let failure: ExecutionFailure = WriterFailure::ActorInterrupted.into();
            self.shared.cleanup(&failure, &self.signal);
            self.shared.unknown(&failure, &self.signal);
        }
        if !self.shared.unresolved.load(Ordering::Acquire) {
            self.shared.phase(WriterPhase::Retire);
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                    std::mem::ManuallyDrop::drop(&mut future)
                }))
            {
                let failure =
                    self.shared
                        .caught_panic(WriterPhase::Retire, true, payload, &self.signal);
                self.shared.unknown(&failure, &self.signal);
                self.shared
                    .dispose_panic(WriterPhase::Retire, true, &self.signal);
                // A Box-containing ManuallyDrop must never be moved after an
                // attempted destructor. Keep only the original owner's sticky
                // custody; the attempted frame is never accessed again.
                self.shared
                    .unknown_self
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_or_insert_with(|| self.shared.clone());
                return;
            } else {
                self.shared.frame_exited.store(true, Ordering::Release);
                return;
            }
        }
        // No destructor was attempted: this retains every still-live
        // original frame field and preserves its pinned heap address.
        *self
            .shared
            .unknown_actor
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(future);
    }
}
impl<O: Send + 'static> WriterActorFrame<O> {
    fn new(
        start: WriterStart<O>,
        shared: Arc<WriterShared<O>>,
        config: AsyncWriterQueueConfig,
        signal: Arc<RuntimeErrorState>,
    ) -> Self {
        Self {
            future: Some(std::mem::ManuallyDrop::new(Box::pin(run_writer_actor(
                start,
                shared.clone(),
                config,
                signal.clone(),
            )))),
            shared,
            signal,
            completed: false,
        }
    }
}
async fn supervise_actor<O: Send + 'static>(mut frame: WriterActorFrame<O>) {
    let shared = frame.shared.clone();
    let signal = frame.signal.clone();
    let outcome = futures::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            frame.future.as_mut().unwrap().as_mut().poll(cx)
        })) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(result)) => std::task::Poll::Ready(Ok(result)),
            Err(payload) => std::task::Poll::Ready(Err(payload)),
        }
    })
    .await;
    let outcome = match outcome {
        Ok(output) => {
            frame.completed = true;
            Some(std::mem::ManuallyDrop::new(output))
        }
        Err(payload) => {
            let phase = shared.current_phase();
            let failure = shared.caught_panic(phase, false, payload, &signal);
            shared.unknown(&failure, &signal);
            shared.dispose_panic(phase, false, &signal);
            None
        }
    };
    drop(frame); // explicit original frame exit, before publishing completion
    if shared.unresolved.load(Ordering::Acquire) {
        *shared
            .unknown_output
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = outcome;
        return;
    }
    shared.clear_queue_usage();
    shared.phase(WriterPhase::Publish);
    match outcome.and_then(std::mem::ManuallyDrop::into_inner) {
        Some(output) => shared.publish_terminal_success(output),
        None => {
            shared.ensure_terminal_outcome(&signal);
            shared.set_done();
        }
    }
}

async fn await_supervised<
    T,
    F: std::future::Future<Output = Result<T, novarocks_spi::connector::ConnectorError>>,
>(
    construct: impl FnOnce() -> F,
    shared: &WriterShared<impl Send>,
    signal: &RuntimeErrorState,
    phase: WriterPhase,
    cancel_open: bool,
) -> AwaitResult<ExecutionResult<T>> {
    shared.phase(phase);
    if cancel_open && shared.abort_requested.load(Ordering::Acquire) {
        return AwaitResult::Aborted;
    }
    let future = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(construct)) {
        Ok(future) => future,
        Err(payload) => {
            let failure = shared.caught_panic(phase, false, payload, signal);
            shared.unknown(&failure, signal);
            shared.dispose_panic(phase, false, signal);
            return std::future::pending().await;
        }
    };
    let mut future = Some(std::mem::ManuallyDrop::new(Box::pin(future)));
    let poll = futures::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.as_mut().unwrap().as_mut().poll(cx)
        })) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(result)) => std::task::Poll::Ready(Ok(result)),
            Err(payload) => std::task::Poll::Ready(Err(payload)),
        }
    });
    let outcome = if cancel_open {
        tokio::select! { biased; _=shared.abort_notify.notified()=>None, outcome=poll=>Some(outcome) }
    } else {
        Some(poll.await)
    };
    let outcome = outcome.map(|observed| {
        observed.map(|result| {
            result.map_err(|source| {
                ExecutionFailure::from(ConnectorCallFailure::new(
                    phase.operation().expect("connector call phase"),
                    source,
                ))
            })
        })
    });
    if let Some(Ok(Err(original))) = &outcome {
        if phase.required() {
            shared.set_error(original.clone(), signal);
        } else {
            // Stabilize the exact typed result before any callback or actual
            // lifecycle future Drop. After this one returned error the same
            // actor is held unresolved, so it cannot issue another abort.
            let mut saved = shared
                .abort_failure
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            debug_assert!(saved.is_none());
            *saved = Some(original.clone());
            drop(saved);
            shared.cleanup(original, signal);
        }
    }
    // Keep even a successful output under explicit custody until the original
    // provider call future has actually completed its one destructor.
    let mut outcome = std::mem::ManuallyDrop::new(outcome);
    let primary = if matches!(&*outcome, Some(Err(_))) {
        let Some(Err(payload)) = (unsafe { std::mem::ManuallyDrop::take(&mut outcome) }) else {
            unreachable!()
        };
        // Replace the now-taken wrapper without dropping or moving its stale
        // Box bit pattern; the original opaque payload is moved only once.
        unsafe {
            std::ptr::write(&mut outcome, std::mem::ManuallyDrop::new(None));
        }
        let failure = shared.caught_panic(phase, false, payload, signal);
        Some(failure)
    } else {
        None
    };
    if let Err(payload) = drop_writer_future(future.take().expect("one original provider future")) {
        let failure = shared.save_panic(phase, true, payload);
        shared.cleanup(&failure, signal);
        shared.unknown(&failure, signal);
        shared.dispose_panic(phase, true, signal);
        // Poll and future-Drop payloads are independent original once phases.
        if primary.is_some() {
            shared.dispose_panic(phase, false, signal);
        }
        return std::future::pending().await;
    }
    if let Some(primary) = primary {
        shared.dispose_panic(phase, false, signal);
        // Returning from this helper proves that this original borrowed call
        // future completed its destructor. Opaque payload custody is separate:
        // the caller must retain its funding while independently aborting the
        // now-unborrowed writer, then preserve whole-scope Unresolved.
        return AwaitResult::Panicked(primary);
    }
    let observed = unsafe { std::mem::ManuallyDrop::take(&mut outcome) };
    unsafe {
        std::ptr::write(&mut outcome, std::mem::ManuallyDrop::new(None));
    }
    let outcome = observed;
    match outcome {
        Some(Err(_)) => unreachable!("opaque panic moved into fixed custody"),
        Some(Ok(value)) => AwaitResult::Completed(value),
        None => AwaitResult::Aborted,
    }
}

fn drop_writer_future<F>(
    future: std::mem::ManuallyDrop<std::pin::Pin<Box<F>>>,
) -> Result<(), WriterPanicPayload> {
    drop_owned_writer_value(future)
}
fn drop_owned_writer_value<T>(
    mut value: std::mem::ManuallyDrop<T>,
) -> Result<(), WriterPanicPayload> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        std::mem::ManuallyDrop::drop(&mut value)
    }))
}

async fn await_lifecycle<
    T,
    F: std::future::Future<Output = Result<T, novarocks_spi::connector::ConnectorError>>,
>(
    construct: impl FnOnce() -> F,
    shared: &WriterShared<impl Send>,
    signal: &RuntimeErrorState,
    phase: WriterPhase,
) -> Result<ExecutionResult<T>, ExecutionFailure> {
    match await_supervised(construct, shared, signal, phase, false).await {
        AwaitResult::Completed(value) => Ok(value),
        AwaitResult::Panicked(error) => Err(error),
        AwaitResult::Aborted => unreachable!("only an open future is cancel-safe"),
    }
}

struct ProviderWriter<O> {
    writer: Box<dyn ConnectorBatchWriter>,
    shared: Arc<WriterShared<O>>,
}
impl<O> ProviderWriter<O> {
    fn as_mut(&mut self) -> &mut dyn ConnectorBatchWriter {
        self.writer.as_mut()
    }
}
impl<O> Drop for ProviderWriter<O> {
    fn drop(&mut self) {
        self.shared.phase(WriterPhase::Retire);
    }
}

async fn abort_writer<O: Send>(
    writer: &mut dyn ConnectorBatchWriter,
    timeout: Duration,
    shared: &WriterShared<O>,
    runtime_error: &RuntimeErrorState,
) -> ExecutionResult<()> {
    let abort = await_lifecycle(
        move || writer.abort(),
        shared,
        runtime_error,
        WriterPhase::Abort,
    );
    tokio::pin!(abort);
    let result = match tokio::time::timeout(timeout, abort.as_mut()).await {
        Ok(result) => result,
        Err(_) => {
            let failure: ExecutionFailure = WriterFailure::AbortTimeout { timeout }.into();
            shared.cleanup(&failure, runtime_error);
            abort.await
        }
    };
    let result = match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            // The actual call and its future destructor exited, but did not
            // dispose the journal's objects. Preserve that original writer in
            // this same frame; its cause was installed before the first wake.
            shared.unknown(&error, runtime_error);
            Err(error)
        }
        Err(error) => Err(error),
    };
    if shared.unresolved.load(Ordering::Acquire) {
        // The independent abort really ran once. Its own completed future does
        // not retire an opaque mapper/append/output or a notification owner.
        std::future::pending::<()>().await;
    }
    result
}

async fn fail_with_abort<O: Send>(
    writer: &mut dyn ConnectorBatchWriter,
    primary: ExecutionFailure,
    shared: &WriterShared<O>,
    config: AsyncWriterQueueConfig,
    runtime_error: &RuntimeErrorState,
) {
    // Publish the originating failure before awaiting cleanup. The writer
    // still owns its real exit, but sibling drivers can stop immediately.
    shared.set_error(primary, runtime_error);
    if let Err(abort_error) =
        abort_writer(writer, config.abort_timeout, shared, runtime_error).await
    {
        let _already_reported = abort_error;
    }
    if shared.unresolved.load(Ordering::Acquire) {
        // This writer may no longer be borrowed, but an independent cleanup
        // payload/notification is still owned by this original frame.
        std::future::pending::<()>().await;
    }
}

fn checked_accepted_rows(current: u64, rows: usize) -> ExecutionResult<u64> {
    let incoming = u64::try_from(rows).map_err(|_| WriterFailure::AcceptedRowsOverflow {
        current,
        incoming: rows,
    })?;
    current.checked_add(incoming).ok_or_else(|| {
        WriterFailure::AcceptedRowsOverflow {
            current,
            incoming: rows,
        }
        .into()
    })
}

async fn run_writer_actor<O: Send + 'static>(
    start: WriterStart<O>,
    shared: Arc<WriterShared<O>>,
    config: AsyncWriterQueueConfig,
    runtime_error: Arc<RuntimeErrorState>,
) -> Option<O> {
    let WriterStart {
        execution,
        request,
        mut receiver,
        finish_mapper,
    } = start;
    let execution_ref = execution.as_ref();
    let writer = match await_supervised(
        move || execution_ref.open_writer(request),
        shared.as_ref(),
        &runtime_error,
        WriterPhase::Open,
        true,
    )
    .await
    {
        AwaitResult::Completed(Ok(writer)) => writer,
        AwaitResult::Completed(Err(error)) => {
            shared.set_error(error, &runtime_error);
            if shared.unresolved.load(Ordering::Acquire) {
                // No writer was returned. Keep the original unopened frame,
                // receiver and queued funding; there is no writer to abort.
                std::future::pending::<()>().await;
            }
            return None;
        }
        AwaitResult::Aborted => {
            return None;
        }
        AwaitResult::Panicked(_original) => {
            if shared.unresolved.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            return None;
        }
    };
    let mut writer = ProviderWriter {
        writer,
        shared: shared.clone(),
    };
    let mut finish_mapper = Some(finish_mapper);
    let mut accepted_rows = 0u64;

    loop {
        if shared.abort_requested.load(Ordering::Acquire) {
            if let Err(error) = abort_writer(
                writer.as_mut(),
                config.abort_timeout,
                shared.as_ref(),
                &runtime_error,
            )
            .await
            {
                let _already_reported = error;
            }
            return None;
        }
        let command = tokio::select! {
            biased;
            _ = shared.abort_notify.notified() => continue,
            command = receiver.recv() => command,
        };
        let Some(command) = command else {
            if let Err(error) = abort_writer(
                writer.as_mut(),
                config.abort_timeout,
                shared.as_ref(),
                &runtime_error,
            )
            .await
            {
                let _already_reported = error;
            }
            return None;
        };
        match command {
            WriterCommand::Append(command) => {
                let rows = command.rows;
                let bytes = command.bytes;
                let WriterAppendBatch {
                    batch,
                    projection,
                    construction,
                } = command.batch;
                let writer_call = writer.as_mut();
                let outcome = await_lifecycle(
                    move || writer_call.append(batch),
                    shared.as_ref(),
                    &runtime_error,
                    WriterPhase::Append,
                )
                .await;
                if shared.unresolved.load(Ordering::Acquire) {
                    // The call future is physically retired, so this independent
                    // abort is legal. Keep the original projection, construction,
                    // transferred/additional accounting and queue charge in this
                    // same frame while its opaque payload remains unresolved.
                    let _ = abort_writer(
                        writer.as_mut(),
                        config.abort_timeout,
                        shared.as_ref(),
                        &runtime_error,
                    )
                    .await;
                    std::future::pending::<()>().await;
                }
                // The actual append future has returned. Only now can its
                // shared type, copy loans, and retained accounting exit.
                drop(projection);
                drop(construction);
                drop(command._additional_accounting);
                drop(command._transferred_accounting);
                shared.release_append(rows, bytes);
                match outcome {
                    Ok(Ok(())) => {
                        accepted_rows = match checked_accepted_rows(accepted_rows, rows) {
                            Ok(total) => total,
                            Err(error) => {
                                fail_with_abort(
                                    writer.as_mut(),
                                    error,
                                    shared.as_ref(),
                                    config,
                                    &runtime_error,
                                )
                                .await;
                                return None;
                            }
                        };
                    }
                    Ok(Err(error)) => {
                        fail_with_abort(
                            writer.as_mut(),
                            error,
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                    Err(original) => {
                        fail_with_abort(
                            writer.as_mut(),
                            original,
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                }
            }
            WriterCommand::Finish => {
                let writer_call = writer.as_mut();
                let fragments = match await_lifecycle(
                    move || writer_call.finish(),
                    shared.as_ref(),
                    &runtime_error,
                    WriterPhase::Finish,
                )
                .await
                {
                    Ok(Ok(fragments)) => fragments,
                    Ok(Err(error)) => {
                        fail_with_abort(
                            writer.as_mut(),
                            error,
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                    Err(original) => {
                        fail_with_abort(
                            writer.as_mut(),
                            original,
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                };
                if shared.abort_requested.load(Ordering::Acquire) {
                    if let Err(error) = abort_writer(
                        writer.as_mut(),
                        config.abort_timeout,
                        shared.as_ref(),
                        &runtime_error,
                    )
                    .await
                    {
                        let _already_reported = error;
                    }
                    return None;
                }
                let mut mapper =
                    Some(std::mem::ManuallyDrop::new(finish_mapper.take().expect(
                        "connector writer finish mapper is consumed exactly once",
                    )));
                let mut fragments = Some(std::mem::ManuallyDrop::new(fragments));
                // The logical once call is marked before entering the foreign
                // encoder. Captures and input remain in their original slots;
                // neither crosses this boundary by value or is retried.
                shared.phase(WriterPhase::Map);
                let mapped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    mapper.as_mut().unwrap()(accepted_rows, fragments.as_ref().unwrap().as_slice())
                }));
                let mut mapped = std::mem::ManuallyDrop::new(Some(match mapped {
                    Ok(result) => result,
                    Err(payload) => {
                        let failure =
                            shared.caught_panic(WriterPhase::Map, false, payload, &runtime_error);
                        shared.dispose_panic(WriterPhase::Map, false, &runtime_error);
                        Err(failure)
                    }
                }));
                if let Some(Err(cause)) = &*mapped {
                    shared.set_error(cause.clone(), &runtime_error);
                }
                if !shared.unresolved.load(Ordering::Acquire) {
                    shared.phase(WriterPhase::MapOwner);
                    if let Err(payload) = drop_owned_writer_value(mapper.take().unwrap()) {
                        let failure = shared.caught_panic(
                            WriterPhase::MapOwner,
                            true,
                            payload,
                            &runtime_error,
                        );
                        shared.unknown(&failure, &runtime_error);
                        shared.dispose_panic(WriterPhase::MapOwner, true, &runtime_error);
                    }
                }
                if !shared.unresolved.load(Ordering::Acquire) {
                    shared.phase(WriterPhase::MapInput);
                    if let Err(payload) = drop_owned_writer_value(fragments.take().unwrap()) {
                        let failure = shared.caught_panic(
                            WriterPhase::MapInput,
                            true,
                            payload,
                            &runtime_error,
                        );
                        shared.unknown(&failure, &runtime_error);
                        shared.dispose_panic(WriterPhase::MapInput, true, &runtime_error);
                    }
                }
                if shared.unresolved.load(Ordering::Acquire) {
                    // No provider future borrows the writer here. Retain every
                    // still-live mapper/input/output slot through this abort;
                    // its completion cannot certify their unknown teardown.
                    let _ = abort_writer(
                        writer.as_mut(),
                        config.abort_timeout,
                        shared.as_ref(),
                        &runtime_error,
                    )
                    .await;
                    std::future::pending::<()>().await;
                }
                let observed = unsafe { std::mem::ManuallyDrop::take(&mut mapped) };
                unsafe {
                    std::ptr::write(&mut mapped, std::mem::ManuallyDrop::new(None));
                }
                match observed.expect("one original mapper outcome") {
                    Ok(output) => return Some(output),
                    Err(error) => {
                        fail_with_abort(
                            writer.as_mut(),
                            error,
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                    }
                }
                return None;
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::write_stack::{
        ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
        ConnectorWriteExecution,
    };
    use novarocks_spi::connector::{
        CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorExecutionResources,
        ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLease,
        ConnectorResourceLedger,
    };

    use super::*;
    use crate::exec::operators::table_writer::tests::{
        catalog_handle, request_context, target, writer_handle,
    };
    use crate::runtime::{ExecutionRuntime, ExecutionRuntimeConfig};

    #[derive(Default)]
    struct Calls {
        opened: AtomicUsize,
        append_started: AtomicUsize,
        appended: AtomicUsize,
        finished: AtomicUsize,
        aborted: AtomicUsize,
    }

    struct ControlledExecution {
        catalog_handle: CatalogHandle,
        calls: Arc<Calls>,
        append_gate: Option<Arc<Notify>>,
        fail_append: bool,
        fail_abort: bool,
    }

    struct TrackerLedger {
        tracker: Arc<MemTracker>,
    }

    pub(crate) fn provider_chunk_with_original_token(
        batch: RecordBatch,
        tracker: &Arc<MemTracker>,
    ) -> (crate::exec::chunk::Chunk, usize) {
        let bytes = batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum::<usize>();
        let resources =
            ConnectorExecutionResources::from_admitted_ledger(Arc::new(TrackerLedger {
                tracker: Arc::clone(tracker),
            }));
        let token = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes as u64)
            .unwrap()
            .into_output(bytes as u64)
            .unwrap();
        let slots = (0..batch.num_columns())
            .map(|ordinal| novarocks_types::SlotId(ordinal as u32 + 1))
            .collect::<Vec<_>>();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &slots,
        )
        .unwrap();
        let mut chunk =
            crate::exec::chunk::Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
        chunk.attach_connector_output_memory(token).unwrap();
        (chunk, bytes)
    }

    impl ConnectorResourceLedger for TrackerLedger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(0))
        }

        fn try_reserve(
            &self,
            class: ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            assert_eq!(class, ConnectorResourceClass::ReaderOutput);
            let bytes = i64::try_from(bytes).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "test connector reservation exceeds i64",
                )
            })?;
            if let Err(error) = self.tracker.consume_and_check_limit(bytes) {
                self.tracker.release(bytes);
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    error,
                ));
            }
            Ok(Box::new(TrackerLease {
                tracker: Arc::clone(&self.tracker),
                bytes,
            }))
        }
    }

    struct TrackerLease {
        tracker: Arc<MemTracker>,
        bytes: i64,
    }

    impl ConnectorResourceLease for TrackerLease {
        fn bytes(&self) -> u64 {
            u64::try_from(self.bytes).expect("non-negative test reservation")
        }

        fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
            let additional = i64::try_from(additional).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "test connector growth exceeds i64",
                )
            })?;
            if let Err(error) = self.tracker.consume_and_check_limit(additional) {
                self.tracker.release(additional);
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    error,
                ));
            }
            self.bytes += additional;
            Ok(())
        }

        fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
            let bytes = i64::try_from(bytes).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "test connector shrink target exceeds i64",
                )
            })?;
            if bytes > self.bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "test connector lease cannot grow through shrink",
                ));
            }
            self.tracker.release(self.bytes - bytes);
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for TrackerLease {
        fn drop(&mut self) {
            self.tracker.release(self.bytes);
        }
    }

    fn connector_accounted_chunk(
        batch: RecordBatch,
        tracker: &Arc<MemTracker>,
    ) -> (crate::exec::chunk::Chunk, usize) {
        let reserved_bytes = batch
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let resources =
            ConnectorExecutionResources::from_admitted_ledger(Arc::new(TrackerLedger {
                tracker: Arc::clone(tracker),
            }));
        let output_memory = resources
            .try_reserve(
                ConnectorResourceClass::ReaderOutput,
                u64::try_from(reserved_bytes).expect("test reservation fits u64"),
            )
            .expect("reserve connector output")
            .into_output(u64::try_from(reserved_bytes).expect("test reservation fits u64"))
            .expect("freeze connector output reservation");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[novarocks_types::SlotId(1)],
        )
        .expect("connector chunk schema");
        let mut chunk = crate::exec::chunk::Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect("connector chunk");
        chunk
            .attach_connector_output_memory(output_memory)
            .expect("attach connector output owner");
        (chunk, reserved_bytes)
    }

    #[async_trait::async_trait]
    impl ConnectorWriteExecution for ControlledExecution {
        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }

        async fn open_writer(
            &self,
            _request: ConnectorOpenWriterRequest,
        ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
            self.calls.opened.fetch_add(1, Ordering::Relaxed);
            Ok(Box::new(ControlledWriter {
                calls: Arc::clone(&self.calls),
                append_gate: self.append_gate.clone(),
                fail_append: self.fail_append,
                fail_abort: self.fail_abort,
            }))
        }
    }

    struct ControlledWriter {
        calls: Arc<Calls>,
        append_gate: Option<Arc<Notify>>,
        fail_append: bool,
        fail_abort: bool,
    }

    #[async_trait::async_trait]
    impl ConnectorBatchWriter for ControlledWriter {
        async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
            self.calls.append_started.fetch_add(1, Ordering::Relaxed);
            if let Some(gate) = &self.append_gate {
                gate.notified().await;
            }
            if self.fail_append {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "injected append failure",
                ));
            }
            self.calls
                .appended
                .fetch_add(batch.num_rows(), Ordering::Relaxed);
            Ok(())
        }

        async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
            self.calls.finished.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn abort(&mut self) -> Result<(), ConnectorError> {
            self.calls.aborted.fetch_add(1, Ordering::Relaxed);
            if self.fail_abort {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "injected abort failure",
                ));
            }
            Ok(())
        }
    }

    fn runtime() -> Arc<ExecutionRuntime> {
        Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .expect("test execution runtime"),
        )
    }

    fn request() -> ConnectorOpenWriterRequest {
        ConnectorOpenWriterRequest {
            handle: writer_handle(),
            target: target(0),
            expected_schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            physical: novarocks_spi::connector::write_stack::ConnectorWriterPhysicalContext::new(
                [1; 16], 2, [3; 16], 0, 0,
            ),
            context: request_context(),
        }
    }

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(0..rows as i32)) as ArrayRef],
        )
        .expect("record batch")
    }

    pub(super) fn fixture_append_batch(batch: RecordBatch) -> WriterAppendBatch {
        let slots = (0..batch.num_columns())
            .map(|i| novarocks_types::SlotId::new(u32::try_from(i + 1).unwrap()))
            .collect::<Vec<_>>();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &slots,
        )
        .expect("original accounting fixture schema");
        let projection = crate::exec::chunk::Chunk::try_new_with_chunk_schema(batch, schema)
            .expect("original accounting fixture batch")
            .into_batch_projection();
        WriterAppendBatch::from_fixture_projection(projection)
    }

    fn wait_until(predicate: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(predicate(), "condition did not become true before timeout");
    }

    #[test]
    fn reserved_rows_at_counter_extent_backpressure_without_mutation_or_materialization() {
        let execution = Arc::new(ControlledExecution {
            catalog_handle: catalog_handle(),
            calls: Arc::new(Calls::default()),
            append_gate: None,
            fail_append: false,
            fail_abort: false,
        });
        let config = AsyncWriterQueueConfig {
            max_batches: 2,
            max_rows: usize::MAX,
            max_bytes: 2,
            max_batch_rows: usize::MAX,
            max_batch_bytes: 1,
            abort_timeout: Duration::from_secs(1),
        };
        let owner =
            AsyncWriterOwner::new(execution, request(), config, Box::new(|rows, _| Ok(rows)));
        // A reservation precedes materialization; no huge payload or I/O task is needed.
        let reserved = owner.try_reserve_input(usize::MAX, 0, None, 0, 0).unwrap();
        assert!(!owner.can_accept());
        let error = match owner.try_reserve_input(1, 0, None, 0, 0) {
            Ok(_) => panic!("overflowing queue rows were accepted"),
            Err(error) => error,
        };
        assert!(
            matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(cause)
            if matches!(cause.cause(), WriterFailure::QueueCapacity {
                dimension: ResourceDimension::Positions, incoming: 1, live: usize::MAX, limit: usize::MAX,
            }))
        );
        assert_eq!(owner.queued_usage(), (usize::MAX, 0));
        drop(reserved);
        assert_eq!(owner.queued_usage(), (0, 0));
    }

    #[test]
    fn observed_actor_cause_survives_later_admission_and_finish_without_new_occurrence() {
        let execution = Arc::new(ControlledExecution {
            catalog_handle: catalog_handle(),
            calls: Arc::new(Calls::default()),
            append_gate: None,
            fail_append: false,
            fail_abort: false,
        });
        let mut owner = AsyncWriterOwner::new(
            execution,
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        let original: ExecutionFailure = ConnectorCallFailure::new(
            ConnectorOperation::AppendWriter,
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "original provider bound",
            ),
        )
        .into();
        owner
            .shared
            .set_error(original.clone(), &RuntimeErrorState::default());
        let admission = match owner.try_reserve_input(1, 1, None, 0, 1) {
            Ok(_) => panic!("failed writer accepted a reservation"),
            Err(error) => error,
        };
        let finish = owner.request_finish().unwrap_err();
        assert!(admission.same_occurrence(&original));
        assert!(finish.same_occurrence(&original));
        assert_eq!(owner.queued_usage(), (0, 0));
    }

    fn terminal_observation_owner() -> AsyncWriterOwner<u64> {
        AsyncWriterOwner::new(
            Arc::new(ControlledExecution {
                catalog_handle: catalog_handle(),
                calls: Arc::new(Calls::default()),
                append_gate: None,
                fail_append: false,
                fail_abort: false,
            }),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        )
    }

    fn is_missing_terminal(error: &ExecutionFailure) -> bool {
        matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(cause)
            if matches!(cause.cause(), WriterFailure::MissingTerminalOutcome))
    }

    #[test]
    fn original_split_observations_have_a_false_missing_cut_with_a_real_publisher() {
        use std::task::Poll;

        let owner = terminal_observation_owner();
        let empty = owner.take_output();
        let shared = Arc::clone(&owner.shared);
        let publisher = std::thread::spawn(move || shared.publish_terminal_success(17));
        let publication = publisher.join();
        // The real publication has actually completed between the old two
        // consumer reads. The old branch would report missing despite the
        // original output still being available to the single consumer.
        let legacy_missing = empty.is_none() && owner.is_done();
        let actual = owner.poll_output();
        publication.unwrap();
        assert!(legacy_missing);
        assert_eq!(actual.unwrap(), Poll::Ready(17));
    }

    #[test]
    fn empty_terminal_observation_cannot_race_success_publication() {
        use std::sync::{TryLockError, mpsc};
        use std::task::Poll;

        let owner = terminal_observation_owner();
        let shared = Arc::clone(&owner.shared);
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let release_observed = Arc::new(AtomicBool::new(false));
        let released = Arc::clone(&release_observed);
        let first = AtomicBool::new(true);
        assert!(
            shared
                .empty_result_observed
                .set(Arc::new(move || {
                    if first.swap(false, Ordering::AcqRel) {
                        let _ = reached_tx.send(());
                        let received = release_rx
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(5))
                            .is_ok();
                        released.store(received, Ordering::Release);
                    }
                }))
                .is_ok()
        );
        let consumer = std::thread::spawn(move || {
            let observation = owner.poll_output();
            (owner, observation)
        });
        // Pause after the real empty take, before the real done load. The
        // publisher must wait at its original result mutex, not an extra gate.
        let reached = reached_rx.recv_timeout(Duration::from_secs(5));
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let publisher = std::thread::spawn(move || {
            let blocked = matches!(shared.result.try_lock(), Err(TryLockError::WouldBlock));
            let _ = blocked_tx.send(blocked);
            shared.publish_terminal_success(17);
        });
        let blocked = blocked_rx.recv_timeout(Duration::from_secs(5));
        let _ = release_tx.send(());
        // Release and actually join both owners before any assertion, even
        // when a checkpoint or the observed lock contract failed.
        let consumer_result = consumer.join();
        let publisher_result = publisher.join();
        let (owner, initial) = consumer_result.unwrap();
        publisher_result.unwrap();
        assert!(reached.is_ok());
        assert!(blocked.unwrap());
        assert!(release_observed.load(Ordering::Acquire));
        assert!(matches!(initial, Ok(Poll::Pending)));
        assert_eq!(owner.poll_output().unwrap(), Poll::Ready(17));
    }

    #[test]
    fn terminal_observation_preserves_cause_published_after_initial_error_check() {
        let owner = terminal_observation_owner();
        assert!(owner.error().is_none());
        let original: ExecutionFailure = ConnectorCallFailure::new(
            ConnectorOperation::FinishWriter,
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "original finish failure after driver error check",
            ),
        )
        .into();
        let runtime_error = RuntimeErrorState::default();
        owner.shared.set_error(original.clone(), &runtime_error);
        owner.shared.set_done();
        let observed = owner.poll_output().unwrap_err();
        assert!(observed.same_occurrence(&original));
        assert!(runtime_error.error().unwrap().same_occurrence(&original));
        assert!(!is_missing_terminal(&observed));
    }

    #[test]
    fn true_missing_terminal_and_second_consumption_keep_the_original_failure() {
        use std::task::Poll;

        let missing = terminal_observation_owner();
        assert_eq!(missing.poll_output().unwrap(), Poll::Pending);
        missing.shared.set_done();
        assert!(is_missing_terminal(&missing.poll_output().unwrap_err()));

        let produced = terminal_observation_owner();
        produced.shared.publish_terminal_success(17);
        assert_eq!(produced.poll_output().unwrap(), Poll::Ready(17));
        assert!(is_missing_terminal(&produced.poll_output().unwrap_err()));
        let runtime_error = RuntimeErrorState::default();
        produced.shared.ensure_terminal_outcome(&runtime_error);
        assert!(
            runtime_error.error().is_none(),
            "single consumer ownership does not erase the actor's actual production fact"
        );
    }

    #[test]
    fn consumed_terminal_result_still_satisfies_actor_exit_contract() {
        let shared = WriterShared::<u64>::new();
        let runtime_error = RuntimeErrorState::default();

        shared.publish_terminal_success(17);
        assert_eq!(shared.result.lock().expect("result lock").take(), Some(17));

        // The sink-I/O task runs this check only after the actor future exits.
        // A consumer is allowed to have drained the result slot by then.
        shared.ensure_terminal_outcome(&runtime_error);
        assert!(runtime_error.error().is_none());
        assert!(
            shared.terminal_result_produced.load(Ordering::Acquire),
            "terminal production is a monotonic event, not result-slot occupancy"
        );
    }

    #[test]
    fn successful_terminal_notification_observes_result_and_done_together() {
        let calls = Arc::new(Calls::default());
        let execution = Arc::new(ControlledExecution {
            catalog_handle: catalog_handle(),
            calls,
            append_gate: None,
            fail_append: false,
            fail_abort: false,
        });
        let mut owner = AsyncWriterOwner::new(
            execution,
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observed_shared = Arc::clone(&owner.shared);
        let observed_values = Arc::clone(&observations);
        owner.observable().add_observer(Arc::new(move || {
            let result = *observed_shared.result.lock().expect("result lock");
            if result.is_some() {
                observed_values
                    .lock()
                    .expect("terminal observations lock")
                    .push((observed_shared.done.load(Ordering::Acquire), result));
            }
        }));

        let runtime = runtime();
        owner
            .bind(
                runtime.services().sink_io().clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .expect("bind writer actor");
        owner.request_finish().expect("request writer finish");
        wait_until(|| owner.is_done());

        assert_eq!(
            observations.lock().expect("observations lock").as_slice(),
            &[(true, Some(0))],
            "the real actor path must never notify result visibility before semantic completion"
        );
    }

    #[test]
    fn semantic_terminal_latch_does_not_prove_executor_wrapper_exit() {
        let calls = Arc::new(Calls::default());
        let execution = Arc::new(ControlledExecution {
            catalog_handle: catalog_handle(),
            calls,
            append_gate: None,
            fail_append: false,
            fail_abort: false,
        });
        let mut owner = AsyncWriterOwner::new(
            execution,
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        let runtime = runtime();
        owner.join = Some(runtime.services().sink_io().spawn(async {
            std::future::pending::<()>().await;
        }));

        owner.shared.set_done();

        assert!(
            !owner.join.as_ref().expect("test join handle").is_finished(),
            "the executor wrapper must still be pending for this regression"
        );
        assert!(
            owner.is_done(),
            "semantic writer completion remains observable before wrapper exit"
        );
        assert!(
            !owner.actor_exited(),
            "the writer task still owns its wrapper"
        );
        owner.join.take().expect("test join handle").abort();
    }

    fn owner(
        calls: Arc<Calls>,
        append_gate: Option<Arc<Notify>>,
        fail_append: bool,
        fail_abort: bool,
        config: AsyncWriterQueueConfig,
    ) -> (
        AsyncWriterOwner<u64>,
        Arc<RuntimeErrorState>,
        Arc<MemTracker>,
        Arc<ExecutionRuntime>,
    ) {
        let execution = Arc::new(ControlledExecution {
            catalog_handle: catalog_handle(),
            calls,
            append_gate,
            fail_append,
            fail_abort,
        });
        let error = Arc::new(RuntimeErrorState::default());
        let tracker = MemTracker::new_root("async-writer-test");
        let mut owner =
            AsyncWriterOwner::new(execution, request(), config, Box::new(|rows, _| Ok(rows)));
        owner.set_mem_tracker(Arc::clone(&tracker));
        let runtime = runtime();
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .expect("bind async writer");
        (owner, error, tracker, runtime)
    }

    #[test]
    fn typed_append_custody_survives_pending_provider_future_until_success_or_abort_exit() {
        use crate::exec::chunk::{Chunk, typed_fixture};
        use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        for abort in [false, true] {
            let calls = Arc::new(Calls::default());
            let gate = Arc::new(Notify::new());
            let (mut writer, error, tracker, _runtime) = owner(
                calls.clone(),
                Some(gate.clone()),
                false,
                false,
                AsyncWriterQueueConfig::default(),
            );
            let factory = try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(),
                error.clone(),
            )
            .unwrap();
            let source = batch(3);
            let declared = typed_fixture::layout(
                source.schema(),
                &[novarocks_types::SlotId::new(1)],
                &[FunctionValueType::new(DataType::Int32, false)],
            );
            let schema = factory.create_compiled_chunk_schema(&declared).unwrap();
            let input_owner = factory.begin_typed_construction(&schema).unwrap();
            let input =
                Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
                    source,
                    schema.clone(),
                    ValueTypeStorageLifetime::pure(),
                    input_owner.storage_lifetime(),
                    |layout| input_owner.request(layout),
                    |step| input_owner.observe(0, &[], step),
                )
                .unwrap();
            let projection = input.into_batch_projection();
            let bytes = crate::exec::chunk::record_batch_bytes(projection.record_batch());
            let reservation = writer.try_reserve_input(3, bytes, None, 0, bytes).unwrap();
            let copy_owner = factory
                .begin_typed_construction(projection.chunk_schema())
                .unwrap();
            let append = WriterAppendBatch::try_new(projection, copy_owner.as_ref()).unwrap();
            let weak_factory = Arc::downgrade(&factory);
            drop((copy_owner, input_owner, schema, declared, factory));
            assert!(
                weak_factory.upgrade().is_none(),
                "queued custody must not retain the factory"
            );
            // Scope/factory control and model objects are transient. Measure
            // the actual durable queue obligation only after those exit.
            let pending_current = tracker.current();
            assert!(
                pending_current > i64::try_from(bytes).unwrap(),
                "real schema/wrapper/copy responsibility is distinct from payload queue accounting"
            );
            reservation.send(append).unwrap();
            wait_until(|| calls.append_started.load(Ordering::Relaxed) == 1);
            assert_eq!(
                tracker.current(),
                pending_current,
                "the actual pending append still owns the finite projection and provider-copy responsibility"
            );
            assert_eq!(writer.queued_usage(), (3, bytes));
            if abort {
                writer.request_abort();
                assert!(!writer.actor_exited());
                assert_eq!(tracker.current(), pending_current);
                assert_eq!(writer.queued_usage(), (3, bytes));
                gate.notify_one();
                wait_until(|| writer.actor_exited());
                assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
                assert_eq!(calls.appended.load(Ordering::Relaxed), 3);
                assert_eq!(calls.finished.load(Ordering::Relaxed), 0);
            } else {
                gate.notify_one();
                wait_until(|| writer.queued_usage() == (0, 0));
                assert_eq!(tracker.current(), 0);
                writer.request_finish().unwrap();
                wait_until(|| writer.actor_exited());
                assert_eq!(writer.take_output(), Some(3));
                assert_eq!(calls.appended.load(Ordering::Relaxed), 3);
                assert_eq!(calls.finished.load(Ordering::Relaxed), 1);
            }
            assert_eq!(
                tracker.current(),
                0,
                "actual final exit releases every finite input holder"
            );
            assert!(error.error().is_none());
        }
    }

    #[test]
    fn slow_append_holds_exact_rows_bytes_and_backpressures_until_release() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) = owner(
            Arc::clone(&calls),
            Some(Arc::clone(&gate)),
            false,
            false,
            config,
        );
        let input = batch(3);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        owner.enqueue(input, bytes).expect("first append");
        assert_eq!(owner.queued_usage(), (3, bytes));
        assert!(!owner.can_accept());
        assert!(tracker.current() > 0);
        assert!(owner.enqueue(batch(1), 4).is_err());

        gate.notify_one();
        wait_until(|| owner.can_accept());
        assert_eq!(owner.queued_usage(), (0, 0));
        assert_eq!(tracker.current(), 0);
        owner.request_finish().expect("finish request");
        wait_until(|| owner.has_output());
        assert_eq!(owner.take_output(), Some(3));
        assert_eq!(calls.finished.load(Ordering::Relaxed), 1);
        assert!(error.error().is_none());
    }

    #[test]
    fn query_memory_limit_rejects_queue_admission_without_leaking_reservation() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (owner, _error, tracker, _runtime) = owner(calls, Some(gate), false, false, config);
        tracker.install_limit_once(1).expect("install query limit");

        let input = batch(2);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        let error = owner
            .enqueue(input, bytes)
            .expect_err("queue admission must enforce query limit");

        assert!(
            matches!(
                error.cause(),
                crate::runtime::fragment::ExecutionFailureCause::MemoryAccounting(
                    crate::runtime::mem_tracker::MemoryAccountingFailure::Limit(_)
                )
            ),
            "{error:?}"
        );
        assert_eq!(owner.queued_usage(), (0, 0));
        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.allocated(), tracker.deallocated());
    }

    #[test]
    fn zero_copy_projection_transfers_existing_charge_without_false_oom() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) =
            owner(calls, Some(Arc::clone(&gate)), false, false, config);
        let input = batch(3);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        tracker
            .install_limit_once(i64::try_from(bytes).unwrap())
            .expect("install exact query limit");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            input.schema().as_ref(),
            &[novarocks_types::SlotId(1)],
        )
        .unwrap();
        let mut chunk =
            crate::exec::chunk::Chunk::try_new_with_chunk_schema(input.clone(), chunk_schema)
                .unwrap();
        chunk.transfer_to(&tracker);
        let accounting = chunk.take_memory_lease();

        owner
            .enqueue_with_accounting(input, bytes, accounting, bytes, 0)
            .expect("the existing Arrow charge transfers into the writer queue");
        drop(chunk);
        assert_eq!(tracker.current(), i64::try_from(bytes).unwrap());
        assert_eq!(tracker.peak(), i64::try_from(bytes).unwrap());

        gate.notify_one();
        wait_until(|| owner.queued_usage() == (0, 0));
        assert_eq!(tracker.current(), 0);
        owner.request_finish().unwrap();
        wait_until(|| owner.has_output());
        assert!(error.error().is_none());
    }

    fn compiled_writer_source_loan(provider: bool) {
        use crate::exec::chunk::{record_batch_additional_bytes, record_batch_shared_owner_bytes};
        use crate::exec::expr::compiled_program::{
            input_source::{BorrowedInputSource, SlotSourceObserver},
            input_source_tests::input_chunk,
            tests::{SeedMode, program},
        };
        use crate::exec::operators::compiled_writer::source_loan_test_projector;
        use crate::exec::pipeline::operator::ExitProgress;
        use arrow::{
            array::Int64Array,
            buffer::{Buffer, ScalarBuffer},
        };
        use novarocks_type_contract::{
            ValueTypeStorageHold, owned_resources::layout::LayoutResourceError,
        };

        struct SourceValues {
            values: Box<[i64]>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for SourceValues {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Creator {
            _bytes: Vec<u8>,
            drops: Arc<AtomicUsize>,
        }
        impl ValueTypeStorageHold for Creator {}
        impl Drop for Creator {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct CountedLedger {
            inner: TrackerLedger,
            drops: Arc<AtomicUsize>,
        }
        struct CountedLease {
            inner: Box<dyn ConnectorResourceLease>,
            drops: Arc<AtomicUsize>,
        }
        impl ConnectorResourceLedger for CountedLedger {
            fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
                self.inner.checkpoint()
            }
            fn try_reserve(
                &self,
                class: ConnectorResourceClass,
                bytes: u64,
            ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
                Ok(Box::new(CountedLease {
                    inner: self.inner.try_reserve(class, bytes)?,
                    drops: self.drops.clone(),
                }))
            }
        }
        impl ConnectorResourceLease for CountedLease {
            fn bytes(&self) -> u64 {
                self.inner.bytes()
            }
            fn try_grow(&mut self, bytes: u64) -> Result<(), ConnectorError> {
                self.inner.try_grow(bytes)
            }
            fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
                self.inner.shrink_to(bytes)
            }
        }
        impl Drop for CountedLease {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[derive(Default)]
        struct Appended {
            values: Option<Vec<Option<i64>>>,
            nullable: Option<bool>,
            schema_matches: Option<bool>,
            last_alias: Option<Buffer>,
        }
        struct LoanExecution {
            catalog: CatalogHandle,
            calls: Arc<Calls>,
            gate: Arc<Notify>,
            appended: Arc<Mutex<Appended>>,
        }
        struct LoanWriter {
            schema: Arc<Schema>,
            calls: Arc<Calls>,
            gate: Arc<Notify>,
            appended: Arc<Mutex<Appended>>,
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for LoanExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog
            }
            async fn open_writer(
                &self,
                request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                self.calls.opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(LoanWriter {
                    schema: request.expected_schema,
                    calls: self.calls.clone(),
                    gate: self.gate.clone(),
                    appended: self.appended.clone(),
                }))
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for LoanWriter {
            async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
                self.calls.append_started.fetch_add(1, Ordering::SeqCst);
                self.gate.notified().await;
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                // Create this alias only after the append gate. It cannot mask
                // loss of queue/future custody while the provider is pending.
                let mut appended = self.appended.lock().unwrap();
                appended.values = Some(array.iter().collect());
                appended.nullable = Some(batch.schema().field(0).is_nullable());
                appended.schema_matches = Some(batch.schema().as_ref() == self.schema.as_ref());
                appended.last_alias = Some(array.values().inner().clone());
                self.calls
                    .appended
                    .fetch_add(batch.num_rows(), Ordering::SeqCst);
                Ok(())
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                self.calls.finished.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.calls.aborted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        struct ReleaseGate(Arc<Notify>);
        impl Drop for ReleaseGate {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let program = program(SeedMode::Input, false);
        let mut input = input_chunk(&program, vec![Some(42), None, Some(7)]);
        let mut open_request = request();
        open_request.expected_schema = Arc::new(Schema::new(vec![
            input.record_batch().schema().field(1).clone(),
        ]));
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let appended = Arc::new(Mutex::new(Appended::default()));
        let error = Arc::new(RuntimeErrorState::default());
        let tracker = MemTracker::new_root("compiled writer finite source custody");
        // This functional fixture admits finite construction metadata and the
        // existing fallback. It does not assert the retired exact S/peak model.
        tracker.install_limit_once(64 * 1024 * 1024).unwrap();
        let execution = Arc::new(LoanExecution {
            catalog: catalog_handle(),
            calls: calls.clone(),
            gate: gate.clone(),
            appended: appended.clone(),
        });
        let runtime = runtime();
        let mut owner = AsyncWriterOwner::new(
            execution,
            open_request,
            AsyncWriterQueueConfig {
                max_batches: 1,
                max_rows: 4,
                max_bytes: 4096,
                max_batch_rows: 4,
                max_batch_bytes: 4096,
                abort_timeout: Duration::from_secs(1),
            },
            Box::new(|rows, _| Ok(rows)),
        );
        owner.set_mem_tracker(tracker.clone());
        owner
            .bind(runtime.services().sink_io().clone(), error.clone())
            .unwrap();
        let _release = ReleaseGate(gate.clone());
        let values_drops = Arc::new(AtomicUsize::new(0));
        let creator_drops = Arc::new(AtomicUsize::new(0));
        let provider_drops = Arc::new(AtomicUsize::new(0));
        let values = Arc::new(SourceValues {
            values: vec![42_i64, 0, 7].into_boxed_slice(),
            drops: values_drops.clone(),
        });
        let values_weak = Arc::downgrade(&values);
        let pointer =
            std::ptr::NonNull::new(values.values.as_ptr().cast_mut().cast::<u8>()).unwrap();
        let bytes = values.values.len() * std::mem::size_of::<i64>();
        // SAFETY: SourceValues retains this initialized, aligned i64 region
        // until the last real Arrow buffer alias drops its custom owner.
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, bytes, values) };
        let source_array: ArrayRef = Arc::new(Int64Array::new(
            ScalarBuffer::new(buffer, 0, 3),
            input.record_batch().column(1).nulls().cloned(),
        ));
        let replacement = RecordBatch::try_new(
            input.record_batch().schema(),
            vec![
                input.record_batch().column(0).clone(),
                source_array,
                input.record_batch().column(2).clone(),
            ],
        )
        .unwrap();
        input.replace_record_batch_for_test(replacement);
        let creator = Arc::new(Creator {
            _bytes: vec![0x5a; 32 * 1024],
            drops: creator_drops.clone(),
        });
        let creator_weak = Arc::downgrade(&creator);
        let storage = ValueTypeStorageLifetime::held(creator);
        input = input
            .retaining_array_type_storage_observed::<LayoutResourceError>(&storage, |_| Ok(()))
            .unwrap();
        drop(storage);
        if provider {
            let resources =
                ConnectorExecutionResources::from_admitted_ledger(Arc::new(CountedLedger {
                    inner: TrackerLedger {
                        tracker: tracker.clone(),
                    },
                    drops: provider_drops.clone(),
                }));
            let bytes = input
                .record_batch()
                .columns()
                .iter()
                .map(|a| a.get_array_memory_size())
                .sum::<usize>();
            let token = resources
                .try_reserve(ConnectorResourceClass::ReaderOutput, bytes as u64)
                .unwrap()
                .into_output(bytes as u64)
                .unwrap();
            input.attach_connector_output_memory(token).unwrap();
        } else {
            input.try_transfer_to(&tracker).unwrap();
        }
        let expected_owner = input.source_authority().owner_identity();
        let expected_counts = input.source_authority().strong_counts();
        let expected_array = Arc::as_ptr(input.record_batch().column(1)) as *const () as usize;
        let seen = Arc::new(Mutex::new(None));
        let observed = seen.clone();
        let observer: Arc<SlotSourceObserver> = Arc::new(move |column| {
            let (array, source) = column.into_parts();
            let BorrowedInputSource::Attached(source) = source else {
                panic!("checked Slot must borrow its original attached source")
            };
            let mut seen = observed.lock().unwrap();
            assert!(
                seen.is_none(),
                "this fixture evaluates exactly one checked Slot"
            );
            *seen = Some((
                Arc::as_ptr(array) as *const () as usize,
                source.owner_identity(),
                source.strong_counts(),
            ));
        });
        let mut projector = source_loan_test_projector(program, Some(observer));
        let output = projector.project(&input).unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            Some((expected_array, expected_owner, expected_counts))
        );
        assert!(output.chunk_schema().is_typed());
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type(),
            input.chunk_schema().slots()[1].typed_binding().value_type()
        );
        assert_eq!(
            output.chunk_schema().slots()[0].data_type(),
            &DataType::Int64
        );
        assert!(output.chunk_schema().slots()[0].nullable());
        assert_eq!(
            output
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(42), None, Some(7)]
        );
        let retained = crate::exec::chunk::record_batch_bytes(output.record_batch());
        let shared = record_batch_shared_owner_bytes(output.record_batch(), input.record_batch());
        let additional = record_batch_additional_bytes(output.record_batch(), input.record_batch());
        let lease = input.take_memory_lease();
        let reservation = owner
            .try_reserve_input(output.len(), retained, lease, shared, additional)
            .expect("finite functional capacity admits the existing shared-source path");
        let construction_tracker = MemTracker::new_root("source-loan append construction");
        construction_tracker
            .install_limit_once(64 * 1024 * 1024)
            .unwrap();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            construction_tracker, error.clone(),
        ).unwrap();
        let copy_owner = factory
            .begin_typed_construction(output.chunk_schema())
            .unwrap();
        let append =
            WriterAppendBatch::try_new(output.into_batch_projection(), copy_owner.as_ref())
                .unwrap();
        drop((copy_owner, factory, projector, input));
        reservation.send(append).unwrap();
        wait_until(|| calls.append_started.load(Ordering::SeqCst) == 1);
        assert_eq!(owner.queued_usage(), (3, retained));
        assert!(appended.lock().unwrap().last_alias.is_none());
        assert_eq!(values_drops.load(Ordering::SeqCst), 0);
        assert_eq!(creator_drops.load(Ordering::SeqCst), 0);
        assert_eq!(provider_drops.load(Ordering::SeqCst), 0);
        assert!(values_weak.upgrade().is_some());
        assert!(creator_weak.upgrade().is_some());
        gate.notify_one();
        wait_until(|| owner.queued_usage() == (0, 0));
        let last_alias = {
            let mut actual = appended.lock().unwrap();
            assert_eq!(actual.values.take().unwrap(), vec![Some(42), None, Some(7)]);
            assert_eq!(actual.nullable, Some(true));
            assert_eq!(actual.schema_matches, Some(true));
            actual.last_alias.take().unwrap()
        };
        owner.request_finish().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let observation = owner.observe_actor_exit();
            assert!(observation.failure.is_none());
            assert!(observation.unresolved_exit.is_none());
            match observation.progress {
                ExitProgress::Exited => break,
                ExitProgress::Unresolved(_) => panic!("normal writer exit became unresolved"),
                ExitProgress::Pending(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "actual writer join did not complete"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        assert!(owner.actor_exit && owner.join.is_none());
        assert_eq!(owner.take_output(), Some(3));
        assert_eq!(calls.opened.load(Ordering::SeqCst), 1);
        assert_eq!(calls.appended.load(Ordering::SeqCst), 3);
        assert_eq!(calls.finished.load(Ordering::SeqCst), 1);
        assert_eq!(calls.aborted.load(Ordering::SeqCst), 0);
        assert!(error.error().is_none());
        drop(owner);
        assert_eq!(values_drops.load(Ordering::SeqCst), 0);
        assert_eq!(creator_drops.load(Ordering::SeqCst), 0);
        assert_eq!(provider_drops.load(Ordering::SeqCst), 0);
        // The actor and all original Chunk/projector owners have really exited.
        // This last buffer alias alone retains its data and finite source.
        std::thread::spawn(move || drop(last_alias)).join().unwrap();
        assert_eq!(values_drops.load(Ordering::SeqCst), 1);
        assert_eq!(creator_drops.load(Ordering::SeqCst), 1);
        assert_eq!(provider_drops.load(Ordering::SeqCst), usize::from(provider));
        assert!(values_weak.upgrade().is_none());
        assert!(creator_weak.upgrade().is_none());
    }

    #[test]
    fn compiled_writer_source_loan_preserves_native_storage_until_actual_last_alias_exit() {
        compiled_writer_source_loan(false);
    }

    #[test]
    fn compiled_writer_source_loan_preserves_provider_storage_until_actual_last_alias_exit() {
        compiled_writer_source_loan(true);
    }

    #[test]
    fn connector_output_owner_moves_into_writer_queue_without_false_oom() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) =
            owner(calls, Some(Arc::clone(&gate)), false, false, config);
        let input = batch(3);
        let retained_bytes = crate::exec::chunk::record_batch_bytes(&input);
        let reserved_bytes = input
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        tracker
            .install_limit_once(i64::try_from(reserved_bytes).unwrap())
            .expect("install exact connector output limit");
        let (mut chunk, actual_reserved_bytes) = connector_accounted_chunk(input.clone(), &tracker);
        assert_eq!(actual_reserved_bytes, reserved_bytes);
        assert_eq!(tracker.current(), i64::try_from(reserved_bytes).unwrap());
        let accounting = chunk.take_memory_lease();

        owner
            .enqueue_with_accounting(input, retained_bytes, accounting, retained_bytes, 0)
            .expect("the connector output owner moves without a second charge");
        drop(chunk);
        assert_eq!(tracker.current(), i64::try_from(retained_bytes).unwrap());
        assert_eq!(tracker.peak(), i64::try_from(reserved_bytes).unwrap());

        gate.notify_one();
        wait_until(|| owner.queued_usage() == (0, 0));
        assert_eq!(tracker.current(), 0);
        owner.request_finish().unwrap();
        wait_until(|| owner.has_output());
        assert!(error.error().is_none());
    }

    #[test]
    fn connector_projection_releases_dead_source_charge_before_later_reservations() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) =
            owner(calls, Some(Arc::clone(&gate)), false, false, config);
        let source = batch(3);
        let projection = batch(3);
        let retained_bytes = crate::exec::chunk::record_batch_bytes(&projection);
        let additional_bytes =
            crate::exec::chunk::record_batch_additional_bytes(&projection, &source);
        assert_eq!(additional_bytes, retained_bytes);
        let source_reserved_bytes = source
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let peak_bytes = source_reserved_bytes
            .checked_add(retained_bytes)
            .expect("test peak fits usize");
        tracker
            .install_limit_once(i64::try_from(peak_bytes).unwrap())
            .expect("install real projection peak limit");
        let (mut source_chunk, actual_reserved_bytes) = connector_accounted_chunk(source, &tracker);
        assert_eq!(actual_reserved_bytes, source_reserved_bytes);

        let accounting = source_chunk.take_memory_lease();
        let reservation = owner
            .try_reserve_input(
                projection.num_rows(),
                retained_bytes,
                accounting,
                0,
                additional_bytes,
            )
            .expect("admit the real source plus projection peak");
        assert_eq!(tracker.current(), i64::try_from(peak_bytes).unwrap());
        drop(source_chunk);
        reservation
            .send(fixture_append_batch(projection))
            .expect("retire dead source bytes before enqueue");
        assert_eq!(tracker.current(), i64::try_from(retained_bytes).unwrap());

        let later_resources =
            ConnectorExecutionResources::from_admitted_ledger(Arc::new(TrackerLedger {
                tracker: Arc::clone(&tracker),
            }));
        let later = later_resources
            .try_reserve(
                ConnectorResourceClass::ReaderOutput,
                u64::try_from(source_reserved_bytes).unwrap(),
            )
            .expect("dead source charge must not cause a later false OOM");
        assert_eq!(tracker.current(), i64::try_from(peak_bytes).unwrap());
        drop(later);

        gate.notify_one();
        wait_until(|| owner.queued_usage() == (0, 0));
        assert_eq!(tracker.current(), 0);
        owner.request_finish().unwrap();
        wait_until(|| owner.has_output());
        assert!(error.error().is_none());
    }

    #[test]
    fn connector_cast_retains_only_shared_null_bitmap_and_new_values() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) =
            owner(calls, Some(Arc::clone(&gate)), false, false, config);
        let source_values = Arc::new(arrow::array::Int32Array::from(vec![Some(1), None, Some(3)]));
        let source = RecordBatch::try_from_iter(vec![(
            "value",
            source_values.clone() as Arc<dyn arrow::array::Array>,
        )])
        .unwrap();
        let projection_values = Arc::new(arrow::array::Int64Array::new(
            vec![1_i64, 0, 3].into(),
            source_values.nulls().cloned(),
        )) as Arc<dyn arrow::array::Array>;
        let projection = RecordBatch::try_from_iter(vec![("value", projection_values)]).unwrap();
        let retained_bytes = crate::exec::chunk::record_batch_bytes(&projection);
        let additional_bytes =
            crate::exec::chunk::record_batch_additional_bytes(&projection, &source);
        let shared_source_bytes =
            crate::exec::chunk::record_batch_shared_owner_bytes(&projection, &source);
        assert!(shared_source_bytes > 0);
        assert_eq!(shared_source_bytes + additional_bytes, retained_bytes);
        let source_reserved_bytes = source
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let peak_bytes = source_reserved_bytes + additional_bytes;
        tracker
            .install_limit_once(i64::try_from(peak_bytes).unwrap())
            .expect("install exact mixed-projection peak limit");
        let (mut source_chunk, _) = connector_accounted_chunk(source, &tracker);

        let accounting = source_chunk.take_memory_lease();
        let reservation = owner
            .try_reserve_input(
                projection.num_rows(),
                retained_bytes,
                accounting,
                shared_source_bytes,
                additional_bytes,
            )
            .expect("admit shared null bitmap plus new values");
        assert_eq!(tracker.current(), i64::try_from(peak_bytes).unwrap());
        drop(source_chunk);
        reservation
            .send(fixture_append_batch(projection))
            .expect("enqueue mixed projection");
        assert_eq!(tracker.current(), i64::try_from(retained_bytes).unwrap());

        let released_source_bytes = source_reserved_bytes - shared_source_bytes;
        let later_resources =
            ConnectorExecutionResources::from_admitted_ledger(Arc::new(TrackerLedger {
                tracker: Arc::clone(&tracker),
            }));
        let later = later_resources
            .try_reserve(
                ConnectorResourceClass::ReaderOutput,
                u64::try_from(released_source_bytes).unwrap(),
            )
            .expect("dead source values must not cause a later false OOM");
        assert_eq!(tracker.current(), i64::try_from(peak_bytes).unwrap());
        drop(later);

        gate.notify_one();
        wait_until(|| owner.queued_usage() == (0, 0));
        assert_eq!(tracker.current(), 0);
        owner.request_finish().unwrap();
        wait_until(|| owner.has_output());
        assert!(error.error().is_none());
    }

    #[test]
    fn rejected_writer_enqueue_releases_moved_connector_output_owner() {
        let calls = Arc::new(Calls::default());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, _error, tracker, _runtime) = owner(calls, None, false, false, config);
        let input = batch(3);
        let retained_bytes = crate::exec::chunk::record_batch_bytes(&input);
        let (mut chunk, reserved_bytes) = connector_accounted_chunk(input.clone(), &tracker);
        assert_eq!(tracker.current(), i64::try_from(reserved_bytes).unwrap());
        let accounting = chunk.take_memory_lease();
        owner.request_finish().unwrap();

        let error = owner
            .enqueue_with_accounting(input, retained_bytes, accounting, reserved_bytes, 0)
            .expect_err("terminal writer must reject connector-owned input");
        assert!(
            matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(cause)
            if matches!(cause.cause(), WriterFailure::AfterTerminal { operation: WriterCommandOperation::Append })),
            "{error:?}"
        );
        drop(chunk);
        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.allocated(), tracker.deallocated());
    }

    #[test]
    fn projected_queue_owns_only_projected_bytes_from_wide_source() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 4096,
            max_batch_rows: 4,
            max_batch_bytes: 4096,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) =
            owner(calls, Some(Arc::clone(&gate)), false, false, config);
        let left = Arc::new(Int32Array::from_iter_values(0..3)) as ArrayRef;
        let right = Arc::new(Int32Array::from_iter_values(10..13)) as ArrayRef;
        let source = RecordBatch::try_from_iter(vec![
            ("left", Arc::clone(&left)),
            ("right", Arc::clone(&right)),
        ])
        .unwrap();
        let projection = RecordBatch::try_from_iter(vec![("v", left)]).unwrap();
        let source_bytes = crate::exec::chunk::record_batch_bytes(&source);
        let projected_bytes = crate::exec::chunk::record_batch_bytes(&projection);
        assert!(source_bytes > projected_bytes);
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            source.schema().as_ref(),
            &[novarocks_types::SlotId(1), novarocks_types::SlotId(2)],
        )
        .unwrap();
        let mut chunk =
            crate::exec::chunk::Chunk::try_new_with_chunk_schema(source, chunk_schema).unwrap();
        chunk.transfer_to(&tracker);
        let accounting = chunk.take_memory_lease();

        owner
            .enqueue_with_accounting(projection, projected_bytes, accounting, projected_bytes, 0)
            .expect("split projected accounting into writer queue");
        drop(chunk);

        assert_eq!(tracker.current(), i64::try_from(projected_bytes).unwrap());
        let queue_tracker = tracker
            .children()
            .into_iter()
            .find(|child| child.label() == "ConnectorWriterQueue")
            .expect("writer queue tracker");
        assert_eq!(
            queue_tracker.current(),
            i64::try_from(projected_bytes).unwrap()
        );

        gate.notify_one();
        wait_until(|| owner.queued_usage() == (0, 0));
        assert_eq!(queue_tracker.current(), 0);
        assert_eq!(tracker.current(), 0);
        owner.request_finish().unwrap();
        wait_until(|| owner.has_output());
        assert!(error.error().is_none());
    }

    #[test]
    fn rejected_enqueue_rolls_accounting_transfer_back_to_input_owner() {
        let calls = Arc::new(Calls::default());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, _error, tracker, _runtime) = owner(calls, None, false, false, config);
        let input_tracker = MemTracker::new_child("input", &tracker);
        let input = batch(3);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            input.schema().as_ref(),
            &[novarocks_types::SlotId(1)],
        )
        .unwrap();
        let mut chunk =
            crate::exec::chunk::Chunk::try_new_with_chunk_schema(input.clone(), chunk_schema)
                .unwrap();
        chunk.transfer_to(&input_tracker);
        let lease = chunk.memory_lease().unwrap();
        owner.request_finish().unwrap();

        let error = owner
            .enqueue_with_accounting(input, bytes, Some(lease.clone()), bytes, 0)
            .expect_err("terminal writer must reject append");
        assert!(
            matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(cause)
            if matches!(cause.cause(), WriterFailure::AfterTerminal { operation: WriterCommandOperation::Append })),
            "{error:?}"
        );
        assert!(Arc::ptr_eq(
            &lease.tracker().expect("native input tracker"),
            &input_tracker
        ));
        assert_eq!(input_tracker.current(), i64::try_from(bytes).unwrap());

        drop(chunk);
        drop(lease);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn full_mailbox_releases_exclusive_split_and_usage_exactly_once() {
        let calls = Arc::new(Calls::default());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let execution = Arc::new(ControlledExecution {
            catalog_handle: catalog_handle(),
            calls,
            append_gate: None,
            fail_append: false,
            fail_abort: false,
        });
        let mut owner =
            AsyncWriterOwner::new(execution, request(), config, Box::new(|rows, _| Ok(rows)));
        let tracker = MemTracker::new_root("full-writer-mailbox");
        owner.set_mem_tracker(Arc::clone(&tracker));
        let (full_sender, _full_receiver) = mpsc::channel(1);
        full_sender
            .try_send(WriterCommand::Finish)
            .expect("prefill test mailbox");
        owner.sender = Some(full_sender);

        let input = batch(3);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            input.schema().as_ref(),
            &[novarocks_types::SlotId(1)],
        )
        .unwrap();
        let mut chunk =
            crate::exec::chunk::Chunk::try_new_with_chunk_schema(input.clone(), chunk_schema)
                .unwrap();
        chunk.transfer_to(&tracker);
        let accounting = chunk.take_memory_lease();

        let error = owner
            .enqueue_with_accounting(input, bytes, accounting, bytes, 0)
            .expect_err("full writer mailbox must reject append");
        assert!(
            matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(cause)
            if matches!(cause.cause(), WriterFailure::MailboxFull { operation: WriterCommandOperation::Append })),
            "{error:?}"
        );
        assert_eq!(owner.queued_usage(), (0, 0));
        assert_eq!(tracker.current(), 0);
        let queue_tracker = tracker
            .children()
            .into_iter()
            .find(|child| child.label() == "ConnectorWriterQueue")
            .expect("writer queue tracker");
        assert_eq!(queue_tracker.current(), 0);
    }

    #[test]
    fn append_failure_aborts_once_and_releases_queue_memory() {
        let calls = Arc::new(Calls::default());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (owner, error, tracker, _runtime) =
            owner(Arc::clone(&calls), None, true, false, config);
        let input = batch(2);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        owner.enqueue(input, bytes).expect("enqueue failing append");
        wait_until(|| owner.is_done());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert_eq!(calls.finished.load(Ordering::Relaxed), 0);
        assert_eq!(owner.queued_usage(), (0, 0));
        assert_eq!(tracker.current(), 0);
        assert!(
            error
                .error()
                .expect("runtime error")
                .detail()
                .contains("injected append failure")
        );
    }

    #[test]
    fn cancellation_retains_inflight_append_then_cooperatively_aborts() {
        let calls = Arc::new(Calls::default());
        let gate = Arc::new(Notify::new());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, tracker, _runtime) = owner(
            Arc::clone(&calls),
            Some(Arc::clone(&gate)),
            false,
            false,
            config,
        );
        let input = batch(2);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        owner.enqueue(input, bytes).expect("enqueue slow append");
        wait_until(|| calls.append_started.load(Ordering::Relaxed) == 1);
        owner.request_abort();
        assert!(!owner.is_done());
        assert!(!owner.actor_exited());
        assert_eq!(owner.queued_usage(), (2, bytes));
        assert!(tracker.current() > 0);
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 0);
        gate.notify_one();
        wait_until(|| owner.actor_exited());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert_eq!(calls.appended.load(Ordering::Relaxed), 2);
        assert_eq!(calls.finished.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.current(), 0);
        assert!(error.error().is_none());
    }

    #[test]
    fn worker_consumes_actor_join_failure_before_exit_is_ready() {
        use crate::exec::pipeline::operator::ExitProgress;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            for panic in [false, true] {
                let mut owner = AsyncWriterOwner::new(
                    Arc::new(ControlledExecution {
                        catalog_handle: catalog_handle(), calls: Arc::new(Calls::default()),
                        append_gate: None, fail_append: false, fail_abort: false,
                    }), request(), AsyncWriterQueueConfig::default(), Box::new(|rows, _| Ok(rows)),
                );
                let (exit, exited) = tokio::sync::oneshot::channel::<()>();
                let handle = tokio::spawn(async move {
                    exited.await.unwrap();
                    panic!("writer wrapper panic");
                });
                let abort = handle.abort_handle();
                owner.install_terminal_actor_for_test(handle);
                assert!(matches!(owner.observe_actor_exit(), ExitObservation {progress:ExitProgress::Pending(_),..}));
                if panic { exit.send(()).unwrap(); } else { abort.abort(); }
                tokio::task::yield_now().await;
                let ExitObservation {progress:ExitProgress::Unresolved(_),failure:Some(ExitFailure::Required(error)),..} = owner.observe_actor_exit() else {
                    panic!("an unqualified installed actor's failed join has no body proof");
                };
                assert!(matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(source)
                    if matches!(source.cause(), WriterFailure::Panicked { .. }) == panic));
                for _ in 0..3 {
                    let observed = owner.observe_actor_exit();
                    assert!(matches!(observed.progress,ExitProgress::Unresolved(_)));
                    assert!(observed.failure.is_none(),"worker consumes original join failure once");
                }
            }
        });
    }

    #[test]
    fn worker_retains_successful_actor_exit_only_after_its_actual_return() {
        use crate::exec::pipeline::operator::ExitProgress;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut owner = AsyncWriterOwner::new(
                Arc::new(ControlledExecution {
                    catalog_handle: catalog_handle(),
                    calls: Arc::new(Calls::default()),
                    append_gate: None,
                    fail_append: false,
                    fail_abort: false,
                }),
                request(),
                AsyncWriterQueueConfig::default(),
                Box::new(|rows, _| Ok(rows)),
            );
            let returned = Arc::new(AtomicBool::new(false));
            struct Returned(Arc<AtomicBool>);
            impl Drop for Returned {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }
            let (release, wait) = tokio::sync::oneshot::channel::<()>();
            let guard = Returned(Arc::clone(&returned));
            let handle = tokio::spawn(async move {
                let _guard = guard;
                wait.await.unwrap();
            });
            owner.install_terminal_actor_for_test(handle);
            assert!(
                owner.is_done(),
                "logical terminal notification precedes real exit"
            );
            assert!(!owner.actor_exited());
            assert!(!returned.load(Ordering::Acquire));
            assert!(matches!(
                owner.observe_actor_exit(),
                ExitObservation {
                    progress: ExitProgress::Pending(_),
                    ..
                }
            ));
            release.send(()).unwrap();
            while !owner.actor_exited() {
                tokio::task::yield_now().await;
            }
            for _ in 0..3 {
                assert!(matches!(
                    owner.observe_actor_exit(),
                    ExitObservation {
                        progress: ExitProgress::Exited,
                        failure: None,
                        ..
                    }
                ));
                assert!(returned.load(Ordering::Acquire));
            }
        });
    }

    #[test]
    fn cancellation_during_finish_retains_exit_and_never_publishes_success() {
        struct FinishExecution {
            catalog: CatalogHandle,
            started: Arc<AtomicBool>,
            gate: Arc<Notify>,
            aborted: Arc<AtomicUsize>,
        }
        struct FinishWriter {
            started: Arc<AtomicBool>,
            gate: Arc<Notify>,
            aborted: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for FinishExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog
            }
            async fn open_writer(
                &self,
                _: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(FinishWriter {
                    started: Arc::clone(&self.started),
                    gate: Arc::clone(&self.gate),
                    aborted: Arc::clone(&self.aborted),
                }))
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for FinishWriter {
            async fn append(&mut self, _: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                self.started.store(true, Ordering::Release);
                self.gate.notified().await;
                Ok(Vec::new())
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.aborted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let started = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let aborted = Arc::new(AtomicUsize::new(0));
        let mut owner = AsyncWriterOwner::new(
            Arc::new(FinishExecution {
                catalog: catalog_handle(),
                started: Arc::clone(&started),
                gate: Arc::clone(&gate),
                aborted: Arc::clone(&aborted),
            }),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .unwrap();
        owner.request_finish().unwrap();
        wait_until(|| started.load(Ordering::Acquire));
        owner.request_abort();
        assert!(!owner.is_done());
        assert!(!owner.actor_exited());
        assert!(owner.take_output().is_none());
        assert_eq!(aborted.load(Ordering::Relaxed), 0);
        gate.notify_one();
        wait_until(|| owner.actor_exited());
        assert!(owner.is_done());
        assert!(owner.take_output().is_none());
        assert_eq!(aborted.load(Ordering::Relaxed), 1);
        assert!(error.error().is_none());
    }

    #[test]
    fn cancellation_drops_cancel_safe_open_without_an_unreachable_writer_abort() {
        struct OpenDropGuard(Arc<AtomicUsize>);

        impl Drop for OpenDropGuard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        struct CancelSafeOpenExecution {
            catalog_handle: CatalogHandle,
            started: Arc<AtomicUsize>,
            effects: Arc<AtomicUsize>,
            dropped: Arc<AtomicUsize>,
            gate: Arc<Notify>,
        }

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for CancelSafeOpenExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog_handle
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                let _drop_guard = OpenDropGuard(Arc::clone(&self.dropped));
                self.started.fetch_add(1, Ordering::Relaxed);
                self.gate.notified().await;
                // The fixture deliberately models the SPI rule: effects may
                // begin only after open has completed and returned a writer.
                self.effects.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new(ControlledWriter {
                    calls: Arc::new(Calls::default()),
                    append_gate: None,
                    fail_append: false,
                    fail_abort: false,
                }))
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let effects = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let execution = Arc::new(CancelSafeOpenExecution {
            catalog_handle: catalog_handle(),
            started: Arc::clone(&started),
            effects: Arc::clone(&effects),
            dropped: Arc::clone(&dropped),
            gate: Arc::new(Notify::new()),
        });
        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let mut owner = AsyncWriterOwner::new(
            execution,
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .expect("bind async writer");
        wait_until(|| started.load(Ordering::Relaxed) == 1);

        owner.request_abort();
        wait_until(|| owner.is_done());

        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(effects.load(Ordering::Relaxed), 0);
        assert!(error.error().is_none());
    }

    #[test]
    fn append_panic_aborts_exactly_once_and_is_not_a_clean_empty_finish() {
        struct PanickingExecution(CatalogHandle, Arc<Calls>);

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for PanickingExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.0
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(PanickingWriter(Arc::clone(&self.1))))
            }
        }

        struct PanickingWriter(Arc<Calls>);

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for PanickingWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                panic!("injected provider panic")
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Ok(Vec::new())
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.0.aborted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let tracker = MemTracker::new_root("panicking-writer-test");
        let calls = Arc::new(Calls::default());
        let mut owner = AsyncWriterOwner::new(
            Arc::new(PanickingExecution(catalog_handle(), Arc::clone(&calls))),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner.set_mem_tracker(Arc::clone(&tracker));
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .expect("bind async writer");
        let input = batch(2);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        owner.enqueue(input, bytes).expect("enqueue panic batch");

        wait_until(|| owner.is_done());
        assert!(!owner.has_output());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert_eq!(owner.queued_usage(), (0, 0));
        assert_eq!(tracker.current(), 0);
        assert!(
            error
                .error()
                .expect("actor panic must be recorded")
                .detail()
                .contains("append connector writer batch panicked: injected provider panic")
        );
    }

    #[test]
    fn finish_panic_cooperatively_aborts_exactly_once_and_fails() {
        struct PanickingFinishExecution {
            catalog_handle: CatalogHandle,
            calls: Arc<Calls>,
        }

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for PanickingFinishExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog_handle
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(PanickingFinishWriter(Arc::clone(&self.calls))))
            }
        }

        struct PanickingFinishWriter(Arc<Calls>);

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for PanickingFinishWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                panic!("injected finish panic")
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.0.aborted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let calls = Arc::new(Calls::default());
        let mut owner = AsyncWriterOwner::new(
            Arc::new(PanickingFinishExecution {
                catalog_handle: catalog_handle(),
                calls: Arc::clone(&calls),
            }),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .expect("bind async writer");
        owner.request_finish().expect("request finish");

        wait_until(|| owner.is_done());
        assert!(!owner.has_output());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert!(
            error
                .error()
                .expect("finish panic must be recorded")
                .detail()
                .contains("finish connector writer panicked: injected finish panic")
        );
    }

    #[test]
    fn abort_panic_is_cleanup_after_actual_future_drop_and_abort_exit() {
        struct PanickingAbortExecution {
            catalog_handle: CatalogHandle,
            opened: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for PanickingAbortExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog_handle
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                self.opened.store(true, Ordering::Release);
                Ok(Box::new(PanickingAbortWriter))
            }
        }

        struct PanickingAbortWriter;

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for PanickingAbortWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Ok(Vec::new())
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                panic!("injected abort panic")
            }
        }

        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let opened = Arc::new(AtomicBool::new(false));
        let mut owner = AsyncWriterOwner::new(
            Arc::new(PanickingAbortExecution {
                catalog_handle: catalog_handle(),
                opened: Arc::clone(&opened),
            }),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .expect("bind async writer");
        wait_until(|| opened.load(Ordering::Acquire));
        owner.request_abort();

        wait_until(|| owner.is_done());
        assert!(
            error.error().is_none(),
            "abort cleanup cannot create an execution origin"
        );
        wait_until(|| owner.actor_exited());
        assert!(matches!(
            owner.observe_actor_exit().progress,
            ExitProgress::Exited
        ));
        assert_eq!(owner.shared.cleanup_diagnostics.lock().unwrap().len(), 1);
    }

    #[test]
    fn failure_notifications_do_not_hold_the_private_writer_lock() {
        let shared = Arc::new(WriterShared::<()>::new());
        let signal = RuntimeErrorState::default();
        let observed = Arc::new(AtomicUsize::new(0));
        for observable in [shared.observable.clone(), signal.stop_observable()] {
            let weak = Arc::downgrade(&shared);
            let observed = Arc::clone(&observed);
            observable.add_observer(Arc::new(move || {
                let shared = weak
                    .upgrade()
                    .expect("writer remains alive during publication");
                assert!(
                    shared.error.try_lock().is_ok(),
                    "notifications must allow reading the writer failure"
                );
                observed.fetch_add(1, Ordering::Relaxed);
            }));
        }
        shared.set_error(WriterFailure::MissingTerminalOutcome.into(), &signal);
        assert_eq!(observed.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn originating_failure_is_visible_while_abort_waits_and_cleanup_cannot_replace_it() {
        struct GatedAbort(Arc<tokio::sync::Semaphore>);
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for GatedAbort {
            async fn append(&mut self, _: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Ok(Vec::new())
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.0.acquire().await.unwrap().forget();
                Err(ConnectorError::new(
                    ConnectorErrorKind::PermissionDenied,
                    "private-cleanup-credential-marker",
                ))
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let mut writer = GatedAbort(Arc::clone(&gate));
            let shared = WriterShared::<()>::new();
            let runtime_error = RuntimeErrorState::default();
            let source =
                ConnectorError::new(ConnectorErrorKind::CorruptData, "original finish failure");
            let primary: ExecutionFailure =
                ConnectorCallFailure::new(ConnectorOperation::FinishWriter, source).into();
            let mut failure = Box::pin(fail_with_abort(
                &mut writer,
                primary.clone(),
                &shared,
                AsyncWriterQueueConfig::default(),
                &runtime_error,
            ));
            assert!(futures::poll!(failure.as_mut()).is_pending());
            assert_eq!(runtime_error.error(), Some(primary.clone()));
            assert_eq!(*shared.error.lock().unwrap(), Some(primary.clone()));
            assert!(
                !shared.done.load(Ordering::Acquire),
                "failure publication is not actual owner exit"
            );
            gate.add_permits(1);
            assert!(futures::poll!(failure.as_mut()).is_pending());
            assert!(shared.unresolved.load(Ordering::Acquire));
            assert!(shared.abort_failure.lock().unwrap().is_some());
            assert_eq!(runtime_error.error(), Some(primary.clone()));
            assert_eq!(*shared.error.lock().unwrap(), Some(primary));
            let diagnostics = shared.cleanup_diagnostics.lock().unwrap();
            assert_eq!(diagnostics.len(), 1);
            assert!(diagnostics[0].as_str().contains("reason 4:3"));
            assert!(
                !diagnostics[0]
                    .as_str()
                    .contains("private-cleanup-credential-marker")
            );
        });
    }

    #[test]
    fn accepted_row_count_overflow_is_rejected_instead_of_saturated() {
        assert_eq!(checked_accepted_rows(41, 1).expect("normal count"), 42);
        assert_eq!(
            checked_accepted_rows(u64::MAX, 1)
                .expect_err("overflow")
                .cause(),
            &crate::runtime::fragment::ExecutionFailureCause::Writer(
                WriterFailure::AcceptedRowsOverflow {
                    current: u64::MAX,
                    incoming: 1
                }
                .into()
            )
        );
    }

    #[test]
    fn abort_failure_is_observable_and_finish_is_never_invoked() {
        let calls = Arc::new(Calls::default());
        let config = AsyncWriterQueueConfig {
            max_batches: 1,
            max_rows: 4,
            max_bytes: 1024,
            max_batch_rows: 4,
            max_batch_bytes: 1024,
            abort_timeout: Duration::from_secs(1),
        };
        let (mut owner, error, _tracker, _runtime) =
            owner(Arc::clone(&calls), None, false, true, config);
        wait_until(|| calls.opened.load(Ordering::Relaxed) == 1);
        owner.request_abort();
        wait_until(|| owner.shared.unresolved.load(Ordering::Acquire));
        assert!(!owner.is_done());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert_eq!(calls.finished.load(Ordering::Relaxed), 0);
        assert!(
            error.error().is_none(),
            "abort returned error is secondary cleanup"
        );
        assert!(!owner.actor_exited());
        assert!(!owner.shared.frame_exited.load(Ordering::Acquire));
        let original = owner.shared.abort_failure.lock().unwrap().clone().unwrap();
        assert!(
            matches!(original.cause(), crate::runtime::fragment::ExecutionFailureCause::Connector(source)
            if source.operation() == ConnectorOperation::AbortWriter)
        );
        assert!(matches!(
            owner.observe_actor_exit().progress,
            ExitProgress::Unresolved(_)
        ));
        for _ in 0..3 {
            assert!(matches!(
                owner.observe_actor_exit().progress,
                ExitProgress::Unresolved(_)
            ));
        }
        assert_eq!(
            calls.aborted.load(Ordering::Relaxed),
            1,
            "failed call is never repolled/reissued"
        );
        assert_eq!(owner.shared.cleanup_diagnostics.lock().unwrap().len(), 1);
    }

    #[test]
    fn abort_deadline_reports_failure_without_discarding_actual_exit() {
        struct HangingAbortExecution(CatalogHandle, Arc<AtomicBool>, Arc<Notify>);

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for HangingAbortExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.0
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                self.1.store(true, Ordering::Release);
                Ok(Box::new(HangingAbortWriter(Arc::clone(&self.2))))
            }
        }

        struct HangingAbortWriter(Arc<Notify>);

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for HangingAbortWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Ok(Vec::new())
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.0.notified().await;
                Ok(())
            }
        }

        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let opened = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let mut owner = AsyncWriterOwner::new(
            Arc::new(HangingAbortExecution(
                catalog_handle(),
                Arc::clone(&opened),
                Arc::clone(&gate),
            )),
            request(),
            AsyncWriterQueueConfig {
                abort_timeout: Duration::from_millis(20),
                ..AsyncWriterQueueConfig::default()
            },
            Box::new(|rows, _| Ok(rows)),
        );
        owner
            .bind(runtime.services().sink_io().clone(), Arc::clone(&error))
            .expect("bind async writer");
        wait_until(|| opened.load(Ordering::Acquire));
        let started = Instant::now();
        owner.request_abort();
        wait_until(|| !owner.shared.cleanup_diagnostics.lock().unwrap().is_empty());
        assert!(!owner.is_done());
        assert!(!owner.actor_exited());

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            error.error().is_none(),
            "cleanup deadline cannot originate Task failure"
        );
        assert_eq!(owner.shared.cleanup_diagnostics.lock().unwrap().len(), 1);
        assert!(matches!(
            owner.observe_actor_exit().progress,
            ExitProgress::Pending(_)
        ));
        let expected: ExecutionFailure = WriterFailure::AbortTimeout {
            timeout: Duration::from_millis(20),
        }
        .into();
        assert_eq!(
            owner.shared.cleanup_diagnostics.lock().unwrap()[0],
            expected.safe_detail()
        );
        gate.notify_one();
        wait_until(|| owner.actor_exited());
        assert!(owner.is_done());
        assert!(owner.take_output().is_none());
    }

    #[test]
    fn finish_is_idempotently_requested_and_invoked_exactly_once() {
        let calls = Arc::new(Calls::default());
        let (mut owner, error, _tracker, _runtime) = owner(
            Arc::clone(&calls),
            None,
            false,
            false,
            AsyncWriterQueueConfig::default(),
        );
        owner.request_finish().expect("first finish request");
        owner.request_finish().expect("second finish request");
        wait_until(|| owner.has_output());
        assert_eq!(owner.take_output(), Some(0));
        assert_eq!(calls.finished.load(Ordering::Relaxed), 1);
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 0);
        assert!(error.error().is_none());
    }
    #[test]
    fn actual_actor_finish_refines_peer_but_abort_failure_only_reports_cleanup() {
        use crate::runtime::task_control::test_support::Owner;
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        };
        struct Execution {
            catalog: CatalogHandle,
            opened: Arc<AtomicBool>,
            calls: Arc<Calls>,
            fail_finish: bool,
        }
        struct Writer {
            calls: Arc<Calls>,
            fail_finish: bool,
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for Execution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog
            }
            async fn open_writer(
                &self,
                _: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                self.opened.store(true, Ordering::Release);
                Ok(Box::new(Writer {
                    calls: self.calls.clone(),
                    fail_finish: self.fail_finish,
                }))
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for Writer {
            async fn append(&mut self, _: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                self.calls.finished.fetch_add(1, Ordering::SeqCst);
                if self.fail_finish {
                    Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "original finish corruption",
                    ))
                } else {
                    Ok(vec![])
                }
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.calls.aborted.fetch_add(1, Ordering::SeqCst);
                Err(ConnectorError::new(
                    ConnectorErrorKind::PermissionDenied,
                    "independent abort failure",
                ))
            }
        }
        for required in [true, false] {
            let runtime = runtime();
            let task = Owner::new();
            task.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
            let accepted = task.accepted().unwrap();
            let signal = Arc::new(RuntimeErrorState::for_task(Arc::new(
                task.view(TaskFailurePhase::Execution),
            )));
            let calls = Arc::new(Calls::default());
            let opened = Arc::new(AtomicBool::new(false));
            let mut owner = AsyncWriterOwner::new(
                Arc::new(Execution {
                    catalog: catalog_handle(),
                    opened: opened.clone(),
                    calls: calls.clone(),
                    fail_finish: required,
                }),
                request(),
                AsyncWriterQueueConfig::default(),
                Box::new(|rows, _| Ok(rows)),
            );
            owner
                .bind(runtime.services().sink_io().clone(), signal.clone())
                .unwrap();
            wait_until(|| opened.load(Ordering::Acquire));
            if required {
                owner.request_finish().unwrap();
            } else {
                owner.request_abort();
            }
            wait_until(|| owner.shared.unresolved.load(Ordering::Acquire));
            assert!(!owner.is_done());
            assert!(!owner.actor_exited());
            assert!(!owner.shared.frame_exited.load(Ordering::Acquire));
            assert!(owner.shared.abort_failure.lock().unwrap().is_some());
            let observation = owner.observe_actor_exit();
            assert!(matches!(observation.progress, ExitProgress::Unresolved(_)));
            assert!(
                observation.failure.is_none(),
                "actor already reported original events exactly once"
            );
            for _ in 0..3 {
                assert!(owner.observe_actor_exit().failure.is_none());
            }
            assert_eq!(calls.aborted.load(Ordering::SeqCst), 1);
            assert_eq!(calls.finished.load(Ordering::SeqCst), usize::from(required));
            let current = task.accepted().unwrap();
            if required {
                let original = owner.error().expect("actual required finish origin");
                assert!(
                    current
                        .originating_failure()
                        .unwrap()
                        .same_occurrence(&original)
                );
                assert!(!Arc::ptr_eq(&accepted, &current));
            } else {
                assert!(Arc::ptr_eq(&accepted, &current));
                assert!(owner.error().is_none());
            }
            assert_eq!(
                owner.shared.cleanup_diagnostics.lock().unwrap().len(),
                1,
                "the actual abort error has one cleanup consumer"
            );
        }
    }

    #[test]
    fn lifecycle_poll_panic_drops_actual_future_once_before_reusing_writer_for_abort() {
        struct FutureDrop {
            polls: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
        }
        impl std::future::Future for FutureDrop {
            type Output = Result<(), ConnectorError>;
            fn poll(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                self.polls.fetch_add(1, Ordering::SeqCst);
                panic!("actual future poll panic");
            }
        }
        impl Drop for FutureDrop {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let polls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let shared = WriterShared::<()>::new();
            let signal = RuntimeErrorState::default();
            let result = await_lifecycle(
                || FutureDrop {
                    polls: polls.clone(),
                    drops: drops.clone(),
                },
                &shared,
                &signal,
                WriterPhase::Append,
            )
            .await;
            let original = result.unwrap_err();
            assert!(signal.error().unwrap().same_occurrence(&original));
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(!shared.unresolved.load(Ordering::Acquire));
            assert!(signal.local_exit_fault().is_none());
        });
    }

    #[test]
    fn actual_append_opaque_secondary_retains_original_batch_and_queue_lease_without_repoll_or_false_join()
     {
        struct Secondary {
            batch: std::mem::ManuallyDrop<RecordBatch>,
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for Secondary {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                unsafe { std::mem::ManuallyDrop::drop(&mut self.batch) };
            }
        }
        struct Primary {
            batch: std::mem::ManuallyDrop<RecordBatch>,
            dropped: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl Drop for Primary {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                let batch = unsafe { std::mem::ManuallyDrop::take(&mut self.batch) };
                std::panic::panic_any(Secondary {
                    batch: std::mem::ManuallyDrop::new(batch),
                    dropped: self.secondary.clone(),
                });
            }
        }
        struct Execution {
            catalog: CatalogHandle,
            calls: Arc<Calls>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        struct Writer {
            calls: Arc<Calls>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for Execution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog
            }
            async fn open_writer(
                &self,
                _: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(Writer {
                    calls: self.calls.clone(),
                    primary: self.primary.clone(),
                    secondary: self.secondary.clone(),
                }))
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for Writer {
            async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
                self.calls.append_started.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(Primary {
                    batch: std::mem::ManuallyDrop::new(batch),
                    dropped: self.primary.clone(),
                    secondary: self.secondary.clone(),
                });
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                self.calls.finished.fetch_add(1, Ordering::SeqCst);
                Ok(vec![])
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.calls.aborted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let runtime = runtime();
        let signal = Arc::new(RuntimeErrorState::default());
        let tracker = MemTracker::new_root("actual-unknown-writer-custody");
        let calls = Arc::new(Calls::default());
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let mut owner = AsyncWriterOwner::new(
            Arc::new(Execution {
                catalog: catalog_handle(),
                calls: calls.clone(),
                primary: primary.clone(),
                secondary: secondary.clone(),
            }),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner.set_mem_tracker(tracker.clone());
        owner
            .bind(runtime.services().sink_io().clone(), signal.clone())
            .unwrap();
        let batch = batch(3);
        let original = Arc::downgrade(batch.column(0));
        let bytes = crate::exec::chunk::record_batch_bytes(&batch);
        owner.enqueue(batch, bytes).unwrap();
        wait_until(|| signal.local_exit_fault().is_some());
        wait_until(|| calls.aborted.load(Ordering::SeqCst) == 1);
        assert_eq!(calls.append_started.load(Ordering::SeqCst), 1);
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(secondary.load(Ordering::SeqCst), 0);
        let original_cause = signal
            .error()
            .expect("the actual append panic is Required before opaque disposal");
        assert!(owner.error().unwrap().same_occurrence(&original_cause));
        assert!(!owner.can_accept());
        let refusal = match owner.try_reserve_input(1, 0, None, 0, 0) {
            Err(cause) => cause,
            Ok(_) => panic!("the original unknown append cannot accept new input"),
        };
        assert!(
            refusal.same_occurrence(&original_cause),
            "stored Required takes precedence over the unknown-exit refusal"
        );
        assert_eq!(owner.queued_usage(), (3, bytes));
        let fault = signal.local_exit_fault();
        assert!(!owner.actor_exited());
        assert!(!owner.is_done());
        assert_eq!(owner.queued_usage(), (3, bytes));
        assert_eq!(tracker.current(), i64::try_from(bytes).unwrap());
        assert_eq!(
            original
                .upgrade()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 1, 2]
        );
        owner.request_abort();
        for _ in 0..3 {
            let observed = owner.observe_actor_exit();
            assert!(matches!(observed.progress, ExitProgress::Unresolved(_)));
            assert!(
                observed.failure.is_none(),
                "the original actor has already consumed the actual Required event"
            );
        }
        assert_eq!(
            calls.aborted.load(Ordering::SeqCst),
            1,
            "the original provider future retired; independent abort runs once while opaque custody remains live"
        );
        assert_eq!(signal.local_exit_fault(), fault);
        assert!(signal.error().unwrap().same_occurrence(&original_cause));
        let retained = Arc::downgrade(&owner.shared);
        drop(owner);
        drop(runtime);
        assert!(
            retained.upgrade().is_some(),
            "executor shutdown keeps the original unknown actor frame in its own custody"
        );
        assert_eq!(secondary.load(Ordering::SeqCst), 0);
        assert_eq!(tracker.current(), i64::try_from(bytes).unwrap());
        assert_eq!(original.upgrade().unwrap().len(), 3);
    }
    #[test]
    fn real_origin_notifications_save_original_opaque_custody_before_runtime_or_readiness_panic() {
        struct BatchSecondary {
            batch: std::mem::ManuallyDrop<RecordBatch>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for BatchSecondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                unsafe { std::mem::ManuallyDrop::drop(&mut self.batch) };
            }
        }
        struct BatchPrimary {
            batch: std::mem::ManuallyDrop<RecordBatch>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl Drop for BatchPrimary {
            fn drop(&mut self) {
                self.primary.fetch_add(1, Ordering::SeqCst);
                let batch = unsafe { std::mem::ManuallyDrop::take(&mut self.batch) };
                std::panic::panic_any(BatchSecondary {
                    batch: std::mem::ManuallyDrop::new(batch),
                    drops: self.secondary.clone(),
                });
            }
        }
        struct NoticeData {
            values: Vec<u64>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for NoticeData {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct NoticeSecondary {
            _data: Arc<NoticeData>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for NoticeSecondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct NoticePrimary {
            data: std::mem::ManuallyDrop<Arc<NoticeData>>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
            signal: std::sync::Weak<RuntimeErrorState>,
        }
        impl Drop for NoticePrimary {
            fn drop(&mut self) {
                assert!(
                    self.signal.upgrade().unwrap().local_exit_fault().is_some(),
                    "shared original Health was committed before foreign notification payload Drop"
                );
                self.primary.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                std::panic::panic_any(NoticeSecondary {
                    _data: data,
                    drops: self.secondary.clone(),
                });
            }
        }
        struct Execution {
            catalog: CatalogHandle,
            calls: Arc<Calls>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        struct Writer {
            calls: Arc<Calls>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for Execution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog
            }
            async fn open_writer(
                &self,
                _: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(Writer {
                    calls: self.calls.clone(),
                    primary: self.primary.clone(),
                    secondary: self.secondary.clone(),
                }))
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for Writer {
            async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
                self.calls.append_started.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(BatchPrimary {
                    batch: std::mem::ManuallyDrop::new(batch),
                    primary: self.primary.clone(),
                    secondary: self.secondary.clone(),
                });
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                self.calls.finished.fetch_add(1, Ordering::SeqCst);
                Ok(vec![])
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.calls.aborted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        for runtime_notice in [true, false] {
            let runtime = runtime();
            let signal = Arc::new(RuntimeErrorState::default());
            let tracker = MemTracker::new_root("actual notification panic custody");
            let calls = Arc::new(Calls::default());
            let batch_primary = Arc::new(AtomicUsize::new(0));
            let batch_secondary = Arc::new(AtomicUsize::new(0));
            let mut owner = AsyncWriterOwner::new(
                Arc::new(Execution {
                    catalog: catalog_handle(),
                    calls: calls.clone(),
                    primary: batch_primary.clone(),
                    secondary: batch_secondary.clone(),
                }),
                request(),
                AsyncWriterQueueConfig::default(),
                Box::new(|rows, _| Ok(rows)),
            );
            owner.set_mem_tracker(tracker.clone());
            let notice_primary = Arc::new(AtomicUsize::new(0));
            let notice_secondary = Arc::new(AtomicUsize::new(0));
            let notice_data_drops = Arc::new(AtomicUsize::new(0));
            let notice_data = Arc::new(NoticeData {
                values: vec![31, 37, 41],
                drops: notice_data_drops.clone(),
            });
            let data_weak = Arc::downgrade(&notice_data);
            let payload = Arc::new(Mutex::new(Some(NoticePrimary {
                data: std::mem::ManuallyDrop::new(notice_data),
                primary: notice_primary.clone(),
                secondary: notice_secondary.clone(),
                signal: Arc::downgrade(&signal),
            })));
            let actual = batch(3);
            let array_weak = Arc::downgrade(actual.column(0));
            let bytes = crate::exec::chunk::record_batch_bytes(&actual);
            let shared = Arc::downgrade(&owner.shared);
            let signal_weak = Arc::downgrade(&signal);
            let entered = Arc::new(AtomicUsize::new(0));
            let callback_entered = entered.clone();
            let array_for_callback = array_weak.clone();
            let observable = if runtime_notice {
                signal.stop_observable()
            } else {
                owner.observable()
            };
            observable.add_observer(Arc::new(move || {
                let shared = shared.upgrade().unwrap();
                let signal = signal_weak.upgrade().unwrap();
                let ready = if runtime_notice {
                    shared.panic_primary.lock().unwrap()[WriterPhase::Append as usize * 2].is_some()
                } else {
                    shared.panic_payloads.lock().unwrap()[WriterPhase::Append as usize * 2]
                        .is_some()
                };
                if !ready {
                    return;
                }
                let payload = payload.lock().unwrap().take();
                let Some(payload) = payload else {
                    return;
                };
                callback_entered.fetch_add(1, Ordering::SeqCst);
                let original = shared
                    .error
                    .try_lock()
                    .expect("no private lock spans original notification")
                    .as_ref()
                    .unwrap()
                    .clone();
                assert!(
                    signal.error().unwrap().same_occurrence(&original),
                    "actual originating source committed before synchronous reentry"
                );
                assert_eq!(
                    array_for_callback
                        .upgrade()
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    &[0, 1, 2]
                );
                if runtime_notice {
                    assert!(
                        shared.panic_primary.try_lock().unwrap()[WriterPhase::Append as usize * 2]
                            .is_some(),
                        "primary Any is in its original owner before Runtime stop notification"
                    );
                } else {
                    assert!(
                        shared.panic_payloads.try_lock().unwrap()[WriterPhase::Append as usize * 2]
                            .is_some(),
                        "secondary Any is saved before Health's readiness notification"
                    );
                }
                std::panic::panic_any(payload);
            }));
            owner
                .bind(runtime.services().sink_io().clone(), signal.clone())
                .unwrap();
            owner.enqueue(actual, bytes).unwrap();
            let notification = if runtime_notice {
                WriterNotification::Publication
            } else {
                WriterNotification::Readiness
            };
            wait_until(|| {
                owner.shared.notification_secondary.lock().unwrap()[notification as usize].is_some()
                    && owner.shared.panic_payloads.lock().unwrap()[WriterPhase::Append as usize * 2]
                        .is_some()
            });
            assert_eq!(entered.load(Ordering::SeqCst), 1);
            assert_eq!(notice_primary.load(Ordering::SeqCst), 1);
            assert_eq!(notice_secondary.load(Ordering::SeqCst), 0);
            assert_eq!(batch_primary.load(Ordering::SeqCst), 1);
            assert_eq!(batch_secondary.load(Ordering::SeqCst), 0);
            assert_eq!(calls.append_started.load(Ordering::SeqCst), 1);
            let original = signal.error().unwrap();
            assert!(owner.error().unwrap().same_occurrence(&original));
            let health = signal.local_exit_fault();
            assert!(health.is_some());
            assert_eq!(owner.queued_usage(), (3, bytes));
            assert_eq!(tracker.current(), i64::try_from(bytes).unwrap());
            assert_eq!(data_weak.upgrade().unwrap().values, [31, 37, 41]);
            assert_eq!(notice_data_drops.load(Ordering::SeqCst), 0);
            for _ in 0..3 {
                let observed = owner.observe_actor_exit();
                assert!(matches!(observed.progress, ExitProgress::Unresolved(_)));
                assert!(observed.failure.is_none());
                assert!(observed.unresolved_exit.is_none());
            }
            wait_until(|| calls.aborted.load(Ordering::SeqCst) == 1);
            owner.request_abort();
            assert_eq!(calls.aborted.load(Ordering::SeqCst), 1);
            assert_eq!(signal.local_exit_fault(), health);
            assert!(signal.error().unwrap().same_occurrence(&original));
            let shared_weak = Arc::downgrade(&owner.shared);
            drop(owner);
            drop(runtime);
            assert!(shared_weak.upgrade().is_some());
            assert_eq!(array_weak.upgrade().unwrap().len(), 3);
            assert_eq!(tracker.current(), i64::try_from(bytes).unwrap());
            assert_eq!(data_weak.upgrade().unwrap().values, [31, 37, 41]);
            assert_eq!(notice_data_drops.load(Ordering::SeqCst), 0);
            assert_eq!(notice_secondary.load(Ordering::SeqCst), 0);
            assert_eq!(batch_secondary.load(Ordering::SeqCst), 0);
        }
    }
    #[test]
    fn never_polled_executor_cancellation_keeps_the_original_accepted_queue_and_typed_funding() {
        use crate::exec::chunk::{Chunk, typed_fixture};
        use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let runtime = runtime();
        let executor = runtime.services().sink_io().clone();
        let (entered, entering) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        // This is the real one-worker sink executor. The original writer task
        // cannot run before its external cancellation request is installed.
        let blocker = executor.spawn(async move {
            entered.send(()).unwrap();
            let _ = released.recv();
        });
        let reached = entering.recv_timeout(Duration::from_secs(5));
        if reached.is_err() {
            drop(release);
            let _ = futures::executor::block_on(blocker);
            panic!("the actual sink executor did not reach its gate");
        }
        let calls = Arc::new(Calls::default());
        let signal = Arc::new(RuntimeErrorState::default());
        let tracker = MemTracker::new_root("never-polled accepted writer funding");
        let mut owner = AsyncWriterOwner::new(
            Arc::new(ControlledExecution {
                catalog_handle: catalog_handle(),
                calls: calls.clone(),
                append_gate: None,
                fail_append: false,
                fail_abort: false,
            }),
            request(),
            AsyncWriterQueueConfig::default(),
            Box::new(|rows, _| Ok(rows)),
        );
        owner.set_mem_tracker(tracker.clone());
        owner.bind(executor.clone(), signal.clone()).unwrap();
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            signal.clone(),
        )
        .unwrap();
        let original = batch(3);
        let array = Arc::downgrade(original.column(0));
        let declared = typed_fixture::layout(
            original.schema(),
            &[novarocks_types::SlotId::new(1)],
            &[FunctionValueType::new(DataType::Int32, false)],
        );
        let schema = factory.create_compiled_chunk_schema(&declared).unwrap();
        let input_owner = factory.begin_typed_construction(&schema).unwrap();
        let input = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            original,
            schema.clone(),
            ValueTypeStorageLifetime::pure(),
            input_owner.storage_lifetime(),
            |layout| input_owner.request(layout),
            |step| input_owner.observe(0, &[], step),
        )
        .unwrap();
        let projection = input.into_batch_projection();
        let bytes = crate::exec::chunk::record_batch_bytes(projection.record_batch());
        let reservation = owner.try_reserve_input(3, bytes, None, 0, bytes).unwrap();
        let copy_owner = factory
            .begin_typed_construction(projection.chunk_schema())
            .unwrap();
        let append = WriterAppendBatch::try_new(projection, copy_owner.as_ref()).unwrap();
        drop((copy_owner, input_owner, schema, declared, factory));
        reservation.send(append).unwrap();
        let funding_before = tracker.current();
        let queued_before = owner.queued_usage();
        let handle = owner.join.as_ref().unwrap().abort_handle();
        handle.abort();
        drop(release);
        // Release and really join the gate before any behavioral assertion.
        let gate_exit = futures::executor::block_on(blocker);
        wait_until(|| owner.join.as_ref().unwrap().is_finished());
        let first = owner.observe_actor_exit();
        assert!(gate_exit.is_ok());
        assert_eq!(
            calls.opened.load(Ordering::SeqCst),
            0,
            "the provider was never polled"
        );
        assert_eq!(calls.append_started.load(Ordering::SeqCst), 0);
        assert_eq!(
            calls.aborted.load(Ordering::SeqCst),
            0,
            "no writer was constructed to abort"
        );
        assert_eq!(queued_before, (3, bytes));
        assert!(
            funding_before > i64::try_from(bytes).unwrap(),
            "the real schema/projection/copy and queue funding were accepted"
        );
        assert_eq!(tracker.current(), funding_before);
        assert_eq!(owner.queued_usage(), queued_before);
        assert!(
            !owner.can_accept(),
            "unknown exit closes fresh input admission without claiming terminal"
        );
        let refusal = match owner.try_reserve_input(1, 0, None, 0, 0) {
            Err(cause) => cause,
            Ok(_) => panic!("unknown actor cannot fund new input"),
        };
        assert!(
            matches!(refusal.cause(), crate::runtime::fragment::ExecutionFailureCause::Writer(source) if matches!(source.cause(), WriterFailure::ExitUnresolved))
        );
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        assert_eq!(
            refusal.failure_fact(),
            FailureFact::new(
                FailureClass::Unexpected,
                FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(595).unwrap())
            )
        );
        assert_eq!(owner.queued_usage(), queued_before);
        assert_eq!(tracker.current(), funding_before);
        assert!(matches!(first.progress, ExitProgress::Unresolved(_)));
        assert!(
            first.failure.is_none(),
            "the original guard already reported cleanup and health"
        );
        assert!(
            signal.error().is_none(),
            "unstarted cancellation is not an OpenWriter failure"
        );
        assert!(signal.local_exit_fault().is_some());
        assert!(!owner.shared.frame_exited.load(Ordering::Acquire));
        assert!(owner.shared.unknown_actor.lock().unwrap().is_some());
        assert_eq!(
            array
                .upgrade()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 1, 2]
        );
        let original_health = signal.local_exit_fault();
        for _ in 0..3 {
            let next = owner.observe_actor_exit();
            assert!(matches!(next.progress, ExitProgress::Unresolved(_)));
            assert!(next.failure.is_none());
        }
        assert_eq!(signal.local_exit_fault(), original_health);
        let shared = Arc::downgrade(&owner.shared);
        drop(owner);
        drop(executor);
        drop(runtime);
        assert!(shared.upgrade().is_some());
        assert_eq!(tracker.current(), funding_before);
        assert_eq!(array.upgrade().unwrap().len(), 3);
    }

    #[test]
    fn real_fragment_encoder_map_call_and_owned_retirement_preserve_their_actual_roles_and_funding()
    {
        use crate::exec::node::table_write_relation::ConnectorCommitFragmentEncoder;
        use novarocks_spi::connector::{
            ConnectorInstanceDescriptor,
            write_stack::{ProviderWriteRuntime, WriteRuntimeAdapter, WriteTargetOrdinal},
        };
        #[derive(Clone, Copy, Eq, PartialEq)]
        enum Mode {
            KnownCall,
            UnknownCall,
            MapperDrop,
            FragmentDrop,
        }
        struct Data {
            bytes: Vec<u8>,
            _funding: TrackedBytes,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Data {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Secondary {
            data: std::mem::ManuallyDrop<Arc<Data>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Secondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                unsafe {
                    std::mem::ManuallyDrop::drop(&mut self.data);
                }
            }
        }
        struct Primary {
            data: std::mem::ManuallyDrop<Arc<Data>>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl Drop for Primary {
            fn drop(&mut self) {
                self.primary.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                std::panic::panic_any(Secondary {
                    data: std::mem::ManuallyDrop::new(data),
                    drops: self.secondary.clone(),
                });
            }
        }
        struct Fragment {
            data: std::mem::ManuallyDrop<Arc<Data>>,
            mode: Mode,
            drops: Arc<AtomicUsize>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl std::fmt::Debug for Fragment {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple("Fragment").field(&self.data.bytes).finish()
            }
        }
        impl Drop for Fragment {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                if self.mode == Mode::FragmentDrop {
                    std::panic::panic_any(Primary {
                        data: std::mem::ManuallyDrop::new(data),
                        primary: self.primary.clone(),
                        secondary: self.secondary.clone(),
                    });
                }
                drop(data);
            }
        }
        struct Runtime {
            descriptor: ConnectorInstanceDescriptor,
            catalog: CatalogHandle,
        }
        impl ProviderWriteRuntime for Runtime {
            type CommitHandle = ();
            type WriterHandle = ();
            type CommitFragment = Fragment;
            fn descriptor(&self) -> &ConnectorInstanceDescriptor {
                &self.descriptor
            }
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog
            }
        }
        struct Encoder {
            adapter: WriteRuntimeAdapter<Runtime>,
            mode: Mode,
            calls: Arc<AtomicUsize>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl ConnectorCommitFragmentEncoder for Encoder {
            fn encode(
                &self,
                _: WriteTargetOrdinal,
                fragment: &ConnectorCommitFragment,
            ) -> Result<Vec<u8>, ConnectorError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let source = self.adapter.commit_fragment(fragment)?;
                assert_eq!(
                    source.data.bytes,
                    [7, 11, 13],
                    "the real provider encoder reads its original fragment"
                );
                match self.mode {
                    Mode::KnownCall => panic!("actual connector fragment encoder panicked"),
                    Mode::UnknownCall => std::panic::panic_any(Primary {
                        data: std::mem::ManuallyDrop::new(Arc::clone(&source.data)),
                        primary: self.primary.clone(),
                        secondary: self.secondary.clone(),
                    }),
                    _ => Ok(source.data.bytes.clone()),
                }
            }
        }
        struct Capture {
            data: std::mem::ManuallyDrop<Arc<Data>>,
            mode: Mode,
            drops: Arc<AtomicUsize>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl Capture {
            fn output(&self) -> Arc<Data> {
                Arc::clone(&self.data)
            }
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                if self.mode == Mode::MapperDrop {
                    std::panic::panic_any(Primary {
                        data: std::mem::ManuallyDrop::new(data),
                        primary: self.primary.clone(),
                        secondary: self.secondary.clone(),
                    });
                }
                drop(data);
            }
        }
        struct Execution {
            adapter: WriteRuntimeAdapter<Runtime>,
            calls: Arc<Calls>,
            tracker: Arc<MemTracker>,
            mode: Mode,
            fragment: Arc<Mutex<Option<std::sync::Weak<Data>>>>,
            drops: Arc<AtomicUsize>,
            data_drops: Arc<AtomicUsize>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        struct Writer {
            adapter: WriteRuntimeAdapter<Runtime>,
            calls: Arc<Calls>,
            tracker: Arc<MemTracker>,
            mode: Mode,
            fragment: Arc<Mutex<Option<std::sync::Weak<Data>>>>,
            drops: Arc<AtomicUsize>,
            data_drops: Arc<AtomicUsize>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for Execution {
            fn catalog_handle(&self) -> &CatalogHandle {
                self.adapter.binding().catalog_handle()
            }
            async fn open_writer(
                &self,
                _: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                self.calls.opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(Writer {
                    adapter: self.adapter.clone(),
                    calls: self.calls.clone(),
                    tracker: self.tracker.clone(),
                    mode: self.mode,
                    fragment: self.fragment.clone(),
                    drops: self.drops.clone(),
                    data_drops: self.data_drops.clone(),
                    primary: self.primary.clone(),
                    secondary: self.secondary.clone(),
                }))
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for Writer {
            async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
                self.calls.append_started.fetch_add(1, Ordering::SeqCst);
                self.calls
                    .appended
                    .fetch_add(batch.num_rows(), Ordering::SeqCst);
                Ok(())
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                self.calls.finished.fetch_add(1, Ordering::SeqCst);
                let bytes = vec![7, 11, 13];
                let retained = bytes.capacity();
                let data = Arc::new(Data {
                    bytes,
                    _funding: TrackedBytes::try_new(retained, self.tracker.clone()).unwrap(),
                    drops: self.data_drops.clone(),
                });
                *self.fragment.lock().unwrap() = Some(Arc::downgrade(&data));
                Ok(vec![self.adapter.wrap_commit_fragment(Fragment {
                    data: std::mem::ManuallyDrop::new(data),
                    mode: self.mode,
                    drops: self.drops.clone(),
                    primary: self.primary.clone(),
                    secondary: self.secondary.clone(),
                })])
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.calls.aborted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        for mode in [
            Mode::KnownCall,
            Mode::UnknownCall,
            Mode::MapperDrop,
            Mode::FragmentDrop,
        ] {
            let runtime = runtime();
            let signal = Arc::new(RuntimeErrorState::default());
            let tracker = MemTracker::new_root("actual encoder input output funding");
            let binding = crate::exec::operators::table_writer::tests::adapter()
                .binding()
                .clone();
            let adapter = WriteRuntimeAdapter::new(Arc::new(Runtime {
                descriptor: binding.descriptor().clone(),
                catalog: binding.catalog_handle().clone(),
            }));
            let calls = Arc::new(Calls::default());
            let encoder_calls = Arc::new(AtomicUsize::new(0));
            let primary = Arc::new(AtomicUsize::new(0));
            let secondary = Arc::new(AtomicUsize::new(0));
            let fragment = Arc::new(Mutex::new(None::<std::sync::Weak<Data>>));
            let fragment_drops = Arc::new(AtomicUsize::new(0));
            let fragment_data_drops = Arc::new(AtomicUsize::new(0));
            let output_drops = Arc::new(AtomicUsize::new(0));
            let capture_drops = Arc::new(AtomicUsize::new(0));
            let bytes = vec![19, 23];
            let retained = bytes.capacity();
            let output = Arc::new(Data {
                bytes,
                _funding: TrackedBytes::try_new(retained, tracker.clone()).unwrap(),
                drops: output_drops.clone(),
            });
            let output_weak = Arc::downgrade(&output);
            let capture = Capture {
                data: std::mem::ManuallyDrop::new(output),
                mode,
                drops: capture_drops.clone(),
                primary: primary.clone(),
                secondary: secondary.clone(),
            };
            let encoder = Encoder {
                adapter: adapter.clone(),
                mode,
                calls: encoder_calls.clone(),
                primary: primary.clone(),
                secondary: secondary.clone(),
            };
            let mut owner = AsyncWriterOwner::new(
                Arc::new(Execution {
                    adapter,
                    calls: calls.clone(),
                    tracker: tracker.clone(),
                    mode,
                    fragment: fragment.clone(),
                    drops: fragment_drops.clone(),
                    data_drops: fragment_data_drops.clone(),
                    primary: primary.clone(),
                    secondary: secondary.clone(),
                }),
                request(),
                AsyncWriterQueueConfig::default(),
                Box::new(move |_, fragments| {
                    encoder.encode(target(0), &fragments[0]).map_err(|cause| {
                        ConnectorCallFailure::new(ConnectorOperation::EncodeCommitFragment, cause)
                    })?;
                    Ok(capture.output())
                }),
            );
            owner
                .bind(runtime.services().sink_io().clone(), signal.clone())
                .unwrap();
            let input = batch(3);
            let bytes = crate::exec::chunk::record_batch_bytes(&input);
            owner.enqueue(input, bytes).unwrap();
            wait_until(|| owner.queued_usage() == (0, 0));
            owner.request_finish().unwrap();
            wait_until(|| calls.aborted.load(Ordering::SeqCst) == 1);
            assert_eq!(calls.finished.load(Ordering::SeqCst), 1);
            assert_eq!(
                encoder_calls.load(Ordering::SeqCst),
                1,
                "the original mapper is logically once"
            );
            assert!(
                owner.take_output().is_none(),
                "no output is published across unknown or failed finalization"
            );
            if mode == Mode::KnownCall {
                wait_until(|| owner.actor_exited());
                let observed = owner.observe_actor_exit();
                assert!(matches!(observed.progress, ExitProgress::Exited));
                assert!(observed.failure.is_none());
                let original = signal.error().unwrap();
                assert!(owner.error().unwrap().same_occurrence(&original));
                assert!(signal.local_exit_fault().is_none());
                assert_eq!(tracker.current(), 0);
                assert_eq!(fragment_data_drops.load(Ordering::SeqCst), 1);
                assert_eq!(output_drops.load(Ordering::SeqCst), 1);
                assert!(
                    fragment
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .upgrade()
                        .is_none()
                );
                assert!(output_weak.upgrade().is_none());
            } else {
                wait_until(|| signal.local_exit_fault().is_some());
                let original = signal.error();
                assert_eq!(
                    original.is_some(),
                    mode == Mode::UnknownCall,
                    "only the actual mapper call is Required; owner/input retirement is Cleanup"
                );
                if let Some(original) = &original {
                    assert!(owner.error().unwrap().same_occurrence(original));
                }
                let health = signal.local_exit_fault();
                assert_eq!(
                    tracker.current(),
                    5,
                    "actual original fragment and captured/output funding remain held"
                );
                assert_eq!(
                    fragment
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .upgrade()
                        .unwrap()
                        .bytes,
                    [7, 11, 13]
                );
                assert_eq!(output_weak.upgrade().unwrap().bytes, [19, 23]);
                assert_eq!(primary.load(Ordering::SeqCst), 1);
                assert_eq!(secondary.load(Ordering::SeqCst), 0);
                assert_eq!(
                    fragment_drops.load(Ordering::SeqCst),
                    usize::from(mode == Mode::FragmentDrop)
                );
                assert_eq!(
                    capture_drops.load(Ordering::SeqCst),
                    usize::from(mode != Mode::UnknownCall)
                );
                for _ in 0..3 {
                    let observed = owner.observe_actor_exit();
                    assert!(matches!(observed.progress, ExitProgress::Unresolved(_)));
                    assert!(observed.failure.is_none());
                }
                owner.request_abort();
                assert_eq!(
                    calls.aborted.load(Ordering::SeqCst),
                    1,
                    "unknown teardown does not repeat abort"
                );
                assert_eq!(signal.local_exit_fault(), health);
                let shared = Arc::downgrade(&owner.shared);
                drop(owner);
                drop(runtime);
                assert!(shared.upgrade().is_some());
                assert_eq!(tracker.current(), 5);
                assert_eq!(
                    fragment
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .upgrade()
                        .unwrap()
                        .bytes,
                    [7, 11, 13]
                );
                assert_eq!(output_weak.upgrade().unwrap().bytes, [19, 23]);
                assert_eq!(fragment_data_drops.load(Ordering::SeqCst), 0);
                assert_eq!(output_drops.load(Ordering::SeqCst), 0);
                assert_eq!(secondary.load(Ordering::SeqCst), 0);
            }
        }
    }
}
