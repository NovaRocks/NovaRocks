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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use futures::FutureExt;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution,
};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::exec::chunk::{ChunkMemoryLease, TransferredChunkBytes};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::execution_services::IoExecutor;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::runtime_state::RuntimeErrorState;

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

struct AppendCommand {
    batch: RecordBatch,
    rows: usize,
    bytes: usize,
    _transferred_accounting: Option<TransferredChunkBytes>,
    _additional_accounting: Option<TrackedBytes>,
}

enum WriterCommand {
    Append(AppendCommand),
    Finish,
}

type FinishMapper<O> =
    Box<dyn FnOnce(u64, Vec<ConnectorCommitFragment>) -> Result<O, String> + Send + 'static>;

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
    error: Mutex<Option<String>>,
    queue_tracker: Mutex<Option<Arc<MemTracker>>>,
    queue_peak_batches: AtomicUsize,
    queue_peak_rows: AtomicUsize,
    queue_peak_bytes: AtomicUsize,
    queue_blocked_checks: AtomicUsize,
    abort_requests: AtomicUsize,
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
            queue_tracker: Mutex::new(None),
            queue_peak_batches: AtomicUsize::new(0),
            queue_peak_rows: AtomicUsize::new(0),
            queue_peak_bytes: AtomicUsize::new(0),
            queue_blocked_checks: AtomicUsize::new(0),
            abort_requests: AtomicUsize::new(0),
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

    fn set_error(&self, error: String, runtime_error: &RuntimeErrorState) {
        let mut guard = self.error.lock().expect("async writer error lock");
        if guard.is_none() {
            runtime_error.set_error(error.clone());
            *guard = Some(error);
        }
        self.wake();
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
            self.set_error(
                "connector writer actor exited without a terminal result".to_string(),
                runtime_error,
            );
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
    pub(crate) fn send(mut self, batch: RecordBatch) {
        let command = WriterCommand::Append(AppendCommand {
            batch,
            rows: self.rows,
            bytes: self.bytes,
            _transferred_accounting: self.transferred_accounting.take(),
            _additional_accounting: self.additional_accounting.take(),
        });
        self.permit
            .take()
            .expect("async writer reservation permit")
            .send(command);
        self.committed = true;
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
    ) -> Result<(), String> {
        let start = self
            .start
            .take()
            .ok_or_else(|| "connector writer actor is already bound".to_string())?;
        let shared = Arc::clone(&self.shared);
        let config = self.config;
        self.join = Some(executor.spawn(async move {
            // Keep the provider lifecycle and its terminal publication in one
            // sink-I/O task. A nested task would publish its result first and
            // require a second scheduler turn before this wrapper could release
            // actor resources and publish `done`, which can strand the driver
            // in PendingFinish when that wrapper turn is delayed.
            let outcome = std::panic::AssertUnwindSafe(run_writer_actor(
                start,
                Arc::clone(&shared),
                config,
                Arc::clone(&runtime_error),
            ))
            .catch_unwind()
            .await;
            // The actor future (including its provider writer, receiver, and
            // in-flight command) has now been dropped even on panic. Clear the
            // logical counters after the matching retained-memory guards.
            shared.clear_queue_usage();
            match outcome {
                Ok(Some(output)) => shared.publish_terminal_success(output),
                Ok(None) => {
                    shared.ensure_terminal_outcome(&runtime_error);
                    shared.set_done();
                }
                Err(payload) => {
                    shared.set_error(
                        format!(
                            "connector writer actor panicked: {}",
                            panic_message(payload)
                        ),
                        &runtime_error,
                    );
                    shared.ensure_terminal_outcome(&runtime_error);
                    shared.set_done();
                }
            }
        }));
        Ok(())
    }

    pub(crate) fn can_accept(&self) -> bool {
        if self.shared.finish_requested.load(Ordering::Acquire)
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
            && usage.rows.saturating_add(self.config.max_batch_rows) <= self.config.max_rows
            && usage.bytes.saturating_add(self.config.max_batch_bytes) <= self.config.max_bytes;
        if !can_accept {
            self.shared
                .queue_blocked_checks
                .fetch_add(1, Ordering::Relaxed);
        }
        can_accept
    }

    #[cfg(test)]
    pub(crate) fn enqueue(&self, batch: RecordBatch, retained_bytes: usize) -> Result<(), String> {
        self.enqueue_with_accounting(batch, retained_bytes, None, retained_bytes)
    }

    #[cfg(test)]
    pub(crate) fn enqueue_with_accounting(
        &self,
        batch: RecordBatch,
        retained_bytes: usize,
        inherited_accounting: Option<ChunkMemoryLease>,
        additional_bytes: usize,
    ) -> Result<(), String> {
        let reservation = self.try_reserve_input(
            batch.num_rows(),
            retained_bytes,
            inherited_accounting,
            additional_bytes,
        )?;
        reservation.send(batch);
        Ok(())
    }

    pub(crate) fn try_reserve_input(
        &self,
        rows: usize,
        retained_bytes: usize,
        inherited_accounting: Option<ChunkMemoryLease>,
        additional_bytes: usize,
    ) -> Result<AsyncWriterReservation<O>, String> {
        if rows > self.config.max_batch_rows {
            return Err(format!(
                "ResourceExhausted: connector writer input batch has {rows} rows, above the per-batch limit of {} rows",
                self.config.max_batch_rows
            ));
        }
        if retained_bytes > self.config.max_batch_bytes {
            return Err(format!(
                "ResourceExhausted: connector writer input batch retains {retained_bytes} bytes, above the per-batch limit of {} bytes",
                self.config.max_batch_bytes
            ));
        }
        {
            let mut usage = self
                .shared
                .queue_usage
                .lock()
                .expect("async writer queue usage lock");
            let fits = usage.batches < self.config.max_batches
                && usage.rows.saturating_add(rows) <= self.config.max_rows
                && usage.bytes.saturating_add(retained_bytes) <= self.config.max_bytes;
            if !fits {
                return Err(
                    "connector writer queue has no reserved rows/bytes capacity".to_string()
                );
            }
            usage.batches += 1;
            usage.rows += rows;
            usage.bytes += retained_bytes;
            self.shared.observe_queue_usage(*usage);
        }
        if additional_bytes > retained_bytes {
            self.shared.release_append(rows, retained_bytes);
            return Err(format!(
                "connector writer additional retained bytes {additional_bytes} exceed total retained bytes {retained_bytes}"
            ));
        }
        let queue_tracker = self
            .shared
            .queue_tracker
            .lock()
            .expect("async writer queue tracker lock")
            .clone();
        let tracker =
            queue_tracker.or_else(|| inherited_accounting.as_ref().map(ChunkMemoryLease::tracker));
        let shared_projected_bytes = retained_bytes - additional_bytes;
        let transferred_accounting = match (inherited_accounting.as_ref(), tracker.as_ref()) {
            (Some(accounting), Some(tracker)) => {
                match accounting.try_split_to(tracker, shared_projected_bytes) {
                    Ok(accounting) => accounting,
                    Err(error) => {
                        self.shared.release_append(rows, retained_bytes);
                        return Err(error);
                    }
                }
            }
            _ => None,
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
                    return Err(error);
                }
            },
            None => None,
        };
        let Some(sender) = self.sender.as_ref().cloned() else {
            self.shared.release_append(rows, retained_bytes);
            return Err("connector writer enqueue after terminal transition".to_string());
        };
        let permit = match sender.try_reserve_owned() {
            Ok(permit) => permit,
            Err(error) => {
                self.shared.release_append(rows, retained_bytes);
                return Err(format!("connector writer enqueue failed: {error}"));
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

    pub(crate) fn request_finish(&mut self) -> Result<(), String> {
        if self.shared.finish_requested.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let Some(sender) = self.sender.take() else {
            return Err("connector writer finish after terminal transition".to_string());
        };
        sender
            .try_send(WriterCommand::Finish)
            .map_err(|error| format!("connector writer finish enqueue failed: {error}"))
    }

    pub(crate) fn request_abort(&mut self) {
        if self.shared.done.load(Ordering::Acquire)
            || self.shared.abort_requested.swap(true, Ordering::AcqRel)
        {
            return;
        }
        self.shared.abort_requests.fetch_add(1, Ordering::Relaxed);
        self.sender = None;
        self.shared.abort_notify.notify_one();
        self.shared.wake();
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.shared.observable)
    }

    pub(crate) fn is_done(&self) -> bool {
        self.shared.done.load(Ordering::Acquire)
    }

    pub(crate) fn has_output(&self) -> bool {
        self.shared
            .result
            .lock()
            .expect("async writer result lock")
            .is_some()
    }

    pub(crate) fn take_output(&self) -> Option<O> {
        self.shared
            .result
            .lock()
            .expect("async writer result lock")
            .take()
    }

    pub(crate) fn error(&self) -> Option<String> {
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
    }
}

enum AwaitResult<T> {
    Completed(T),
    Aborted,
    Panicked(String),
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

async fn await_or_abort<T>(
    future: impl std::future::Future<Output = T>,
    shared: &WriterShared<impl Send>,
) -> AwaitResult<T> {
    if shared.abort_requested.load(Ordering::Acquire) {
        return AwaitResult::Aborted;
    }
    tokio::select! {
        biased;
        _ = shared.abort_notify.notified() => AwaitResult::Aborted,
        result = std::panic::AssertUnwindSafe(future).catch_unwind() => match result {
            Ok(result) => AwaitResult::Completed(result),
            Err(payload) => AwaitResult::Panicked(panic_message(payload)),
        },
    }
}

async fn abort_writer(
    writer: &mut dyn ConnectorBatchWriter,
    timeout: Duration,
) -> Result<(), String> {
    let abort = std::panic::AssertUnwindSafe(writer.abort()).catch_unwind();
    match tokio::time::timeout(timeout, abort).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(format!("abort connector writer: {error}")),
        Ok(Err(payload)) => Err(format!(
            "abort connector writer panicked: {}",
            panic_message(payload)
        )),
        Err(_) => Err(format!(
            "abort connector writer exceeded the bounded wait of {} ms",
            timeout.as_millis()
        )),
    }
}

async fn fail_with_abort<O>(
    writer: &mut dyn ConnectorBatchWriter,
    primary: String,
    shared: &WriterShared<O>,
    config: AsyncWriterQueueConfig,
    runtime_error: &RuntimeErrorState,
) {
    let error = match abort_writer(writer, config.abort_timeout).await {
        Ok(()) => primary,
        Err(abort_error) => format!("{primary}; {abort_error}"),
    };
    shared.set_error(error, runtime_error);
}

fn checked_accepted_rows(current: u64, rows: usize) -> Result<u64, String> {
    let rows = u64::try_from(rows)
        .map_err(|_| "connector writer accepted row count does not fit u64".to_string())?;
    current
        .checked_add(rows)
        .ok_or_else(|| "connector writer accepted row count overflowed u64".to_string())
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
    let mut writer = match await_or_abort(execution.open_writer(request), shared.as_ref()).await {
        AwaitResult::Completed(Ok(writer)) => writer,
        AwaitResult::Completed(Err(error)) => {
            shared.set_error(format!("open connector writer: {error}"), &runtime_error);
            return None;
        }
        AwaitResult::Aborted => {
            return None;
        }
        AwaitResult::Panicked(detail) => {
            shared.set_error(
                format!("open connector writer panicked: {detail}"),
                &runtime_error,
            );
            return None;
        }
    };
    let mut finish_mapper = Some(finish_mapper);
    let mut accepted_rows = 0u64;

    loop {
        if shared.abort_requested.load(Ordering::Acquire) {
            if let Err(error) = abort_writer(writer.as_mut(), config.abort_timeout).await {
                shared.set_error(error, &runtime_error);
            }
            return None;
        }
        let command = tokio::select! {
            biased;
            _ = shared.abort_notify.notified() => continue,
            command = receiver.recv() => command,
        };
        let Some(command) = command else {
            if let Err(error) = abort_writer(writer.as_mut(), config.abort_timeout).await {
                shared.set_error(error, &runtime_error);
            }
            return None;
        };
        match command {
            WriterCommand::Append(command) => {
                let rows = command.rows;
                let bytes = command.bytes;
                let outcome = await_or_abort(writer.append(command.batch), shared.as_ref()).await;
                drop(command._additional_accounting);
                drop(command._transferred_accounting);
                shared.release_append(rows, bytes);
                match outcome {
                    AwaitResult::Completed(Ok(())) => {
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
                    AwaitResult::Completed(Err(error)) => {
                        fail_with_abort(
                            writer.as_mut(),
                            format!("append connector writer batch: {error}"),
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                    AwaitResult::Aborted => {
                        if let Err(error) =
                            abort_writer(writer.as_mut(), config.abort_timeout).await
                        {
                            shared.set_error(error, &runtime_error);
                        }
                        return None;
                    }
                    AwaitResult::Panicked(detail) => {
                        fail_with_abort(
                            writer.as_mut(),
                            format!("append connector writer batch panicked: {detail}"),
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
                let fragments = match await_or_abort(writer.finish(), shared.as_ref()).await {
                    AwaitResult::Completed(Ok(fragments)) => fragments,
                    AwaitResult::Completed(Err(error)) => {
                        fail_with_abort(
                            writer.as_mut(),
                            format!("finish connector writer: {error}"),
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                    AwaitResult::Aborted => {
                        if let Err(error) =
                            abort_writer(writer.as_mut(), config.abort_timeout).await
                        {
                            shared.set_error(error, &runtime_error);
                        }
                        return None;
                    }
                    AwaitResult::Panicked(detail) => {
                        fail_with_abort(
                            writer.as_mut(),
                            format!("finish connector writer panicked: {detail}"),
                            shared.as_ref(),
                            config,
                            &runtime_error,
                        )
                        .await;
                        return None;
                    }
                };
                let mapper = finish_mapper
                    .take()
                    .expect("connector writer finish mapper is consumed exactly once");
                match mapper(accepted_rows, fragments) {
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
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::write_stack::{
        ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
        ConnectorWriteExecution,
    };
    use novarocks_spi::connector::{CatalogHandle, ConnectorError, ConnectorErrorKind};

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
                    scan_threads: 1,
                    scan_queue_capacity: 1,
                    spill_io_threads: 1,
                    spill_io_queue_capacity: 1,
                    spill_storage:
                        crate::runtime::execution_runtime::ExecutionSpillStorageConfig::default(),
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    connector_io_tasks_per_scan_operator: 1,
                    scan_submit_fail_max: 1,
                    scan_submit_fail_timeout_ms: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
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
    fn semantic_terminal_latch_does_not_wait_for_executor_wrapper_completion() {
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
            "semantic writer completion must not depend on an unobservable JoinHandle transition"
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

        assert!(error.contains("memory limit exceeded"), "{error}");
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
            .enqueue_with_accounting(input, bytes, accounting, 0)
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
            .enqueue_with_accounting(projection, projected_bytes, accounting, 0)
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
            .enqueue_with_accounting(input, bytes, Some(lease.clone()), 0)
            .expect_err("terminal writer must reject append");
        assert!(error.contains("terminal transition"), "{error}");
        assert!(Arc::ptr_eq(&lease.tracker(), &input_tracker));
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
            .enqueue_with_accounting(input, bytes, accounting, 0)
            .expect_err("full writer mailbox must reject append");
        assert!(error.contains("connector writer enqueue failed"), "{error}");
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
                .contains("injected append failure")
        );
    }

    #[test]
    fn cancellation_interrupts_append_then_cooperatively_aborts() {
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
            owner(Arc::clone(&calls), Some(gate), false, false, config);
        let input = batch(2);
        let bytes = crate::exec::chunk::record_batch_bytes(&input);
        owner.enqueue(input, bytes).expect("enqueue slow append");
        wait_until(|| calls.append_started.load(Ordering::Relaxed) == 1);
        owner.request_abort();
        wait_until(|| owner.is_done());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert_eq!(calls.appended.load(Ordering::Relaxed), 0);
        assert_eq!(calls.finished.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.current(), 0);
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
                .contains("finish connector writer panicked: injected finish panic")
        );
    }

    #[test]
    fn abort_panic_is_an_explicit_terminal_failure() {
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
            error
                .error()
                .expect("abort panic must be recorded")
                .contains("abort connector writer panicked: injected abort panic")
        );
    }

    #[test]
    fn accepted_row_count_overflow_is_rejected_instead_of_saturated() {
        assert_eq!(checked_accepted_rows(41, 1).expect("normal count"), 42);
        assert_eq!(
            checked_accepted_rows(u64::MAX, 1).expect_err("overflow"),
            "connector writer accepted row count overflowed u64"
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
        wait_until(|| owner.is_done());
        assert_eq!(calls.aborted.load(Ordering::Relaxed), 1);
        assert_eq!(calls.finished.load(Ordering::Relaxed), 0);
        assert!(
            error
                .error()
                .expect("abort failure")
                .contains("injected abort failure")
        );
    }

    #[test]
    fn abort_wait_is_bounded_and_timeout_is_an_explicit_failure() {
        struct HangingAbortExecution(CatalogHandle, Arc<AtomicBool>);

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
                Ok(Box::new(HangingAbortWriter))
            }
        }

        struct HangingAbortWriter;

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for HangingAbortWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Ok(Vec::new())
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                std::future::pending().await
            }
        }

        let runtime = runtime();
        let error = Arc::new(RuntimeErrorState::default());
        let opened = Arc::new(AtomicBool::new(false));
        let mut owner = AsyncWriterOwner::new(
            Arc::new(HangingAbortExecution(catalog_handle(), Arc::clone(&opened))),
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
        wait_until(|| owner.is_done());

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            error
                .error()
                .expect("abort timeout must fail the writer")
                .contains("exceeded the bounded wait of 20 ms")
        );
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
}
