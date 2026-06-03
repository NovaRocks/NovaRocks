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
//! Async sink operator contract (IW-2).
//!
//! Responsibilities:
//! - Lets a sink enqueue chunks to a bounded queue and drain them on the
//!   dedicated `sink_io` execution service (IW-1) instead of doing blocking
//!   I/O on the driver thread.
//! - Carries the full pipeline contract on the existing `ProcessorOperator`
//!   methods: backpressure via `need_input`/`OutputFull`, async finish via
//!   `pending_finish`/`PendingFinish`, and error propagation via
//!   `RuntimeErrorState`.
//!
//! Concrete sinks implement only `AsyncSinkBackend`; `AsyncSinkOperator<B>`
//! wraps them and is the single place the contract is implemented.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::execution_services::IoExecutor;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Minimal async backend implemented by concrete sinks. The wrapper drives it.
#[async_trait::async_trait]
pub trait AsyncSinkBackend: Send + 'static {
    /// Result handed to the caller after a clean finish (e.g. staged files, stats).
    type Output: Send + 'static;

    /// Write one chunk. Runs on the `sink_io` runtime; may do real I/O.
    async fn write_chunk(&mut self, chunk: Chunk) -> Result<(), String>;

    /// Finalize after all chunks are drained. Runs on the `sink_io` runtime.
    async fn finish(&mut self) -> Result<Self::Output, String>;
}

/// State shared between the driver-side operator and the background drain task.
struct SinkShared<O> {
    /// Fires when the queue drains (backpressure relief) or finish completes.
    observable: Arc<Observable>,
    /// Chunks enqueued but not yet drained (need_input watermark + metrics).
    queued: AtomicUsize,
    /// Background drain + finish fully done.
    finished: AtomicBool,
    /// Background hit an error.
    errored: AtomicBool,
    /// Output produced by a clean finish.
    result: std::sync::Mutex<Option<O>>,
}

impl<O> SinkShared<O> {
    fn new() -> Self {
        Self {
            observable: Arc::new(Observable::new()),
            queued: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
            errored: AtomicBool::new(false),
            result: std::sync::Mutex::new(None),
        }
    }

    /// Wake any driver parked on this sink's observable.
    fn wake(&self) {
        self.observable.defer_notify().arm();
    }
}

/// Generic async sink operator. Implements the full pipeline contract; concrete
/// sinks only implement [`AsyncSinkBackend`].
pub struct AsyncSinkOperator<B: AsyncSinkBackend> {
    name: String,
    capacity: usize,
    // Pre-bind state (moved into the background task at bind_runtime_state):
    backend: Option<B>,
    rx: Option<mpsc::Receiver<Chunk>>,
    // Live state:
    sender: Option<mpsc::Sender<Chunk>>,
    shared: Arc<SinkShared<B::Output>>,
    join: Option<JoinHandle<()>>,
    finishing: bool,
}

impl<B: AsyncSinkBackend> AsyncSinkOperator<B> {
    pub fn new(name: impl Into<String>, backend: B, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            name: name.into(),
            capacity,
            backend: Some(backend),
            rx: Some(rx),
            sender: Some(tx),
            shared: Arc::new(SinkShared::new()),
            join: None,
            finishing: false,
        }
    }

    /// Take the finish output (available once `is_finished()` is true after a
    /// clean finish). Returns None if errored or not finished.
    pub fn take_output(&self) -> Option<B::Output> {
        self.shared.result.lock().expect("sink result lock").take()
    }
}

/// Background drain loop: pull chunks, write them, then finish. Reports errors
/// through `RuntimeErrorState` and never blocks the driver thread.
async fn drain_loop<B: AsyncSinkBackend>(
    mut backend: B,
    mut rx: mpsc::Receiver<Chunk>,
    shared: Arc<SinkShared<B::Output>>,
    error_state: Arc<RuntimeErrorState>,
) -> Result<(), String> {
    // `while let` ends the loop when the sender is dropped (set_finishing /
    // cancel) — equivalent to matching `recv()` and breaking on `None`.
    while let Some(chunk) = rx.recv().await {
        match backend.write_chunk(chunk).await {
            Ok(()) => {
                shared.queued.fetch_sub(1, Ordering::AcqRel);
                shared.wake(); // queue has room → wake a backpressured driver
            }
            Err(e) => {
                error_state.set_error(e.clone());
                shared.errored.store(true, Ordering::Release);
                shared.finished.store(true, Ordering::Release);
                shared.wake();
                return Err(e);
            }
        }
    }
    let result = backend.finish().await;
    match result {
        Ok(out) => {
            *shared.result.lock().expect("sink result lock") = Some(out);
        }
        Err(e) => {
            error_state.set_error(e.clone());
            shared.errored.store(true, Ordering::Release);
            shared.finished.store(true, Ordering::Release);
            shared.wake();
            return Err(e);
        }
    }
    shared.finished.store(true, Ordering::Release);
    shared.wake();
    Ok(())
}

impl<B: AsyncSinkBackend> Operator for AsyncSinkOperator<B> {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        let sink_io: IoExecutor = state.sink_io_executor()?;
        let error_state = state.error_state();
        let backend = self
            .backend
            .take()
            .ok_or_else(|| "async sink backend already bound".to_string())?;
        let rx = self
            .rx
            .take()
            .ok_or_else(|| "async sink receiver already bound".to_string())?;
        let shared = Arc::clone(&self.shared);
        let join = sink_io.spawn(async move {
            let _ = drain_loop(backend, rx, shared, error_state).await;
        });
        self.join = Some(join);
        Ok(())
    }

    fn is_finished(&self) -> bool {
        // "No more input needed": true once finishing was requested, or once the
        // background fully completed / errored / was canceled. Together with
        // pending_finish() this lets the driver enter DriverState::PendingFinish
        // during the async tail and reach Finished only after it clears.
        self.finishing || self.shared.finished.load(Ordering::Acquire)
    }

    fn pending_finish(&self) -> bool {
        self.finishing && !self.shared.finished.load(Ordering::Acquire)
    }

    fn cancel(&mut self) {
        self.sender = None; // close the channel
        if let Some(join) = self.join.take() {
            join.abort();
        }
        self.shared.errored.store(true, Ordering::Release);
        self.shared.finished.store(true, Ordering::Release);
        self.shared.wake();
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl<B: AsyncSinkBackend> ProcessorOperator for AsyncSinkOperator<B> {
    fn need_input(&self) -> bool {
        !self.finishing
            && !self.shared.errored.load(Ordering::Acquire)
            && self.shared.queued.load(Ordering::Acquire) < self.capacity
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        let Some(sender) = self.sender.as_ref() else {
            return Err("async sink push after finishing/cancel".to_string());
        };
        self.shared.queued.fetch_add(1, Ordering::AcqRel);
        match sender.try_send(chunk) {
            Ok(()) => Ok(()),
            Err(e) => {
                // need_input gates this; a Full/closed here is a contract bug.
                self.shared.queued.fetch_sub(1, Ordering::AcqRel);
                Err(format!("async sink enqueue failed: {e}"))
            }
        }
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        self.sender = None; // drop sender → background sees recv()==None → finish()
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.shared.observable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::BlockedReason;
    use crate::runtime::runtime_state::RuntimeState;

    fn make_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));
        let data: Vec<i32> = (0..rows as i32).collect();
        let array = Arc::new(Int32Array::from(data)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    /// Test backend: records rows seen; optional per-chunk gate and fail point.
    struct TestAsyncSink {
        rows: Arc<AtomicUsize>,
        chunks: Arc<AtomicUsize>,
        // When set, write_chunk waits until released (simulates slow I/O / backpressure).
        gate: Arc<tokio::sync::Semaphore>,
        // When Some(n), the n-th (0-based) write_chunk fails.
        fail_at: Option<usize>,
        // Delay applied inside finish() to exercise pending_finish.
        finish_delay: Duration,
    }

    impl TestAsyncSink {
        fn new(gate_permits: usize) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
            let rows = Arc::new(AtomicUsize::new(0));
            let chunks = Arc::new(AtomicUsize::new(0));
            let sink = Self {
                rows: Arc::clone(&rows),
                chunks: Arc::clone(&chunks),
                gate: Arc::new(tokio::sync::Semaphore::new(gate_permits)),
                fail_at: None,
                finish_delay: Duration::ZERO,
            };
            (sink, rows, chunks)
        }
    }

    #[async_trait::async_trait]
    impl AsyncSinkBackend for TestAsyncSink {
        type Output = usize;

        async fn write_chunk(&mut self, chunk: Chunk) -> Result<(), String> {
            let permit = self.gate.acquire().await.expect("gate");
            permit.forget();
            let idx = self.chunks.fetch_add(1, Ordering::AcqRel);
            if self.fail_at == Some(idx) {
                return Err(format!("forced failure at chunk {idx}"));
            }
            self.rows.fetch_add(chunk.len(), Ordering::AcqRel);
            Ok(())
        }

        async fn finish(&mut self) -> Result<usize, String> {
            if !self.finish_delay.is_zero() {
                tokio::time::sleep(self.finish_delay).await;
            }
            Ok(self.rows.load(Ordering::Acquire))
        }
    }

    fn poll_until<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        pred()
    }

    #[test]
    fn drains_all_chunks_with_backpressure() {
        let state = RuntimeState::default();
        // gate starts open (large permit count) so writes flow.
        let (backend, rows, chunks) = TestAsyncSink::new(1_000);
        let mut op = AsyncSinkOperator::new("test_async_sink", backend, 2);
        op.bind_runtime_state(&state).expect("bind");

        // Push 5 chunks of 3 rows each, respecting need_input backpressure.
        let mut pushed = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while pushed < 5 {
            if op.need_input() {
                op.push_chunk(&state, make_chunk(3)).expect("push");
                pushed += 1;
            } else {
                assert!(Instant::now() < deadline, "stuck on backpressure");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        op.set_finishing(&state).expect("finish");

        assert!(
            poll_until(
                || op.is_finished() && !op.pending_finish(),
                Duration::from_secs(5)
            ),
            "sink did not finish"
        );
        assert_eq!(chunks.load(Ordering::Acquire), 5);
        assert_eq!(rows.load(Ordering::Acquire), 15);
        assert_eq!(op.take_output(), Some(15));
    }

    #[test]
    fn need_input_goes_false_when_queue_full_then_recovers() {
        let state = RuntimeState::default();
        // gate starts closed (0 permits): background blocks on the first write.
        let (backend, _rows, _chunks) = TestAsyncSink::new(0);
        let gate = Arc::clone(&backend.gate);
        let mut op = AsyncSinkOperator::new("bp_sink", backend, 2);
        op.bind_runtime_state(&state).expect("bind");

        // Fill the queue: capacity=2, plus 1 in-flight pulled by the bg task.
        // Push until need_input() reports full.
        let mut pushed = 0;
        while op.need_input() && pushed < 8 {
            op.push_chunk(&state, make_chunk(1)).expect("push");
            pushed += 1;
        }
        assert!(
            !op.need_input(),
            "sink should report backpressure when full"
        );

        // Release the gate; background drains; need_input must recover.
        gate.add_permits(100);
        assert!(
            poll_until(|| op.need_input(), Duration::from_secs(5)),
            "need_input did not recover after drain"
        );

        op.set_finishing(&state).expect("finish");
        assert!(
            poll_until(
                || op.is_finished() && !op.pending_finish(),
                Duration::from_secs(5)
            ),
            "sink did not finish"
        );
    }

    #[test]
    fn pending_finish_true_while_finishing_then_clears() {
        let state = RuntimeState::default();
        let (mut backend, _rows, _chunks) = TestAsyncSink::new(1_000);
        backend.finish_delay = Duration::from_millis(200);
        let mut op = AsyncSinkOperator::new("finish_sink", backend, 4);
        op.bind_runtime_state(&state).expect("bind");

        op.push_chunk(&state, make_chunk(2)).expect("push");
        op.set_finishing(&state).expect("finish");

        // While finish() sleeps, pending_finish must be true and is_finished false.
        assert!(
            poll_until(|| op.pending_finish(), Duration::from_secs(1)),
            "expected pending_finish during async finish"
        );
        assert!(
            op.take_output().is_none(),
            "output must not be ready mid-finish"
        );

        // After finish completes, pending_finish clears and is_finished is true.
        assert!(
            poll_until(
                || op.is_finished() && !op.pending_finish(),
                Duration::from_secs(5)
            ),
            "sink did not finish"
        );
        assert!(
            !op.pending_finish(),
            "pending_finish must clear after finish"
        );
        assert_eq!(op.take_output(), Some(2));
    }

    #[test]
    fn background_failure_sets_query_error_and_does_not_hang() {
        let state = RuntimeState::default();
        let (mut backend, _rows, _chunks) = TestAsyncSink::new(1_000);
        backend.fail_at = Some(1); // second chunk fails
        let mut op = AsyncSinkOperator::new("err_sink", backend, 4);
        op.bind_runtime_state(&state).expect("bind");

        // Push a few chunks; one of them triggers the failure in the bg task.
        for _ in 0..3 {
            if op.need_input() {
                let _ = op.push_chunk(&state, make_chunk(1));
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        // The error must surface through the runtime error channel within bounded time.
        assert!(
            poll_until(|| state.error().is_some(), Duration::from_secs(5)),
            "background failure did not set runtime error"
        );
        // And the operator must converge (no hang): errored ⇒ finished, need_input false.
        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "errored sink did not converge to finished"
        );
        assert!(!op.need_input(), "errored sink must stop accepting input");
        assert!(
            state.error().unwrap().contains("forced failure"),
            "unexpected error text"
        );
    }

    #[test]
    fn cancel_mid_flight_aborts_without_hang() {
        let state = RuntimeState::default();
        // gate closed: bg task is parked inside write_chunk (before it counts the
        // chunk) when we cancel, so this exercises cancel of an in-flight write.
        let (backend, _rows, chunks) = TestAsyncSink::new(0);
        let mut op = AsyncSinkOperator::new("cancel_sink", backend, 4);
        op.bind_runtime_state(&state).expect("bind");

        op.push_chunk(&state, make_chunk(1)).expect("push");
        // Give the bg task a moment to dequeue the chunk and park at the closed gate.
        assert!(
            poll_until(
                || chunks.load(Ordering::Acquire) == 0,
                Duration::from_millis(200)
            ),
            "precondition: bg write must be parked at the closed gate"
        );

        // cancel() must be non-blocking; bound the call itself.
        let started = Instant::now();
        op.cancel();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "cancel() must not block on the background task"
        );

        // Cancel converges the operator without hang.
        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "cancel did not converge sink"
        );
        assert!(!op.need_input(), "canceled sink must not accept input");
        // The parked write never completed → it was genuinely in-flight when canceled.
        assert_eq!(
            chunks.load(Ordering::Acquire),
            0,
            "aborted in-flight write must not have completed"
        );
    }

    /// Source operator that emits `remaining` chunks then finishes.
    struct TestSource {
        remaining: usize,
        finished: bool,
    }

    impl TestSource {
        fn new(n: usize) -> Self {
            Self {
                remaining: n,
                finished: false,
            }
        }
    }

    impl Operator for TestSource {
        fn name(&self) -> &str {
            "test_source"
        }
        fn is_finished(&self) -> bool {
            self.finished
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for TestSource {
        fn need_input(&self) -> bool {
            false
        }
        fn has_output(&self) -> bool {
            self.remaining > 0
        }
        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("source does not accept input".to_string())
        }
        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            if self.remaining == 0 {
                self.finished = true;
                return Ok(None);
            }
            self.remaining -= 1;
            if self.remaining == 0 {
                self.finished = true;
            }
            Ok(Some(make_chunk(1)))
        }
        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }
    }

    #[test]
    fn driver_parks_on_output_full_and_pending_finish_then_finishes() {
        let runtime_state = Arc::new(RuntimeState::default());

        // gate starts closed so the sink saturates and the driver must block.
        let (mut backend, _rows, _chunks) = TestAsyncSink::new(0);
        backend.finish_delay = Duration::from_millis(150);
        let gate = Arc::clone(&backend.gate);
        let mut sink = AsyncSinkOperator::new("driver_sink", backend, 2);
        // A directly-constructed PipelineDriver does NOT call bind_runtime_state
        // (only the pipeline builder does). Bind here — with the SAME RuntimeState
        // the driver uses — so the sink's drain task spawns; otherwise the queue
        // never drains and the driver would hang on OutputFull forever.
        sink.bind_runtime_state(&runtime_state).expect("bind sink");

        let driver_state = Arc::clone(&runtime_state);
        let mut driver = PipelineDriver::new(
            1,
            vec![Box::new(TestSource::new(6)), Box::new(sink)],
            None,
            Vec::new(),
            driver_state,
            None,
        );

        // Drive until the sink reports OutputFull (queue saturated, gate closed).
        let mut saw_output_full = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let st = driver.process(Duration::from_millis(10));
            if matches!(st, DriverState::Blocked(BlockedReason::OutputFull)) {
                saw_output_full = true;
                break;
            }
            if matches!(st, DriverState::Failed(_) | DriverState::Finished) {
                break;
            }
        }
        assert!(saw_output_full, "driver never parked on OutputFull");

        // Release the gate; keep driving. We must observe PendingFinish then Finished.
        gate.add_permits(1_000);
        let mut saw_pending_finish = false;
        let mut finished = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let st = driver.process(Duration::from_millis(10));
            match st {
                DriverState::PendingFinish => saw_pending_finish = true,
                DriverState::Finished => {
                    finished = true;
                    break;
                }
                DriverState::Failed(e) => panic!("driver failed: {e}"),
                _ => {}
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(saw_pending_finish, "driver never entered PendingFinish");
        assert!(finished, "driver did not reach Finished");
    }
}
