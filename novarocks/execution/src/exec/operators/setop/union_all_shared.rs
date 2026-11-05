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
//! Shared queue state for UNION ALL execution.
//!
//! Responsibilities:
//! - Buffers incoming chunks from multiple sink branches for source-side consumption.
//! - Coordinates sink completion and fair chunk draining across input branches.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::chunk_buffer_memory_manager::ChunkBufferMemoryManager;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::ExecutionResult;

struct UnionAllState {
    buffer: VecDeque<Chunk>,
    remaining_producers: usize,
    consumer_closed: bool,
    abandoned_producer: bool,
}

#[derive(Clone)]
/// Shared queue state for UNION ALL branch fan-in and source-side draining.
pub(crate) struct UnionAllSharedState {
    inner: Arc<Mutex<UnionAllState>>,
    observable: Arc<Observable>,
    label: String,
    queue_tracker: Arc<OnceLock<Arc<MemTracker>>>,
    memory: Arc<OnceLock<ChunkBufferMemoryManager>>,
    producer_count: usize,
}

impl UnionAllSharedState {
    pub(crate) fn new(producer_count: usize, node_id: i32) -> Self {
        let label = if node_id >= 0 {
            format!("union_all_queue_{node_id}")
        } else {
            "union_all_queue".to_string()
        };
        Self {
            inner: Arc::new(Mutex::new(UnionAllState {
                buffer: VecDeque::new(),
                remaining_producers: producer_count,
                consumer_closed: false,
                abandoned_producer: false,
            })),
            observable: Arc::new(Observable::new()),
            label,
            queue_tracker: Arc::new(OnceLock::new()),
            memory: Arc::new(OnceLock::new()),
            producer_count,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_limits(
        producers: usize,
        node_id: i32,
        bytes_per_driver: usize,
        rows: i64,
    ) -> Self {
        let state = Self::new(producers, node_id);
        state.memory.get_or_init(|| {
            ChunkBufferMemoryManager::new(
                producers,
                i64::try_from(bytes_per_driver).unwrap_or(i64::MAX),
                if rows <= 0 { i64::MAX } else { rows },
            )
        });
        state
    }

    pub(crate) fn bind_runtime(&self, state: &RuntimeState) -> ExecutionResult<()> {
        if let Some(runtime) = state.execution_runtime() {
            let config = runtime.config();
            self.memory.get_or_init(|| {
                ChunkBufferMemoryManager::new(
                    self.producer_count,
                    i64::try_from(config.local_exchange_buffer_mem_limit_per_driver)
                        .unwrap_or(i64::MAX),
                    if config.local_exchange_max_buffered_rows <= 0 {
                        i64::MAX
                    } else {
                        config.local_exchange_max_buffered_rows
                    },
                )
            });
        }
        if self.memory.get().is_none() {
            return Err(novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "union queue waterline binding is missing",
                ),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn need_input(&self) -> bool {
        let guard = self.inner.lock().expect("union all state lock");
        !guard.consumer_closed && self.memory.get().is_some_and(|memory| !memory.is_full())
    }
    pub(crate) fn consumer_closed(&self) -> bool {
        self.inner
            .lock()
            .expect("union all state lock")
            .consumer_closed
    }
    pub(crate) fn close_consumer(&self) {
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("union all state lock");
        if !guard.consumer_closed {
            guard.consumer_closed = true;
            guard.buffer.clear();
            if let Some(memory) = self.memory.get() {
                memory.clear();
            }
            notify.arm();
        }
    }

    pub(crate) fn push_chunk(
        &self,
        tracker: Option<&Arc<MemTracker>>,
        mut chunk: Chunk,
    ) -> ExecutionResult<()> {
        let memory = self.memory.get().ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "union queue waterline binding is missing",
                ),
            )
        })?;
        if let Some(tracker) = tracker {
            chunk.transfer_to(tracker);
        }
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("union all state lock");
        if guard.consumer_closed {
            return Ok(());
        }
        let bytes = i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX);
        let rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
        guard.buffer.push_back(chunk);
        memory.update_memory_usage(bytes, rows);
        notify.arm();
        Ok(())
    }

    pub(crate) fn next_chunk(&self) -> crate::exec::pipeline::shape::SourceRead {
        use crate::exec::pipeline::shape::SourceRead;
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("union all state lock");
        match guard.buffer.pop_front() {
            Some(chunk) => {
                if let Some(memory) = self.memory.get() {
                    memory.update_memory_usage(
                        -i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX),
                        -i64::try_from(chunk.len()).unwrap_or(i64::MAX),
                    );
                }
                notify.arm();
                SourceRead::Chunk(chunk)
            }
            None if guard.consumer_closed || guard.remaining_producers == 0 => SourceRead::Finished,
            None => SourceRead::Pending,
        }
    }

    /// Normal producer EOS is distinct from lifecycle teardown. Ordinary
    /// UNION retains its withdrawal behavior; strict writer finish checks it.
    pub(crate) fn normal_input_exhausted(&self) -> bool {
        let state = self.inner.lock().expect("union all state lock");
        state.remaining_producers == 0
            && state.buffer.is_empty()
            && !state.consumer_closed
            && !state.abandoned_producer
    }
    pub(crate) fn producer_abandoned(&self) {
        self.finish_producer(false);
    }
    pub(crate) fn producer_finished(&self) {
        self.finish_producer(true);
    }
    fn finish_producer(&self, normal: bool) {
        let notify = self.observable.defer_notify();
        {
            let mut guard = self.inner.lock().expect("union all state lock");
            guard.abandoned_producer |= !normal;
            if guard.remaining_producers > 0 {
                guard.remaining_producers -= 1;
            }
        }
        notify.arm();
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    pub(crate) fn queue_mem_tracker(&self, state: &RuntimeState) -> Option<Arc<MemTracker>> {
        let root = state.mem_tracker()?;
        let label = self.label.clone();
        let tracker = self
            .queue_tracker
            .get_or_init(|| MemTracker::new_child(label, &root));
        Some(Arc::clone(tracker))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        union_all_sink::UnionAllSinkFactory, union_all_source::UnionAllSourceFactory,
    };
    use super::*;
    use crate::exec::{
        chunk::ChunkSchema,
        pipeline::{
            driver_step::{DriverStep, TurnWork},
            input::RawInput,
            operator_factory::OperatorFactory,
            shape::SourceRead,
            step::{AdmissionNext, InputResult},
            test_driver::TestDriver,
        },
    };
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_types::SlotId;

    fn with_test_step<T>(rt: &RuntimeState, action: impl FnOnce(&mut DriverStep<'_>) -> T) -> T {
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    fn chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let shape =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(1)]).unwrap();
        Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from((0..rows as i32).collect::<Vec<_>>())) as ArrayRef],
            )
            .unwrap(),
            shape,
        )
        .unwrap()
    }
    #[test]
    fn waterline_defers_original_input_and_dequeue_invalidates_wait() {
        let rt = RuntimeState::default();
        let shared = UnionAllSharedState::new_with_limits(1, 7, 1, -1);
        let mut sink = UnionAllSinkFactory::new(shared.clone(), 7).create(1, 0);
        sink.bind_runtime_state(&rt).unwrap();
        let mut calls = TestDriver::new(sink.as_mut(), &rt);
        calls.input_and_collect(chunk(100)).unwrap();
        assert!(
            !shared.need_input(),
            "a single chunk may cross the waterline from below"
        );
        let input = RawInput::for_test(chunk(1));
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = calls.try_push(input)
        else {
            panic!("a full queue must retain the original input upstream");
        };
        assert!(!wait.is_ready());
        let SourceRead::Chunk(output) = shared.next_chunk() else {
            panic!("queued large chunk");
        };
        assert_eq!(output.len(), 100);
        assert!(wait.is_ready());
        assert!(shared.need_input());
        assert!(matches!(
            calls.try_push(input),
            InputResult::Accepted(Ok(_))
        ));
    }
    #[test]
    fn consumer_cancel_drains_custody_and_wakes_blocked_producers() {
        let rt = RuntimeState::default();
        let shared = UnionAllSharedState::new_with_limits(2, 7, 1, 1);
        let tracker = MemTracker::new_root("union retained input");
        let mut retained = chunk(2);
        retained.transfer_to(&tracker);
        let weak = Arc::downgrade(&retained.columns()[0]);
        shared.push_chunk(None, retained).unwrap();
        let observable = shared.observable();
        let generation = observable.generation();
        let mut active = UnionAllSourceFactory::new(shared.clone(), 7).create(1, 0);
        active.cancel();
        assert!(weak.upgrade().is_none());
        assert_eq!(tracker.current(), 0);
        assert!(observable.generation() > generation);
        assert!(shared.consumer_closed());
        assert!(matches!(shared.next_chunk(), SourceRead::Finished));
        let closed_generation = observable.generation();
        active.cancel();
        active.close().unwrap();
        assert_eq!(observable.generation(), closed_generation);
        let mut sink = UnionAllSinkFactory::new(shared.clone(), 7).create(1, 0);
        sink.bind_runtime_state(&rt).unwrap();
        with_test_step(&rt, |ctx| sink.poll_control(ctx)).unwrap();
        sink.close().unwrap();
        sink.cancel();
        assert_eq!(
            shared.inner.lock().unwrap().remaining_producers,
            1,
            "producer lifecycle settles once"
        );
    }
    #[test]
    fn producer_cancel_wakes_empty_source_and_last_retained_row_precedes_eos() {
        let rt = RuntimeState::default();
        let shared = UnionAllSharedState::new_with_limits(2, 7, 1024, 1024);
        let mut first = UnionAllSinkFactory::new(shared.clone(), 7).create(2, 0);
        let mut second = UnionAllSinkFactory::new(shared.clone(), 7).create(2, 1);
        first.bind_runtime_state(&rt).unwrap();
        second.bind_runtime_state(&rt).unwrap();
        let observable = shared.observable();
        let before = observable.generation();
        first.cancel();
        first.close().unwrap();
        assert!(observable.generation() > before);
        assert!(matches!(shared.next_chunk(), SourceRead::Pending));
        shared.push_chunk(None, chunk(3)).unwrap();
        second.cancel();
        second.close().unwrap();
        let SourceRead::Chunk(output) = shared.next_chunk() else {
            panic!("tail is retained before EOS");
        };
        assert_eq!(output.len(), 3);
        assert!(matches!(shared.next_chunk(), SourceRead::Finished));
    }
}
