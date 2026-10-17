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
//! Local exchange buffer and partitioning implementation.
//!
//! Responsibilities:
//! - Implements passthrough, broadcast, and partitioned in-process chunk routing.
//! - Maintains per-partition queues, memory statistics, and producer/consumer coordination.
//!
//! Key exported interfaces:
//! - Types: `LocalExchangePartitionSpec`, `LocalExchanger`, `LocalExchangePartitionStats`, `LocalExchangeStats`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
#[cfg(test)]
use crate::exec::operators::data_stream_sink::{
    partition_chunk_by_hash, partition_chunk_by_hash_arrays,
};
use crate::exec::pipeline::chunk_buffer_memory_manager::ChunkBufferMemoryManager;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;
use tracing::debug;

static NEXT_EXCHANGE_ID: AtomicUsize = AtomicUsize::new(1);
const LOCAL_EXCHANGE_NOTIFY_LOG_EVERY: u64 = 1024;
static LOCAL_EXCHANGE_NOTIFY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);
// Notify on every push to avoid missed wakeups with edge-triggered signaling.
const LOCAL_EXCHANGE_NOTIFY_EVERY: u64 = 1;
static LOCAL_EXCHANGE_NOTIFY_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_notify() -> bool {
    LOCAL_EXCHANGE_NOTIFY_LOG_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(LOCAL_EXCHANGE_NOTIFY_LOG_EVERY)
}

fn should_notify_on_push() -> bool {
    if LOCAL_EXCHANGE_NOTIFY_EVERY <= 1 {
        LOCAL_EXCHANGE_NOTIFY_COUNT.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    let every = LOCAL_EXCHANGE_NOTIFY_EVERY.max(2);
    LOCAL_EXCHANGE_NOTIFY_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
}

struct LocalExchangerState {
    partitions: Vec<VecDeque<Chunk>>,
    /// Consumers that have left, by consumer index.
    consumer_closed: Vec<bool>,
    /// Consumers still reading each partition.
    open_consumers: Vec<usize>,
}

#[derive(Clone)]
/// Partitioning strategies used by local exchange routing.
pub(crate) enum LocalExchangePartitionSpec {
    Single,
    Exprs(Vec<ExprId>),
    InputSlotIds(Vec<SlotId>),
}

/// How a local exchange bounds the chunks it queues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalExchangeCapacity {
    /// Byte/row thresholds scaled by the producer count. A full queue
    /// applies backpressure until a consumer frees capacity.
    Buffered,
    /// Pure work handoff: at most `max_chunks` queued chunks in the whole
    /// exchange, independent of producer and consumer counts. A full queue
    /// only applies backpressure; it never spills.
    QueuedChunks { max_chunks: usize },
}

/// In-process exchange buffer that routes chunks by passthrough, broadcast, or partitioned policy.
///
/// Design: docs/adr/ADR-0162-in-memory-query-execution-boundary.md
pub(crate) struct LocalExchanger {
    inner: Arc<Mutex<LocalExchangerState>>,
    exchange_id: usize,
    partition_count: usize,
    consumer_count: usize,
    partition_spec: LocalExchangePartitionSpec,
    capacity: LocalExchangeCapacity,
    arena: Arc<ExprArena>,
    memory_manager: Arc<ChunkBufferMemoryManager>,
    source_observable: Arc<Observable>,
    sink_observable: Arc<Observable>,
    /// Notified once, when every consumer has left.
    closed_observable: Arc<Observable>,
    /// Partitions nobody reads any more; chunks routed to them are dropped.
    /// Set under `inner`, so the push path checks the exact state under that lock.
    closed_partitions: Vec<AtomicBool>,
    queue_tracker: OnceLock<Arc<MemTracker>>,
    remaining_producers: AtomicUsize,
    /// Chunks currently queued over all partitions; changed only under `inner`.
    queued_chunks: AtomicUsize,
    all_consumers_closed: AtomicBool,
    pushed_rows: Vec<AtomicU64>,
    popped_rows: Vec<AtomicU64>,
    pushed_chunks: Vec<AtomicU64>,
    popped_chunks: Vec<AtomicU64>,
}

#[allow(
    dead_code,
    reason = "The explicit constructor is retained for local-exchange integration tests."
)]
impl LocalExchanger {
    pub(crate) fn new(
        partition_count: usize,
        producer_count: usize,
        partition_spec: LocalExchangePartitionSpec,
        arena: Arc<ExprArena>,
    ) -> Arc<Self> {
        Self::new_with_limits(
            partition_count,
            producer_count,
            partition_spec,
            arena,
            1,
            i64::MAX,
        )
    }

    /// Exchange whose consumers each read one partition, bounded by bytes and
    /// rows per producer.
    pub(crate) fn new_with_limits(
        partition_count: usize,
        producer_count: usize,
        partition_spec: LocalExchangePartitionSpec,
        arena: Arc<ExprArena>,
        buffer_mem_limit_per_driver: usize,
        max_buffered_rows: i64,
    ) -> Arc<Self> {
        let max_rows = if max_buffered_rows <= 0 {
            i64::MAX
        } else {
            max_buffered_rows
        };
        let partition_count = partition_count.max(1);
        let memory_manager = Arc::new(ChunkBufferMemoryManager::new(
            producer_count.max(1),
            buffer_mem_limit_per_driver.max(1) as i64,
            max_rows,
        ));
        Self::build(
            partition_count,
            partition_count,
            producer_count,
            partition_spec,
            LocalExchangeCapacity::Buffered,
            memory_manager,
            arena,
        )
    }

    /// Work-handoff exchange: `consumer_count` consumers take whole chunks
    /// from one shared queue that holds at most `max_queued_chunks` chunks.
    /// Output carries no key ownership or order; the queue never spills.
    pub(crate) fn new_handoff(
        producer_count: usize,
        consumer_count: usize,
        max_queued_chunks: usize,
        arena: Arc<ExprArena>,
    ) -> Arc<Self> {
        // Bytes are still accounted for peaks and trackers, but only the chunk
        // count decides admission.
        let memory_manager = Arc::new(ChunkBufferMemoryManager::new(
            producer_count.max(1),
            i64::MAX,
            i64::MAX,
        ));
        Self::build(
            1,
            consumer_count.max(1),
            producer_count,
            LocalExchangePartitionSpec::Single,
            LocalExchangeCapacity::QueuedChunks {
                max_chunks: max_queued_chunks.max(1),
            },
            memory_manager,
            arena,
        )
    }

    fn build(
        partition_count: usize,
        consumer_count: usize,
        producer_count: usize,
        partition_spec: LocalExchangePartitionSpec,
        capacity: LocalExchangeCapacity,
        memory_manager: Arc<ChunkBufferMemoryManager>,
        arena: Arc<ExprArena>,
    ) -> Arc<Self> {
        let partition_count = partition_count.max(1);
        let consumer_count = consumer_count.max(1);
        let exchange_id = NEXT_EXCHANGE_ID.fetch_add(1, Ordering::Relaxed);
        let pushed_rows = (0..partition_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let popped_rows = (0..partition_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let pushed_chunks = (0..partition_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let popped_chunks = (0..partition_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let mut open_consumers = vec![0usize; partition_count];
        for consumer in 0..consumer_count {
            open_consumers[consumer % partition_count] += 1;
        }
        let closed_partitions = open_consumers
            .iter()
            .map(|open| AtomicBool::new(*open == 0))
            .collect();
        Arc::new(Self {
            inner: Arc::new(Mutex::new(LocalExchangerState {
                partitions: (0..partition_count).map(|_| VecDeque::new()).collect(),
                consumer_closed: vec![false; consumer_count],
                open_consumers,
            })),
            exchange_id,
            partition_count,
            consumer_count,
            partition_spec,
            capacity,
            arena,
            memory_manager,
            source_observable: Arc::new(Observable::new()),
            sink_observable: Arc::new(Observable::new()),
            closed_observable: Arc::new(Observable::new()),
            closed_partitions,
            queue_tracker: OnceLock::new(),
            remaining_producers: AtomicUsize::new(producer_count.max(1)),
            queued_chunks: AtomicUsize::new(0),
            all_consumers_closed: AtomicBool::new(false),
            pushed_rows,
            popped_rows,
            pushed_chunks,
            popped_chunks,
        })
    }

    pub(crate) fn exchange_id(&self) -> usize {
        self.exchange_id
    }

    pub(crate) fn remaining_producers(&self) -> usize {
        self.remaining_producers.load(Ordering::Acquire)
    }

    /// Whether every consumer has left. Producers then stop: nothing they
    /// push can be read, so their sinks report finished.
    pub(crate) fn all_consumers_closed(&self) -> bool {
        self.all_consumers_closed.load(Ordering::Acquire)
    }

    /// Observable notified once every consumer has left.
    pub(crate) fn closed_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.closed_observable)
    }

    pub(crate) const fn consumer_count(&self) -> usize {
        self.consumer_count
    }

    /// Consumer `consumer` leaves the exchange, whether at end of stream or
    /// because its pipeline ended early. Idempotent.
    ///
    /// A partition that no consumer reads any more drops its queued chunks
    /// and every chunk routed to it later. Hash partitions are
    /// never rerouted: another consumer does not own their keys. When the
    /// last consumer leaves, producers are woken so their sinks observe that
    /// they are finished.
    pub(crate) fn close_consumer(&self, consumer: usize) {
        let notify_sink = self.sink_observable.defer_notify();
        let notify_closed = self.closed_observable.defer_notify();
        let (discarded, all_closed) = {
            let mut guard = self.inner.lock().expect("local exchanger lock");
            let Some(closed) = guard.consumer_closed.get_mut(consumer) else {
                return;
            };
            if *closed {
                return;
            }
            *closed = true;
            let partition = consumer % self.partition_count;
            guard.open_consumers[partition] = guard.open_consumers[partition].saturating_sub(1);
            let mut discarded = Vec::new();
            if guard.open_consumers[partition] == 0
                && !self.closed_partitions[partition].swap(true, Ordering::AcqRel)
            {
                discarded = self.drain_partition_locked(&mut guard, partition);
                // Demand withdrawal changes routing even when a necessary
                // sibling still keeps the byte/row queue full. Publish that
                // exact transition once, as well as any capacity relief.
                notify_sink.arm();
            }
            let all_closed = self
                .closed_partitions
                .iter()
                .all(|closed| closed.load(Ordering::Acquire));
            (discarded, all_closed)
        };
        drop(discarded);
        if all_closed && !self.all_consumers_closed.swap(true, Ordering::AcqRel) {
            debug!(
                "LocalExchange all consumers closed: exchange_id={} consumers={}",
                self.exchange_id, self.consumer_count
            );
            notify_sink.arm();
            notify_closed.arm();
        }
    }

    fn drain_partition_locked(
        &self,
        guard: &mut LocalExchangerState,
        partition: usize,
    ) -> Vec<Chunk> {
        let queue = guard
            .partitions
            .get_mut(partition)
            .expect("local exchanger partition");
        let drained = queue.drain(..).collect::<Vec<_>>();
        for chunk in &drained {
            let bytes = i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX);
            let rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
            self.memory_manager.update_memory_usage(-bytes, -rows);
        }
        self.queued_chunks
            .fetch_sub(drained.len(), Ordering::AcqRel);
        drained
    }

    fn partition_closed(&self, partition: usize) -> bool {
        self.closed_partitions
            .get(partition)
            .is_some_and(|closed| closed.load(Ordering::Acquire))
    }

    /// Whether the queue has reached its admission bound.
    fn capacity_full(&self) -> bool {
        match self.capacity {
            LocalExchangeCapacity::Buffered => self.memory_manager.is_full(),
            LocalExchangeCapacity::QueuedChunks { max_chunks } => {
                self.queued_chunks.load(Ordering::Acquire) >= max_chunks
            }
        }
    }

    pub(crate) fn finish_producer(&self) -> bool {
        let notify = self.source_observable.defer_notify();
        let mut current = self.remaining_producers.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return true;
            }
            let next = current - 1;
            match self.remaining_producers.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if next == 0 {
                        debug!(
                            "LocalExchange all producers finished: exchange_id={} remaining_before={} remaining_after={}",
                            self.exchange_id, current, next
                        );
                        notify.arm();
                        return true;
                    }
                    return false;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn need_input(&self) -> bool {
        !self.all_consumers_closed() && !self.capacity_full()
    }

    #[cfg(test)]
    pub(crate) fn accept(
        self: &Arc<Self>,
        state: &RuntimeState,
        chunk: Chunk,
        _sink_driver_seq: usize,
    ) -> Result<(), String> {
        self.accept_with(state, chunk, _sink_driver_seq, |chunk| {
            self.partition_chunk(chunk)
        })
    }

    pub(crate) fn accept_created(
        self: &Arc<Self>,
        state: &RuntimeState,
        chunk: Chunk,
        driver: usize,
        factory: Option<&dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        self.accept_with(state, chunk, driver, |chunk| {
            let factory = factory
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            let control =
                super::compiled_expression::RuntimeKernelControl::new(state.error_state());
            self.partition_chunk_created(chunk, factory, &control)
        })
    }

    fn accept_with<E: From<String>>(
        self: &Arc<Self>,
        state: &RuntimeState,
        chunk: Chunk,
        _driver: usize,
        mut partition: impl FnMut(&Chunk) -> Result<Vec<(usize, Chunk)>, E>,
    ) -> Result<(), E> {
        if chunk.is_empty() || self.all_consumers_closed() {
            return Ok(());
        }
        let queue_tracker = self.queue_mem_tracker(state);
        if self.partition_count <= 1 {
            let mut chunk = chunk;
            if let Some(tracker) = queue_tracker.as_ref() {
                chunk.transfer_to(tracker);
            }
            self.push_chunk_to_partition(0, chunk);
            return Ok(());
        }
        let partitioned = partition(&chunk)?;
        for (idx, mut part_chunk) in partitioned {
            if part_chunk.is_empty() {
                continue;
            }
            if let Some(tracker) = queue_tracker.as_ref() {
                part_chunk.transfer_to(tracker);
            }
            self.push_chunk_to_partition(idx, part_chunk);
        }
        Ok(())
    }

    pub(crate) fn pop_chunk(&self, partition: usize) -> Option<Chunk> {
        self.pop_chunk_from_partition(partition)
    }

    pub(crate) fn source_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.source_observable)
    }

    pub(crate) fn sink_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.sink_observable)
    }

    pub(crate) fn is_done(&self, partition: usize) -> bool {
        if self.remaining_producers.load(Ordering::Acquire) != 0 {
            return false;
        }
        let in_memory_empty = {
            let guard = self.inner.lock().expect("local exchanger lock");
            guard
                .partitions
                .get(partition)
                .expect("local exchanger partition")
                .is_empty()
        };
        if !in_memory_empty {
            return false;
        }
        true
    }

    pub(crate) fn partition_buffered_chunks(&self, partition: usize) -> Option<(usize, usize)> {
        let guard = self.inner.lock().expect("local exchanger lock");
        let buffered = guard.partitions.get(partition).map(|buf| buf.len())?;
        Some((buffered, self.remaining_producers()))
    }

    pub(crate) fn stats_snapshot(&self) -> LocalExchangeStats {
        let guard = self.inner.lock().expect("local exchanger lock");
        let mut partitions = Vec::with_capacity(guard.partitions.len());
        for idx in 0..guard.partitions.len() {
            let pushed_rows = self
                .pushed_rows
                .get(idx)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let popped_rows = self
                .popped_rows
                .get(idx)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let pushed_chunks = self
                .pushed_chunks
                .get(idx)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let popped_chunks = self
                .popped_chunks
                .get(idx)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let buffered_chunks = guard.partitions.get(idx).map(|buf| buf.len()).unwrap_or(0);
            partitions.push(LocalExchangePartitionStats {
                partition: idx,
                pushed_rows,
                popped_rows,
                pushed_chunks,
                popped_chunks,
                buffered_chunks,
            });
        }
        LocalExchangeStats {
            exchange_id: self.exchange_id,
            remaining_producers: self.remaining_producers(),
            partitions,
        }
    }

    fn queue_mem_tracker(&self, state: &RuntimeState) -> Option<Arc<MemTracker>> {
        let root = state.mem_tracker()?;
        let tracker = self.queue_tracker.get_or_init(|| {
            let label = format!("local_exchange_queue_{}", self.exchange_id);
            MemTracker::new_child(label, &root)
        });
        Some(Arc::clone(tracker))
    }

    fn partition_chunk_created(
        &self,
        chunk: &Chunk,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        control: &super::compiled_expression::RuntimeKernelControl,
    ) -> crate::runtime::fragment::ExecutionResult<Vec<(usize, Chunk)>> {
        let partitioned = match &self.partition_spec {
            LocalExchangePartitionSpec::Exprs(exprs) => {
                let owner = factory.begin_input_construction(chunk)?;
                let evaluation = crate::exec::expr::ExprEvaluationContext::new(
                    &self.arena,
                    owner.as_ref(),
                    control,
                );
                super::data_stream_sink::partition_chunk_by_hash_created(
                    chunk,
                    exprs,
                    &evaluation,
                    self.partition_count,
                    false,
                    factory,
                    control,
                )?
            }
            LocalExchangePartitionSpec::InputSlotIds(slots) => {
                let arrays = slots
                    .iter()
                    .map(|slot| chunk.column_by_slot_id(*slot))
                    .collect::<Result<Vec<_>, _>>()?;
                super::data_stream_sink::partition_chunk_by_hash_arrays_created(
                    chunk,
                    &arrays,
                    self.partition_count,
                    false,
                    factory,
                    control,
                )?
            }
            LocalExchangePartitionSpec::Single => {
                return Err("local exchange partition spec missing".into());
            }
        };
        Ok(partitioned.into_iter().enumerate().collect())
    }

    #[cfg(test)]
    fn partition_chunk(&self, chunk: &Chunk) -> Result<Vec<(usize, Chunk)>, String> {
        let partitioned = match &self.partition_spec {
            LocalExchangePartitionSpec::Exprs(exprs) => {
                partition_chunk_by_hash(chunk, exprs, &self.arena, self.partition_count, false)
                    .map_err(|e| e.to_string())?
            }
            LocalExchangePartitionSpec::InputSlotIds(slot_ids) => {
                let mut arrays = Vec::with_capacity(slot_ids.len());
                for slot_id in slot_ids {
                    arrays.push(
                        chunk
                            .column_by_slot_id(*slot_id)
                            .map_err(|e| e.to_string())?,
                    );
                }
                partition_chunk_by_hash_arrays(chunk, &arrays, self.partition_count, false)
                    .map_err(|e| e.to_string())?
            }
            LocalExchangePartitionSpec::Single => {
                return Err("local exchange partition spec missing".to_string());
            }
        };
        Ok(partitioned.into_iter().enumerate().collect())
    }

    fn pop_chunk_from_partition(&self, partition: usize) -> Option<Chunk> {
        let notify = self.sink_observable.defer_notify();
        let mut popped_rows = 0u64;
        let mut popped_chunks = 0u64;
        let mut has_chunk = false;
        let mut notify_sink = false;
        let chunk = {
            let mut guard = self.inner.lock().expect("local exchanger lock");
            let was_full = self.capacity_full();
            let queue = guard
                .partitions
                .get_mut(partition)
                .expect("local exchanger partition");
            let chunk = queue.pop_front();
            if let Some(ref c) = chunk {
                let bytes = i64::try_from(c.estimated_bytes()).unwrap_or(i64::MAX);
                let rows = i64::try_from(c.len()).unwrap_or(i64::MAX);
                self.memory_manager.update_memory_usage(-bytes, -rows);
                self.queued_chunks.fetch_sub(1, Ordering::AcqRel);
                has_chunk = true;
                popped_rows = c.len() as u64;
                popped_chunks = 1;
            }
            let is_full = self.capacity_full();
            if was_full && !is_full {
                notify_sink = true;
            }
            chunk
        };
        if notify_sink {
            notify.arm();
        }
        if has_chunk {
            if let Some(counter) = self.popped_rows.get(partition) {
                counter.fetch_add(popped_rows, Ordering::Relaxed);
            }
            if let Some(counter) = self.popped_chunks.get(partition) {
                counter.fetch_add(popped_chunks, Ordering::Relaxed);
            }
        }
        chunk
    }

    fn push_chunk_to_partition(&self, partition: usize, chunk: Chunk) {
        let notify = self.source_observable.defer_notify();
        let row_count = chunk.len();
        let bytes = i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX);
        let rows = i64::try_from(row_count).unwrap_or(i64::MAX);
        let (notify_source, buffered_after) = {
            let mut guard = self.inner.lock().expect("local exchanger lock");
            if self.partition_closed(partition) {
                // Nobody reads this partition any more; late pushes must not refill it.
                drop(guard);
                drop(chunk);
                return;
            }
            let queue = guard
                .partitions
                .get_mut(partition)
                .expect("local exchanger partition");
            let was_empty = queue.is_empty();
            queue.push_back(chunk);
            self.queued_chunks.fetch_add(1, Ordering::AcqRel);
            self.memory_manager.update_memory_usage(bytes, rows);
            (was_empty, queue.len())
        };
        if let Some(counter) = self.pushed_rows.get(partition) {
            counter.fetch_add(row_count as u64, Ordering::Relaxed);
        }
        if let Some(counter) = self.pushed_chunks.get(partition) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if notify_source || should_notify_on_push() {
            if should_log_notify() {
                debug!(
                    "LocalExchange notify source: exchange_id={} partition={} buffered_chunks={} remaining_producers={}",
                    self.exchange_id,
                    partition,
                    buffered_after,
                    self.remaining_producers.load(Ordering::Acquire)
                );
            }
            notify.arm();
        }
    }
}

/// Per-partition queue statistics reported by local exchange.
pub(crate) struct LocalExchangePartitionStats {
    pub partition: usize,
    pub pushed_rows: u64,
    pub popped_rows: u64,
    pub pushed_chunks: u64,
    pub popped_chunks: u64,
    pub buffered_chunks: usize,
}

/// Aggregated local-exchange queue and memory statistics.
pub(crate) struct LocalExchangeStats {
    pub exchange_id: usize,
    pub remaining_producers: usize,
    pub partitions: Vec<LocalExchangePartitionStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::Schema;

    use crate::exec::chunk::ChunkSchema;

    fn exchanger() -> Arc<LocalExchanger> {
        LocalExchanger::new(
            1,
            1,
            LocalExchangePartitionSpec::Single,
            Arc::new(ExprArena::default()),
        )
    }

    fn int_chunk(value: i32) -> Chunk {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::{DataType, Field};
        use arrow::record_batch::RecordBatch;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(vec![value])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn closed_hash_partition_drops_late_chunks_without_rerouting() {
        let exchanger = LocalExchanger::new_with_limits(
            2,
            1,
            LocalExchangePartitionSpec::InputSlotIds(vec![SlotId::new(1)]),
            Arc::new(ExprArena::default()),
            1 << 30,
            -1,
        );
        let queued = int_chunk(1);
        let queued_array = Arc::downgrade(&queued.columns()[0]);
        exchanger.push_chunk_to_partition(1, queued);
        assert!(queued_array.upgrade().is_some());
        exchanger.close_consumer(1);
        assert!(
            queued_array.upgrade().is_none(),
            "closing releases the queued Arrow owner"
        );
        assert_eq!(
            exchanger
                .partition_buffered_chunks(1)
                .map(|(chunks, _)| chunks),
            Some(0)
        );

        // Late pushes for the closed partition are dropped; the
        // partition's keys are not handed to the other consumer.
        let late = int_chunk(2);
        let late_array = Arc::downgrade(&late.columns()[0]);
        exchanger.push_chunk_to_partition(1, late);
        assert!(
            late_array.upgrade().is_none(),
            "late pushes release their Arrow owner"
        );
        assert_eq!(
            exchanger
                .partition_buffered_chunks(1)
                .map(|(chunks, _)| chunks),
            Some(0)
        );
        exchanger.push_chunk_to_partition(0, int_chunk(3));
        assert_eq!(
            exchanger
                .partition_buffered_chunks(0)
                .map(|(chunks, _)| chunks),
            Some(1)
        );
        assert!(!exchanger.all_consumers_closed());
        assert!(exchanger.need_input());

        exchanger.close_consumer(0);
        assert!(exchanger.all_consumers_closed());
        assert!(!exchanger.need_input());
        assert_eq!(
            exchanger
                .partition_buffered_chunks(0)
                .map(|(chunks, _)| chunks),
            Some(0)
        );
    }

    #[test]
    fn natural_end_of_one_consumer_does_not_close_a_shared_handoff_queue() {
        let exchanger = LocalExchanger::new_handoff(1, 3, 4, Arc::new(ExprArena::default()));
        exchanger.close_consumer(0);
        exchanger.close_consumer(0);
        exchanger.close_consumer(1);
        assert!(
            !exchanger.all_consumers_closed(),
            "two of three consumers left, and a repeated close counts once"
        );
        exchanger.close_consumer(2);
        assert!(exchanger.all_consumers_closed());
    }

    #[test]
    fn observable_identity_is_stable_for_the_exchanger_lifetime() {
        let exchanger = exchanger();
        let source = exchanger.source_observable();
        let sink = exchanger.sink_observable();

        assert!(Arc::ptr_eq(&source, &exchanger.source_observable()));
        assert!(Arc::ptr_eq(&sink, &exchanger.sink_observable()));
        assert!(!Arc::ptr_eq(&source, &sink));
    }
}
