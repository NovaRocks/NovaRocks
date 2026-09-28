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
use std::time::Instant;

use arrow::datatypes::SchemaRef;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::operators::data_stream_sink::{
    partition_chunk_by_hash, partition_chunk_by_hash_arrays,
};
use crate::exec::pipeline::chunk_buffer_memory_manager::ChunkBufferMemoryManager;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::spill::spill_channel::SpillChannelHandle;
use crate::exec::spill::spiller::{SpillFile, Spiller};
use crate::exec::spill::{SpillConfig, SpillMode, SpillProfile};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;
use tracing::debug;
use tracing::warn;

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

#[derive(Debug, Clone)]
struct SpillFileEntry {
    schema: SchemaRef,
    chunk_schema: ChunkSchemaRef,
    file: SpillFile,
}

#[derive(Debug, Clone)]
struct SpillInput {
    partition: usize,
    chunks: Vec<Chunk>,
    rows: u64,
    bytes: u64,
}

struct LocalExchangerSpillState {
    config: SpillConfig,
    spiller: Arc<Spiller>,
    channel: SpillChannelHandle,
    profile: Option<SpillProfile>,
    spill_inflight: AtomicBool,
    spill_blocked: AtomicBool,
    restore_inflight: AtomicBool,
    restore_blocked: AtomicBool,
    spill_files: Mutex<Vec<VecDeque<SpillFileEntry>>>,
    capacity_forwarding_installed: OnceLock<()>,
}

impl LocalExchangerSpillState {
    fn new(
        config: SpillConfig,
        spiller: Arc<Spiller>,
        channel: SpillChannelHandle,
        profile: Option<SpillProfile>,
        partition_count: usize,
    ) -> Self {
        let mut spill_files = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            spill_files.push(VecDeque::new());
        }
        Self {
            config,
            spiller,
            channel,
            profile,
            spill_inflight: AtomicBool::new(false),
            spill_blocked: AtomicBool::new(false),
            restore_inflight: AtomicBool::new(false),
            restore_blocked: AtomicBool::new(false),
            spill_files: Mutex::new(spill_files),
            capacity_forwarding_installed: OnceLock::new(),
        }
    }
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
    /// Byte/row thresholds scaled by the producer count. A full queue may
    /// spill when the query's spill policy allows it.
    Buffered,
    /// Pure work handoff: at most `max_chunks` queued chunks in the whole
    /// exchange, independent of producer and consumer counts. A full queue
    /// only applies backpressure; it never spills.
    QueuedChunks { max_chunks: usize },
}

/// In-process exchange buffer that routes chunks by passthrough, broadcast, or partitioned policy.
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
    /// Partitions nobody reads any more; chunks and spill files routed to
    /// them are dropped. Set under `inner`, so the push path's check under
    /// `inner` is exact; the spill path reads it under its own file lock.
    closed_partitions: Vec<AtomicBool>,
    queue_tracker: OnceLock<Arc<MemTracker>>,
    spill_state: OnceLock<Arc<LocalExchangerSpillState>>,
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
            spill_state: OnceLock::new(),
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
    /// A partition that no consumer reads any more drops its queued and
    /// spilled chunks and every chunk routed to it later. Hash partitions are
    /// never rerouted: another consumer does not own their keys. When the
    /// last consumer leaves, producers are woken so their sinks observe that
    /// they are finished.
    pub(crate) fn close_consumer(&self, consumer: usize) {
        let notify_sink = self.sink_observable.defer_notify();
        let notify_closed = self.closed_observable.defer_notify();
        let (closed_partition, discarded, all_closed) = {
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
            let mut closed_partition = None;
            let mut discarded = Vec::new();
            if guard.open_consumers[partition] == 0
                && !self.closed_partitions[partition].swap(true, Ordering::AcqRel)
            {
                closed_partition = Some(partition);
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
            (closed_partition, discarded, all_closed)
        };
        drop(discarded);
        if let Some(partition) = closed_partition {
            self.discard_spilled_partition(partition);
        }
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

    fn discard_spilled_partition(&self, partition: usize) {
        let Some(spill) = self.spill_state() else {
            return;
        };
        let entries = {
            let mut guard = spill.spill_files.lock().expect("spill files lock");
            guard
                .get_mut(partition)
                .map(std::mem::take)
                .unwrap_or_default()
        };
        self.remove_discarded_spill_files(entries);
    }

    fn partition_closed(&self, partition: usize) -> bool {
        self.closed_partitions
            .get(partition)
            .is_some_and(|closed| closed.load(Ordering::Acquire))
    }

    fn remove_discarded_spill_files(&self, entries: impl IntoIterator<Item = SpillFileEntry>) {
        for entry in entries {
            if let Err(err) = std::fs::remove_file(&entry.file.path) {
                warn!(
                    "LocalExchange remove discarded spill file failed: exchange_id={} path={} error={}",
                    self.exchange_id,
                    entry.file.path.display(),
                    err
                );
            }
        }
    }

    /// Files a spill task wrote. A partition closed while its task ran keeps
    /// none of them. The closed flag is read under the file lock that
    /// `discard_spilled_partition` takes after setting it, so every file is
    /// either discarded here or reaches that discard.
    fn install_spilled_files(
        &self,
        spill_state: &LocalExchangerSpillState,
        spilled_files: Vec<(usize, SpillFileEntry)>,
    ) {
        if spilled_files.is_empty() {
            return;
        }
        let mut discarded = Vec::new();
        {
            let mut guard = spill_state.spill_files.lock().expect("spill files lock");
            for (partition, entry) in spilled_files {
                if self.partition_closed(partition) {
                    discarded.push(entry);
                } else if let Some(queue) = guard.get_mut(partition) {
                    queue.push_back(entry);
                }
            }
        }
        self.remove_discarded_spill_files(discarded);
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

    pub(crate) fn need_input(self: &Arc<Self>) -> bool {
        if self.all_consumers_closed() {
            return false;
        }
        if let LocalExchangeCapacity::QueuedChunks { .. } = self.capacity {
            return !self.capacity_full();
        }
        if self.memory_manager.is_full() {
            self.maybe_schedule_spill_without_state();
            return false;
        }
        if let Some(spill) = self.spill_state()
            && spill.spill_inflight.load(Ordering::Acquire)
        {
            return false;
        }
        true
    }

    pub(crate) fn accept(
        self: &Arc<Self>,
        state: &RuntimeState,
        chunk: Chunk,
        _sink_driver_seq: usize,
    ) -> Result<(), String> {
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
            self.maybe_schedule_spill(state)?;
            return Ok(());
        }
        let partitioned = self.partition_chunk(&chunk)?;
        for (idx, mut part_chunk) in partitioned {
            if part_chunk.is_empty() {
                continue;
            }
            if let Some(tracker) = queue_tracker.as_ref() {
                part_chunk.transfer_to(tracker);
            }
            self.push_chunk_to_partition(idx, part_chunk);
        }
        self.maybe_schedule_spill(state)?;
        Ok(())
    }

    pub(crate) fn pop_chunk(
        self: &Arc<Self>,
        state: &RuntimeState,
        partition: usize,
    ) -> Option<Chunk> {
        let chunk = self.pop_chunk_from_partition(partition);
        if chunk.is_some() {
            return chunk;
        }
        self.maybe_schedule_restore(state, partition);
        None
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
        if let Some(spill) = self.spill_state() {
            if spill.spill_inflight.load(Ordering::Acquire)
                || spill.restore_inflight.load(Ordering::Acquire)
            {
                return false;
            }
            if self.partition_has_spill_files(&spill, partition) {
                return false;
            }
        }
        true
    }

    pub(crate) fn partition_buffered_chunks(&self, partition: usize) -> Option<(usize, usize)> {
        let guard = self.inner.lock().expect("local exchanger lock");
        let buffered = guard.partitions.get(partition).map(|buf| buf.len())?;
        Some((buffered, self.remaining_producers()))
    }

    pub(crate) fn has_spill_pending(&self, partition: usize) -> bool {
        let Some(spill) = self.spill_state() else {
            return false;
        };
        self.partition_has_spill_files(&spill, partition)
    }

    pub(crate) fn restore_inflight(&self) -> bool {
        self.spill_state()
            .map(|spill| spill.restore_inflight.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    pub(crate) fn restore_blocked(&self) -> bool {
        self.spill_state()
            .map(|spill| spill.restore_blocked.load(Ordering::Acquire))
            .unwrap_or(false)
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

    fn spill_state(&self) -> Option<Arc<LocalExchangerSpillState>> {
        self.spill_state.get().cloned()
    }

    fn ensure_spill_state(
        &self,
        state: &RuntimeState,
    ) -> Result<Option<Arc<LocalExchangerSpillState>>, String> {
        if let Some(existing) = self.spill_state.get() {
            self.ensure_spill_capacity_forwarding(existing);
            return Ok(Some(Arc::clone(existing)));
        }
        if let LocalExchangeCapacity::QueuedChunks { .. } = self.capacity {
            // A handoff queue is a scheduling buffer, not a place to park
            // data: when it is full the producer waits.
            return Ok(None);
        }
        let Some(config) = state.spill_config().cloned() else {
            return Ok(None);
        };
        if !config.enable_spill {
            return Ok(None);
        }
        let manager = state
            .spill_manager()
            .ok_or_else(|| "spill manager is missing".to_string())?;
        let runtime = state.execution_runtime().ok_or_else(|| {
            "spill-enabled local exchange requires an execution runtime".to_string()
        })?;
        let spiller = Arc::new(Spiller::new_from_execution_runtime(
            runtime,
            config.spill_encode_level,
        )?);
        let spill_state = Arc::new(LocalExchangerSpillState::new(
            config,
            spiller,
            manager.channel(),
            manager.profile(),
            self.partition_count,
        ));
        let installed = self.spill_state.get_or_init(|| spill_state);
        self.ensure_spill_capacity_forwarding(installed);
        Ok(Some(Arc::clone(installed)))
    }

    fn ensure_spill_capacity_forwarding(&self, spill: &Arc<LocalExchangerSpillState>) {
        spill.capacity_forwarding_installed.get_or_init(|| {
            let capacity = spill.channel.capacity_observable();
            self.forward_spill_capacity(spill, &capacity);
        });
    }

    fn forward_spill_capacity(
        &self,
        spill: &Arc<LocalExchangerSpillState>,
        capacity: &Arc<Observable>,
    ) {
        // Capacity recovery is a state transition, not merely a wakeup. Clear
        // the matching blocked latch before notifying the stable operator-facing
        // observable so the resumed driver can retry submission. Keep every
        // callback edge weak because the capacity observable is owned by spill.
        let spill = Arc::downgrade(spill);
        let source = Arc::downgrade(&self.source_observable);
        let sink = Arc::downgrade(&self.sink_observable);
        capacity.add_observer(Arc::new(move || {
            let Some(spill) = spill.upgrade() else {
                return;
            };
            let restore_ready = spill.restore_blocked.swap(false, Ordering::AcqRel);
            let spill_ready = spill.spill_blocked.swap(false, Ordering::AcqRel);
            if restore_ready && let Some(source) = source.upgrade() {
                source.notify_observers();
            }
            if spill_ready && let Some(sink) = sink.upgrade() {
                sink.notify_observers();
            }
        }));
    }

    fn maybe_schedule_spill(self: &Arc<Self>, state: &RuntimeState) -> Result<(), String> {
        let Some(spill) = self.ensure_spill_state(state)? else {
            return Ok(());
        };
        self.schedule_spill_if_needed(spill)
    }

    fn maybe_schedule_spill_without_state(self: &Arc<Self>) {
        let Some(spill) = self.spill_state() else {
            return;
        };
        let _ = self.schedule_spill_if_needed(spill);
    }

    fn schedule_spill_if_needed(
        self: &Arc<Self>,
        spill: Arc<LocalExchangerSpillState>,
    ) -> Result<(), String> {
        if !self.should_trigger_spill(&spill) {
            return Ok(());
        }
        if spill
            .spill_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let (inputs, notify_sink) = self.drain_for_spill();
        if inputs.is_empty() {
            spill.spill_inflight.store(false, Ordering::Release);
            return Ok(());
        }
        if notify_sink {
            let notify = self.sink_observable.defer_notify();
            notify.arm();
        }

        let inputs_for_task = inputs.clone();
        let exchanger = Arc::clone(self);
        let spill_state = Arc::clone(&spill);
        match spill.channel.submit(Box::new(move || {
            exchanger.run_spill_task(spill_state, inputs_for_task)
        })) {
            Ok(()) => {
                spill.spill_blocked.store(false, Ordering::Release);
                Ok(())
            }
            Err(err) => {
                spill.spill_inflight.store(false, Ordering::Release);
                spill.spill_blocked.store(true, Ordering::Release);
                if let Some(profile) = spill.profile.as_ref() {
                    profile.spill_block_count.add(1);
                }
                spill.channel.register_capacity_waiter();
                self.requeue_spill_inputs(inputs);
                warn!(
                    "LocalExchange spill submit failed: exchange_id={} error={}",
                    self.exchange_id, err
                );
                Ok(())
            }
        }
    }

    fn should_trigger_spill(&self, spill: &LocalExchangerSpillState) -> bool {
        if spill.config.spill_mode == SpillMode::None {
            return false;
        }
        let buffered_bytes = self.memory_manager.get_memory_usage();
        if buffered_bytes <= 0 {
            return false;
        }
        if let Some(max_bytes) = spill.config.spill_operator_max_bytes
            && buffered_bytes > max_bytes
        {
            return true;
        }
        let min_bytes = spill.config.spill_operator_min_bytes.unwrap_or(0);
        if buffered_bytes < min_bytes {
            return false;
        }
        match spill.config.spill_mode {
            SpillMode::Force => true,
            SpillMode::Auto => self.memory_manager.is_full(),
            SpillMode::Random | SpillMode::None => false,
        }
    }

    fn drain_for_spill(&self) -> (Vec<SpillInput>, bool) {
        let mut inputs = Vec::new();
        let mut notify_sink = false;
        let mut guard = self.inner.lock().expect("local exchanger lock");
        let was_full = self.memory_manager.is_full();
        for (partition, queue) in guard.partitions.iter_mut().enumerate() {
            if queue.is_empty() {
                continue;
            }
            self.queued_chunks.fetch_sub(queue.len(), Ordering::AcqRel);
            let mut chunks = Vec::with_capacity(queue.len());
            let mut rows = 0u64;
            let mut bytes = 0u64;
            while let Some(chunk) = queue.pop_front() {
                let chunk_rows = chunk.len() as u64;
                let chunk_bytes = chunk.estimated_bytes() as u64;
                rows = rows.saturating_add(chunk_rows);
                bytes = bytes.saturating_add(chunk_bytes);
                let bytes_i64 = i64::try_from(chunk_bytes).unwrap_or(i64::MAX);
                let rows_i64 = i64::try_from(chunk_rows).unwrap_or(i64::MAX);
                self.memory_manager
                    .update_memory_usage(-bytes_i64, -rows_i64);
                chunks.push(chunk);
            }
            inputs.push(SpillInput {
                partition,
                chunks,
                rows,
                bytes,
            });
        }
        if was_full && !self.memory_manager.is_full() {
            notify_sink = true;
        }
        (inputs, notify_sink)
    }

    fn requeue_spill_inputs(&self, inputs: Vec<SpillInput>) {
        for input in inputs {
            for chunk in input.chunks {
                self.push_chunk_to_partition_inner(input.partition, chunk, false);
            }
        }
    }

    fn run_spill_task(
        self: Arc<Self>,
        spill_state: Arc<LocalExchangerSpillState>,
        inputs: Vec<SpillInput>,
    ) -> Result<(), String> {
        let start = Instant::now();
        let mut spilled_rows = 0u64;
        let mut spilled_bytes = 0u64;
        let mut spilled_files = Vec::new();
        let mut failed_inputs = Vec::new();

        for input in inputs {
            if input.chunks.is_empty() {
                continue;
            }
            let schema = input.chunks[0].schema();
            let chunk_schema = input.chunks[0].chunk_schema_ref();
            match spill_state
                .spiller
                .spill_chunks(schema.clone(), &input.chunks)
            {
                Ok(file) => {
                    spilled_rows = spilled_rows.saturating_add(input.rows);
                    spilled_bytes = spilled_bytes.saturating_add(input.bytes);
                    spilled_files.push((
                        input.partition,
                        SpillFileEntry {
                            schema,
                            chunk_schema,
                            file,
                        },
                    ));
                }
                Err(err) => {
                    warn!(
                        "LocalExchange spill failed: exchange_id={} partition={} error={}",
                        self.exchange_id, input.partition, err
                    );
                    failed_inputs.push(input);
                }
            }
        }

        self.install_spilled_files(&spill_state, spilled_files);

        if !failed_inputs.is_empty() {
            self.requeue_spill_inputs(failed_inputs);
        }

        if let Some(profile) = spill_state.profile.as_ref() {
            let elapsed_ns = start.elapsed().as_nanos();
            let elapsed_ns = i64::try_from(elapsed_ns).unwrap_or(i64::MAX);
            profile.spill_time.add(elapsed_ns);
            profile
                .spill_rows
                .add(i64::try_from(spilled_rows).unwrap_or(i64::MAX));
            profile
                .spill_bytes
                .add(i64::try_from(spilled_bytes).unwrap_or(i64::MAX));
        }

        spill_state.spill_inflight.store(false, Ordering::Release);
        let notify = self.sink_observable.defer_notify();
        notify.arm();
        Ok(())
    }

    fn maybe_schedule_restore(self: &Arc<Self>, state: &RuntimeState, partition: usize) {
        if self.partition_closed(partition) {
            return;
        }
        let Some(spill) = self.spill_state() else {
            return;
        };
        if spill
            .restore_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let Some(entry) = self.take_spill_file(&spill, partition) else {
            spill.restore_inflight.store(false, Ordering::Release);
            return;
        };
        let entry_for_task = entry.clone();
        let queue_tracker = self.queue_mem_tracker(state);
        let exchanger = Arc::clone(self);
        let spill_state = Arc::clone(&spill);
        match spill.channel.submit(Box::new(move || {
            exchanger.run_restore_task(spill_state, partition, entry_for_task, queue_tracker)
        })) {
            Ok(()) => {
                spill.restore_blocked.store(false, Ordering::Release);
            }
            Err(err) => {
                spill.restore_inflight.store(false, Ordering::Release);
                spill.restore_blocked.store(true, Ordering::Release);
                spill.channel.register_capacity_waiter();
                self.push_spill_file_front(&spill, partition, entry);
                warn!(
                    "LocalExchange restore submit failed: exchange_id={} error={}",
                    self.exchange_id, err
                );
            }
        }
    }

    fn run_restore_task(
        self: Arc<Self>,
        spill_state: Arc<LocalExchangerSpillState>,
        partition: usize,
        entry: SpillFileEntry,
        queue_tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let start = Instant::now();
        let result = spill_state.spiller.restore_chunks(
            entry.schema.clone(),
            entry.chunk_schema.clone(),
            &entry.file,
        );
        match result {
            Ok(chunks) => {
                let mut restore_rows = 0u64;
                let mut restore_bytes = 0u64;
                for mut chunk in chunks {
                    restore_rows = restore_rows.saturating_add(chunk.len() as u64);
                    restore_bytes = restore_bytes.saturating_add(chunk.estimated_bytes() as u64);
                    if let Some(tracker) = queue_tracker.as_ref() {
                        chunk.transfer_to(tracker);
                    }
                    self.push_chunk_to_partition_inner(partition, chunk, false);
                }
                if let Err(err) = std::fs::remove_file(&entry.file.path) {
                    warn!(
                        "LocalExchange remove spill file failed: exchange_id={} path={} error={}",
                        self.exchange_id,
                        entry.file.path.display(),
                        err
                    );
                }
                if let Some(profile) = spill_state.profile.as_ref() {
                    let elapsed_ns = start.elapsed().as_nanos();
                    let elapsed_ns = i64::try_from(elapsed_ns).unwrap_or(i64::MAX);
                    profile.restore_time.add(elapsed_ns);
                    profile
                        .restore_rows
                        .add(i64::try_from(restore_rows).unwrap_or(i64::MAX));
                    profile
                        .restore_bytes
                        .add(i64::try_from(restore_bytes).unwrap_or(i64::MAX));
                    profile.spill_read_io_count.add(1);
                }
            }
            Err(err) => {
                warn!(
                    "LocalExchange restore failed: exchange_id={} partition={} error={}",
                    self.exchange_id, partition, err
                );
                self.push_spill_file_front(&spill_state, partition, entry);
            }
        }

        spill_state.restore_inflight.store(false, Ordering::Release);
        let notify = self.source_observable.defer_notify();
        notify.arm();
        Ok(())
    }

    fn take_spill_file(
        &self,
        spill_state: &LocalExchangerSpillState,
        partition: usize,
    ) -> Option<SpillFileEntry> {
        let mut guard = spill_state.spill_files.lock().expect("spill files lock");
        guard.get_mut(partition).and_then(|queue| queue.pop_front())
    }

    fn push_spill_file_front(
        &self,
        spill_state: &LocalExchangerSpillState,
        partition: usize,
        entry: SpillFileEntry,
    ) {
        let discarded = {
            let mut guard = spill_state.spill_files.lock().expect("spill files lock");
            if self.partition_closed(partition) {
                Some(entry)
            } else {
                if let Some(queue) = guard.get_mut(partition) {
                    queue.push_front(entry);
                }
                None
            }
        };
        self.remove_discarded_spill_files(discarded);
    }

    fn partition_has_spill_files(
        &self,
        spill_state: &LocalExchangerSpillState,
        partition: usize,
    ) -> bool {
        let guard = spill_state.spill_files.lock().expect("spill files lock");
        guard
            .get(partition)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false)
    }

    fn queue_mem_tracker(&self, state: &RuntimeState) -> Option<Arc<MemTracker>> {
        let root = state.mem_tracker()?;
        let tracker = self.queue_tracker.get_or_init(|| {
            let label = format!("local_exchange_queue_{}", self.exchange_id);
            MemTracker::new_child(label, &root)
        });
        Some(Arc::clone(tracker))
    }

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
        self.push_chunk_to_partition_inner(partition, chunk, true);
    }

    fn push_chunk_to_partition_inner(&self, partition: usize, chunk: Chunk, count_stats: bool) {
        let notify = self.source_observable.defer_notify();
        let row_count = chunk.len();
        let bytes = i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX);
        let rows = i64::try_from(row_count).unwrap_or(i64::MAX);
        let (notify_source, buffered_after) = {
            let mut guard = self.inner.lock().expect("local exchanger lock");
            if self.partition_closed(partition) {
                // Nobody reads this partition any more; late pushes and
                // restores must not refill it.
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
        if count_stats {
            if let Some(counter) = self.pushed_rows.get(partition) {
                counter.fetch_add(row_count as u64, Ordering::Relaxed);
            }
            if let Some(counter) = self.pushed_chunks.get(partition) {
                counter.fetch_add(1, Ordering::Relaxed);
            }
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
    use std::path::PathBuf;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Duration;

    use arrow::datatypes::Schema;

    use crate::exec::chunk::ChunkSchema;
    use crate::exec::operators::local_exchange_source::LocalExchangeSourceFactory;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::exec::spill::block_manager::{BlockHeader, BlockMeta};
    use crate::exec::spill::ipc_serde::SpillCodec;

    fn exchanger() -> Arc<LocalExchanger> {
        LocalExchanger::new(
            1,
            1,
            LocalExchangePartitionSpec::Single,
            Arc::new(ExprArena::default()),
        )
    }

    fn spill_config() -> SpillConfig {
        SpillConfig {
            enable_spill: true,
            spill_mode: SpillMode::Force,
            spill_mem_limit_threshold: None,
            spill_operator_min_bytes: None,
            spill_operator_max_bytes: None,
            spill_encode_level: None,
            enable_spill_buffer_read: None,
            max_spill_read_buffer_bytes_per_driver: None,
            spill_mem_table_size: None,
            spill_mem_table_num: None,
        }
    }

    fn install_spill_state(
        exchanger: &Arc<LocalExchanger>,
        channel: SpillChannelHandle,
    ) -> Arc<LocalExchangerSpillState> {
        let spill = Arc::new(LocalExchangerSpillState::new(
            spill_config(),
            Arc::new(Spiller::new()),
            channel,
            None,
            1,
        ));
        assert!(exchanger.spill_state.set(Arc::clone(&spill)).is_ok());
        exchanger.ensure_spill_capacity_forwarding(&spill);
        spill
    }

    fn saturate_channel(channel: &SpillChannelHandle) -> (Sender<()>, Sender<()>, Receiver<()>) {
        let (release_active_tx, release_active_rx) = mpsc::channel();
        let (active_tx, active_rx) = mpsc::channel();
        channel
            .submit(Box::new(move || {
                active_tx.send(()).expect("active task signal");
                release_active_rx.recv().expect("release active task");
                Ok(())
            }))
            .expect("submit active task");
        active_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("active task started");

        let (release_queued_tx, release_queued_rx) = mpsc::channel();
        let (queued_tx, queued_rx) = mpsc::channel();
        channel
            .submit(Box::new(move || {
                queued_tx.send(()).expect("queued task signal");
                release_queued_rx.recv().expect("release queued task");
                Ok(())
            }))
            .expect("fill spill queue");
        (release_active_tx, release_queued_tx, queued_rx)
    }

    fn fake_spill_entry() -> SpillFileEntry {
        SpillFileEntry {
            schema: Arc::new(Schema::empty()),
            chunk_schema: Arc::new(ChunkSchema::empty()),
            file: SpillFile {
                path: PathBuf::from("missing-local-exchange-spill-file"),
                meta: BlockMeta {
                    header: BlockHeader::new(SpillCodec::None, 0),
                    index: Vec::new(),
                },
            },
        }
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
        exchanger.push_chunk_to_partition(1, int_chunk(1));
        exchanger.close_consumer(1);
        assert_eq!(
            exchanger
                .partition_buffered_chunks(1)
                .map(|(chunks, _)| chunks),
            Some(0)
        );

        // Late pushes and restores for the closed partition are dropped; the
        // partition's keys are not handed to the other consumer.
        exchanger.push_chunk_to_partition_inner(1, int_chunk(2), false);
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

    fn spill_entry_at(path: PathBuf) -> SpillFileEntry {
        std::fs::write(&path, b"spilled").expect("write spill file");
        SpillFileEntry {
            file: SpillFile {
                path,
                ..fake_spill_entry().file
            },
            ..fake_spill_entry()
        }
    }

    #[test]
    fn spill_files_of_a_closed_partition_are_removed_wherever_they_land() {
        let dir = tempfile::tempdir().expect("spill dir");
        let exchanger = exchanger();
        let spill = install_spill_state(&exchanger, SpillChannelHandle::new_with_limits(1, 1));
        let queued = dir.path().join("queued");
        spill.spill_files.lock().expect("spill files lock")[0]
            .push_back(spill_entry_at(queued.clone()));

        exchanger.close_consumer(0);
        assert!(!queued.exists(), "queued files go with the partition");

        // A spill task that was running when the partition closed, and a
        // restore that failed after it, land their files afterwards.
        let late_spill = dir.path().join("late-spill");
        exchanger.install_spilled_files(&spill, vec![(0, spill_entry_at(late_spill.clone()))]);
        let failed_restore = dir.path().join("failed-restore");
        exchanger.push_spill_file_front(&spill, 0, spill_entry_at(failed_restore.clone()));

        assert!(!late_spill.exists());
        assert!(!failed_restore.exists());
        assert!(!exchanger.has_spill_pending(0));
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

    #[test]
    fn spill_capacity_clears_blocked_latches_before_waking_stable_observables() {
        let exchanger = exchanger();
        let spill = install_spill_state(&exchanger, SpillChannelHandle::new_with_limits(1, 1));
        let source = exchanger.source_observable();
        let sink = exchanger.sink_observable();
        spill.restore_blocked.store(true, Ordering::Release);
        spill.spill_blocked.store(true, Ordering::Release);
        let source_generation = source.generation();
        let sink_generation = sink.generation();

        spill.channel.capacity_observable().notify_observers();

        assert!(!spill.restore_blocked.load(Ordering::Acquire));
        assert!(!spill.spill_blocked.load(Ordering::Acquire));
        assert_eq!(source.generation(), source_generation + 1);
        assert_eq!(sink.generation(), sink_generation + 1);
        assert!(Arc::ptr_eq(&source, &exchanger.source_observable()));
        assert!(Arc::ptr_eq(&sink, &exchanger.sink_observable()));
    }

    #[test]
    fn restore_retries_after_spill_capacity_recovers() {
        let exchanger = exchanger();
        let channel = SpillChannelHandle::new_with_limits(1, 1);
        let spill = install_spill_state(&exchanger, channel.clone());
        spill.spill_files.lock().expect("spill files lock")[0].push_back(fake_spill_entry());
        let (release_active, release_queued, queued_started) = saturate_channel(&channel);

        exchanger.maybe_schedule_restore(&RuntimeState::default(), 0);
        assert!(spill.restore_blocked.load(Ordering::Acquire));
        assert!(!spill.restore_inflight.load(Ordering::Acquire));

        let source = exchanger.source_observable();
        let (woken_tx, woken_rx) = mpsc::channel();
        source.add_observer(Arc::new(move || {
            let _ = woken_tx.send(());
        }));
        release_active.send(()).expect("release active task");
        queued_started
            .recv_timeout(Duration::from_secs(1))
            .expect("queued task started");
        woken_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("restore capacity wake");

        let factory = LocalExchangeSourceFactory::new(1, 1, Arc::clone(&exchanger));
        let mut source_operator = factory.create(1, 0);
        assert!(
            source_operator
                .as_processor_ref()
                .expect("source processor")
                .has_output()
        );
        assert!(
            source_operator
                .as_processor_mut()
                .expect("source processor")
                .pull_chunk(&RuntimeState::default())
                .expect("pull local exchange")
                .is_none()
        );
        assert!(spill.restore_inflight.load(Ordering::Acquire));
        assert!(!spill.restore_blocked.load(Ordering::Acquire));

        release_queued.send(()).expect("release queued task");
    }

    #[test]
    fn spill_retries_after_spill_capacity_recovers() {
        let exchanger = exchanger();
        let channel = SpillChannelHandle::new_with_limits(1, 1);
        let spill = install_spill_state(&exchanger, channel.clone());
        exchanger
            .inner
            .lock()
            .expect("local exchanger lock")
            .partitions[0]
            .push_back(Chunk::default());
        exchanger.memory_manager.update_memory_usage(1, 0);
        let (release_active, release_queued, queued_started) = saturate_channel(&channel);

        exchanger
            .schedule_spill_if_needed(Arc::clone(&spill))
            .expect("initial spill scheduling");
        assert!(spill.spill_blocked.load(Ordering::Acquire));
        assert!(!spill.spill_inflight.load(Ordering::Acquire));

        let sink = exchanger.sink_observable();
        let (woken_tx, woken_rx) = mpsc::channel();
        sink.add_observer(Arc::new(move || {
            let _ = woken_tx.send(());
        }));
        release_active.send(()).expect("release active task");
        queued_started
            .recv_timeout(Duration::from_secs(1))
            .expect("queued task started");
        woken_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("spill capacity wake");

        assert!(!exchanger.need_input());
        assert!(spill.spill_inflight.load(Ordering::Acquire));
        assert!(!spill.spill_blocked.load(Ordering::Acquire));

        release_queued.send(()).expect("release queued task");
    }
}
