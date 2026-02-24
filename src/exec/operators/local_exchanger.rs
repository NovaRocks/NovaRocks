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

use crate::common::config::{
    local_exchange_buffer_mem_limit_per_driver, local_exchange_max_buffered_rows,
};
use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
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
use crate::novarocks_logging::debug;
use crate::novarocks_logging::warn;

static NEXT_EXCHANGE_ID: AtomicUsize = AtomicUsize::new(1);
const LOCAL_EXCHANGE_NOTIFY_LOG_EVERY: u64 = 1024;
static LOCAL_EXCHANGE_NOTIFY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);
// Notify on every push to avoid missed wakeups with edge-triggered signaling.
const LOCAL_EXCHANGE_NOTIFY_EVERY: u64 = 1;
static LOCAL_EXCHANGE_NOTIFY_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_notify() -> bool {
    LOCAL_EXCHANGE_NOTIFY_LOG_COUNT.fetch_add(1, Ordering::Relaxed)
        % LOCAL_EXCHANGE_NOTIFY_LOG_EVERY
        == 0
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
}

#[derive(Debug, Clone)]
struct SpillFileEntry {
    schema: SchemaRef,
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

/// In-process exchange buffer that routes chunks by passthrough, broadcast, or partitioned policy.
pub(crate) struct LocalExchanger {
    inner: Arc<Mutex<LocalExchangerState>>,
    exchange_id: usize,
    partition_count: usize,
    partition_spec: LocalExchangePartitionSpec,
    arena: Arc<ExprArena>,
    memory_manager: Arc<ChunkBufferMemoryManager>,
    source_observable: Arc<Observable>,
    sink_observable: Arc<Observable>,
    queue_tracker: OnceLock<Arc<MemTracker>>,
    spill_state: OnceLock<Arc<LocalExchangerSpillState>>,
    remaining_producers: AtomicUsize,
    finished_sources: AtomicUsize,
    pushed_rows: Vec<AtomicU64>,
    popped_rows: Vec<AtomicU64>,
    pushed_chunks: Vec<AtomicU64>,
    popped_chunks: Vec<AtomicU64>,
}

impl LocalExchanger {
    pub(crate) fn new(
        partition_count: usize,
        producer_count: usize,
        partition_spec: LocalExchangePartitionSpec,
        arena: Arc<ExprArena>,
    ) -> Arc<Self> {
        let partition_count = partition_count.max(1);
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
        let max_rows = local_exchange_max_buffered_rows();
        let max_rows = if max_rows <= 0 { i64::MAX } else { max_rows };
        let memory_manager = Arc::new(ChunkBufferMemoryManager::new(
            producer_count.max(1),
            local_exchange_buffer_mem_limit_per_driver() as i64,
            max_rows,
        ));
        Arc::new(Self {
            inner: Arc::new(Mutex::new(LocalExchangerState {
                partitions: (0..partition_count).map(|_| VecDeque::new()).collect(),
            })),
            exchange_id,
            partition_count,
            partition_spec,
            arena,
            memory_manager,
            source_observable: Arc::new(Observable::new()),
            sink_observable: Arc::new(Observable::new()),
            queue_tracker: OnceLock::new(),
            spill_state: OnceLock::new(),
            remaining_producers: AtomicUsize::new(producer_count.max(1)),
            finished_sources: AtomicUsize::new(0),
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

    pub(crate) fn is_all_sources_finished(&self) -> bool {
        self.finished_sources.load(Ordering::Acquire) >= self.partition_count
    }

    pub(crate) fn finish_source(&self) {
        self.finished_sources.fetch_add(1, Ordering::AcqRel);
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
        if self.is_all_sources_finished() {
            return false;
        }
        if self.memory_manager.is_full() {
            self.maybe_schedule_spill_without_state();
            return false;
        }
        if let Some(spill) = self.spill_state() {
            if spill.spill_inflight.load(Ordering::Acquire) {
                return false;
            }
        }
        true
    }

    pub(crate) fn accept(
        self: &Arc<Self>,
        state: &RuntimeState,
        chunk: Chunk,
        _sink_driver_seq: usize,
    ) -> Result<(), String> {
        if chunk.is_empty() {
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
        if let Some(spill) = self.spill_state() {
            if spill.restore_blocked.load(Ordering::Acquire) {
                spill.channel.register_capacity_waiter();
                spill.restore_blocked.store(false, Ordering::Release);
                return spill.channel.capacity_observable();
            }
        }
        Arc::clone(&self.source_observable)
    }

    pub(crate) fn sink_observable(&self) -> Arc<Observable> {
        if let Some(spill) = self.spill_state() {
            if spill.spill_blocked.load(Ordering::Acquire) {
                spill.channel.register_capacity_waiter();
                spill.spill_blocked.store(false, Ordering::Release);
                return spill.channel.capacity_observable();
            }
        }
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
            return Ok(Some(Arc::clone(existing)));
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
        let spiller = Arc::new(Spiller::new_from_config(config.spill_encode_level)?);
        let spill_state = Arc::new(LocalExchangerSpillState::new(
            config,
            spiller,
            manager.channel(),
            manager.profile(),
            self.partition_count,
        ));
        let _ = self.spill_state.set(Arc::clone(&spill_state));
        Ok(self.spill_state())
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
        if let Some(max_bytes) = spill.config.spill_operator_max_bytes {
            if buffered_bytes > max_bytes {
                return true;
            }
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
            match spill_state
                .spiller
                .spill_chunks(schema.clone(), &input.chunks)
            {
                Ok(file) => {
                    spilled_rows = spilled_rows.saturating_add(input.rows);
                    spilled_bytes = spilled_bytes.saturating_add(input.bytes);
                    spilled_files.push((input.partition, SpillFileEntry { schema, file }));
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

        if !spilled_files.is_empty() {
            let mut guard = spill_state.spill_files.lock().expect("spill files lock");
            for (partition, entry) in spilled_files {
                if let Some(queue) = guard.get_mut(partition) {
                    queue.push_back(entry);
                }
            }
        }

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
        let result = spill_state
            .spiller
            .restore_chunks(entry.schema.clone(), &entry.file);
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
        let mut guard = spill_state.spill_files.lock().expect("spill files lock");
        if let Some(queue) = guard.get_mut(partition) {
            queue.push_front(entry);
        }
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
            let was_full = self.memory_manager.is_full();
            let queue = guard
                .partitions
                .get_mut(partition)
                .expect("local exchanger partition");
            let chunk = queue.pop_front();
            if let Some(ref c) = chunk {
                let bytes = i64::try_from(c.estimated_bytes()).unwrap_or(i64::MAX);
                let rows = i64::try_from(c.len()).unwrap_or(i64::MAX);
                self.memory_manager.update_memory_usage(-bytes, -rows);
                has_chunk = true;
                popped_rows = c.len() as u64;
                popped_chunks = 1;
            }
            let is_full = self.memory_manager.is_full();
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
        let rows = chunk.len();
        let bytes = chunk.estimated_bytes();
        if count_stats {
            if let Some(counter) = self.pushed_rows.get(partition) {
                counter.fetch_add(rows as u64, Ordering::Relaxed);
            }
            if let Some(counter) = self.pushed_chunks.get(partition) {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        let rows = i64::try_from(rows).unwrap_or(i64::MAX);
        let (notify_source, buffered_after) = {
            let mut guard = self.inner.lock().expect("local exchanger lock");
            let queue = guard
                .partitions
                .get_mut(partition)
                .expect("local exchanger partition");
            let was_empty = queue.is_empty();
            queue.push_back(chunk);
            self.memory_manager.update_memory_usage(bytes, rows);
            (was_empty, queue.len())
        };
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
