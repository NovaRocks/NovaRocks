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
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read, Write};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A live, bounded test observation supplied by the exact application owner.
/// This interface knows no action vocabulary and grants no authority itself.
#[cfg(novarocks_dev_controls)]
pub trait TestEventObserver: Send + Sync + std::fmt::Debug {
    fn is_active(&self) -> bool;
    fn observe(&self, event: &str);
}

#[cfg(novarocks_dev_controls)]
pub(crate) fn emit_exchange_snapshot_marker(
    observer: Option<&dyn TestEventObserver>,
    build: impl FnOnce() -> String,
) {
    if let Some(observer) = observer.filter(|observer| observer.is_active()) {
        observer.observe(&format!("NOVAROCKS_EXCHANGE_SNAPSHOT {}", build()));
    }
}

use arrow::array::{Array, ArrayRef, Int8Array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::observable::Observable;
use novarocks_execution_contract::{
    ExchangeCloseDisposition, ExchangeSource, ExchangeStreamClose, ExchangeStreamIdentity,
};
use novarocks_types::format_uuid;
use novarocks_types::{SlotId, UniqueId};
use tracing::debug;

mod reader_structure;
mod result_ipc_preflight;
mod unit_ingress;
pub use result_ipc_preflight::{TypedRootResultDecodeBounds, preflight_typed_root_result_decode};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExchangeKey {
    pub finst_id_hi: i64,
    pub finst_id_lo: i64,
    pub node_id: i32,
}

impl ExchangeKey {
    #[inline]
    pub fn finst_uuid(&self) -> String {
        format_uuid(self.finst_id_hi, self.finst_id_lo)
    }
}

/// The sender whose schema and end-of-stream facts this receiver observes.
///
/// Native frames use the immutable source fragment identity and the ordinal
/// frozen by the task graph. Local execution has no native fragment identity,
/// so it remains an explicit, disjoint identity instead of overloading a
/// native sender field.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExchangeSenderIdentity {
    Native {
        source_fragment_instance_id: UniqueId,
        sender_ordinal: u32,
    },
    Local {
        sender_id: i32,
        backend_number: i32,
    },
}

impl ExchangeSenderIdentity {
    pub const fn native(source_fragment_instance_id: UniqueId, sender_ordinal: u32) -> Self {
        Self::Native {
            source_fragment_instance_id,
            sender_ordinal,
        }
    }

    pub const fn local(sender_id: i32, backend_number: i32) -> Self {
        Self::Local {
            sender_id,
            backend_number,
        }
    }
}

impl std::fmt::Display for ExchangeSenderIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native {
                source_fragment_instance_id,
                sender_ordinal,
            } => write!(
                formatter,
                "native(source_finst={}, ordinal={sender_ordinal})",
                format_uuid(
                    source_fragment_instance_id.high(),
                    source_fragment_instance_id.low()
                )
            ),
            Self::Local {
                sender_id,
                backend_number,
            } => write!(
                formatter,
                "local(sender_id={sender_id}, backend_number={backend_number})"
            ),
        }
    }
}

mod failure;
pub use failure::ExchangeFailure;
use failure::{
    contract as exchange_contract, payload as exchange_payload, stopped as exchange_stopped,
};

/// How a registered receiver binds the columns of a decoded wire payload to
/// its expected chunk schema.
///
/// The choice belongs to the receiver's registration and is never inferred
/// from the payload: whether a sender's slot ids mean anything in the
/// receiver's namespace is a property of how both fragments were compiled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeColumnBinding {
    /// Legacy fragments share one slot namespace across the edge: a wire
    /// column binds to the expected slot with the same id whenever every wire
    /// slot id exists in the expected schema, and by position otherwise.
    BySlotId,
    /// Compiled fragments allocate slot ids per fragment, so a sender's ids
    /// carry no meaning in the receiver. Wire column `i` binds to expected
    /// slot `i`, exactly the cut law that pairs `projection[i]` with
    /// `imports[i]`; slot ids are never consulted.
    Positional,
}

impl ExchangeColumnBinding {
    const fn prefers_wire_slot_ids(self) -> bool {
        matches!(self, Self::BySlotId)
    }
}

/// The receiver's registered type contract and how wire columns bind to it.
#[derive(Clone)]
struct ExpectedReceiverSchema {
    chunk_schema: ChunkSchemaRef,
    column_binding: ExchangeColumnBinding,
    creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
}

const CANCELED_KEYS_TTL: Duration = Duration::from_secs(600);
const CANCELED_KEYS_MAX_SIZE: usize = 8192;
const EXCHANGE_NOT_READY_LOG_EVERY: u64 = 1024;
const EXCHANGE_READY_LOG_EVERY: u64 = 4096;
const EXCHANGE_PAYLOAD_MAGIC: &[u8; 4] = b"NRX1";
const EXCHANGE_PAYLOAD_VERSION: u8 = 1;
const EXCHANGE_PAYLOAD_FLAG_SLOT_IDS: u8 = 0x01;
const EXCHANGE_ZERO_COLUMN_MARKER_FIELD: &str = "__novarocks_exchange_zero_column__";

static EXCHANGE_NOT_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);
static EXCHANGE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_not_ready() -> bool {
    EXCHANGE_NOT_READY_LOG_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(EXCHANGE_NOT_READY_LOG_EVERY)
}

fn should_log_exchange_ready() -> bool {
    EXCHANGE_READY_LOG_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(EXCHANGE_READY_LOG_EVERY)
}

/// Instance-owned receiver state for one application-hosted execution runtime.
///
/// The registry deliberately has no process-global fallback: application
/// composition gives the same instance to ingress, fragment preparation and
/// cancellation paths.
pub struct ExecutionExchangeRegistry {
    unit_slots: super::exchange_slots::ExchangeSlotRegistry,
    canceled_keys: Mutex<HashMap<ExchangeKey, Instant>>,
    receivers: Mutex<HashMap<ExchangeKey, Arc<Receiver>>>,
}

impl Default for ExecutionExchangeRegistry {
    fn default() -> Self {
        Self::with_flow_limits(super::execution_runtime::ExchangeFlowLimits::default())
    }
}

impl ExecutionExchangeRegistry {
    pub fn try_flow_snapshot(&self) -> Option<super::exchange_slots::ExchangeFlowSnapshot> {
        self.unit_slots.try_flow_snapshot()
    }

    pub fn with_flow_limits(limits: super::execution_runtime::ExchangeFlowLimits) -> Self {
        Self {
            unit_slots: super::exchange_slots::ExchangeSlotRegistry::new(limits),
            canceled_keys: Mutex::new(HashMap::new()),
            receivers: Mutex::new(HashMap::new()),
        }
    }
}

impl std::fmt::Debug for ExecutionExchangeRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ExecutionExchangeRegistry")
    }
}

fn cleanup_canceled_keys_locked(keys: &mut HashMap<ExchangeKey, Instant>, now: Instant) {
    keys.retain(|_, ts| now.duration_since(*ts) <= CANCELED_KEYS_TTL);
    if keys.len() > CANCELED_KEYS_MAX_SIZE {
        keys.clear();
    }
}

fn mark_key_canceled(registry: &ExecutionExchangeRegistry, key: ExchangeKey) {
    let now = Instant::now();
    let mut guard = registry
        .canceled_keys
        .lock()
        .expect("exchange canceled keys lock");
    cleanup_canceled_keys_locked(&mut guard, now);
    guard.insert(key, now);
}

fn is_key_canceled(registry: &ExecutionExchangeRegistry, key: &ExchangeKey) -> bool {
    let now = Instant::now();
    let mut guard = registry
        .canceled_keys
        .lock()
        .expect("exchange canceled keys lock");
    cleanup_canceled_keys_locked(&mut guard, now);
    guard.contains_key(key)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExchangeWireMeta {
    slot_ids_by_index: Vec<SlotId>,
}

impl ExchangeWireMeta {
    fn from_chunks(chunks: &[Chunk]) -> Result<Option<Self>, ExchangeFailure> {
        let Some(first) = chunks.first() else {
            return Ok(None);
        };
        let slot_ids_by_index = first
            .chunk_schema()
            .slots()
            .iter()
            .map(|slot| slot.slot_id())
            .collect::<Vec<_>>();
        for (idx, chunk) in chunks.iter().enumerate().skip(1) {
            let slot_ids = chunk
                .chunk_schema()
                .slots()
                .iter()
                .map(|slot| slot.slot_id())
                .collect::<Vec<_>>();
            if slot_ids != slot_ids_by_index {
                return Err(exchange_contract(format_args!(
                    "exchange encode schema mismatch at chunk index {}: wire slot id mismatch expected={:?} actual={:?}",
                    idx, slot_ids_by_index, slot_ids
                )));
            }
        }
        Ok(Some(Self { slot_ids_by_index }))
    }
}

#[cfg(test)]
struct DecodedExchangePayload<'a> {
    wire_meta: Option<ExchangeWireMeta>,
    arrow_payload: &'a [u8],
}

/// Provenance is receiver-authored metadata, never a payload-selected Task.
pub struct SourcedExchangeChunk {
    source: Option<ExchangeSource>,
    chunk: Chunk,
}
impl SourcedExchangeChunk {
    pub const fn source(&self) -> Option<ExchangeSource> {
        self.source
    }
    pub fn into_parts(self) -> (Option<ExchangeSource>, Chunk) {
        (self.source, self.chunk)
    }
}
impl std::ops::Deref for SourcedExchangeChunk {
    type Target = Chunk;
    fn deref(&self) -> &Chunk {
        &self.chunk
    }
}
impl std::ops::DerefMut for SourcedExchangeChunk {
    fn deref_mut(&mut self) -> &mut Chunk {
        &mut self.chunk
    }
}
#[derive(Default)]
struct ReceiverState {
    expected_senders: usize,
    units: unit_ingress::UnitQueue,
    #[cfg(novarocks_dev_controls)]
    test_observer: Option<Arc<dyn TestEventObserver>>,
    expected: Option<ExpectedReceiverSchema>,
    sender_wire_meta: HashMap<ExchangeSenderIdentity, Arc<GuardedExchangeWireMeta>>,
    sender_wire_table_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    finished: HashSet<ExchangeSenderIdentity>,
    chunks: VecDeque<SourcedExchangeChunk>,
    recv_request_count: u128,
    recv_payload_bytes: u128,
    recv_deserialize_ns: u128,
    recv_chunks: u128,
    recv_rows: u128,
    canceled: bool,
    mem_tracker: Option<Arc<MemTracker>>,
}

struct Receiver {
    unit_owner: Mutex<Option<unit_ingress::UnitReceiverOwner>>,
    mu: Mutex<ReceiverState>,
    observable: Arc<Observable>,
}

fn get_or_create(registry: &ExecutionExchangeRegistry, key: ExchangeKey) -> Arc<Receiver> {
    use tracing::debug;

    let mut guard = registry.receivers.lock().expect("exchange lock");
    let existed = guard.contains_key(&key);
    let receiver = guard
        .entry(key)
        .or_insert_with(|| {
            Arc::new(Receiver {
                unit_owner: Mutex::new(None),
                mu: Mutex::new(ReceiverState::default()),
                observable: Arc::new(Observable::new()),
            })
        })
        .clone();
    if !existed {
        debug!(
            "exchange receiver CREATED: finst={} node_id={}",
            key.finst_uuid(),
            key.node_id
        );
    }
    receiver
}

/// Remove all exchange entries for a fragment instance after normal completion.
/// Adds tombstones so late-arriving sender data is rejected, but does NOT
/// wake any blocked waiters (none should exist on the success path).
impl ExecutionExchangeRegistry {
    #[cfg(novarocks_dev_controls)]
    pub fn install_test_observer(
        &self,
        key: ExchangeKey,
        observer: Arc<dyn TestEventObserver>,
    ) -> Result<(), ExchangeFailure> {
        let receiver = self
            .receivers
            .lock()
            .map_err(|_| failure::poisoned("registry"))?
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                exchange_contract("test observer requires the exact registered receiver")
            })?;
        let mut state = receiver
            .mu
            .lock()
            .map_err(|_| failure::poisoned("receiver"))?;
        if let Some(existing) = &state.test_observer {
            if Arc::ptr_eq(existing, &observer) {
                return Ok(());
            }
            return Err(exchange_contract(
                "exchange receiver already has another test observation",
            ));
        }
        state.test_observer = Some(observer);
        Ok(())
    }

    pub fn remove_fragment(&self, finst_id_hi: i64, finst_id_lo: i64) {
        let keys: Vec<_> = self
            .receivers
            .lock()
            .expect("exchange lock")
            .keys()
            .copied()
            .filter(|key| key.finst_id_hi == finst_id_hi && key.finst_id_lo == finst_id_lo)
            .collect();
        for key in keys {
            self.remove_exchange_key(key);
        }
    }

    pub fn remove_exchange_key(&self, key: ExchangeKey) {
        mark_key_canceled(self, key);
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned();
        if let Some(receiver) = receiver
            && let Err(error) = receiver.retire_units()
        {
            let owner = receiver.unit_owner();
            if let Some(owner) = owner {
                owner.report_failure(error);
            }
        }
        self.receivers.lock().expect("exchange lock").remove(&key);
    }

    pub fn cancel_fragment(&self, finst_id_hi: i64, finst_id_lo: i64) {
        let keys: Vec<_> = self
            .receivers
            .lock()
            .expect("exchange lock")
            .keys()
            .copied()
            .filter(|key| key.finst_id_hi == finst_id_hi && key.finst_id_lo == finst_id_lo)
            .collect();
        for key in keys {
            self.cancel_exchange_key(key);
        }
    }

    pub fn cancel_exchange_key(&self, key: ExchangeKey) {
        mark_key_canceled(self, key);
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned();
        if let Some(receiver) = receiver {
            if let Err(error) = receiver.retire_units() {
                let owner = receiver.unit_owner();
                if let Some(owner) = owner {
                    owner.report_failure(error);
                }
            }
            {
                let mut state = receiver.mu.lock().expect("exchange receiver lock");
                state.canceled = true;
            }
            receiver.observable.defer_notify().arm();
        }
        self.receivers.lock().expect("exchange lock").remove(&key);
    }

    pub fn register_expected_chunk_schema(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
        chunk_schema: ChunkSchemaRef,
        creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> Result<(), ExchangeFailure> {
        Self::register_expected_chunk_schema_inner(
            self,
            key,
            expected_senders,
            chunk_schema,
            ExchangeColumnBinding::BySlotId,
            false,
            creation_factory,
        )
    }

    /// Register a legacy receiver exactly once: wire columns bind by slot id
    /// ([`ExchangeColumnBinding::BySlotId`]).
    pub fn try_register_expected_chunk_schema(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
        chunk_schema: ChunkSchemaRef,
        creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> Result<(), ExchangeFailure> {
        self.try_register_expected_chunk_schema_with_binding(
            key,
            expected_senders,
            chunk_schema,
            ExchangeColumnBinding::BySlotId,
            creation_factory,
        )
    }

    /// Register a receiver exactly once with an explicit column binding.
    pub fn try_register_expected_chunk_schema_with_binding(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
        chunk_schema: ChunkSchemaRef,
        column_binding: ExchangeColumnBinding,
        creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> Result<(), ExchangeFailure> {
        Self::register_expected_chunk_schema_inner(
            self,
            key,
            expected_senders,
            chunk_schema,
            column_binding,
            true,
            creation_factory,
        )
    }

    fn register_expected_chunk_schema_inner(
        registry: &ExecutionExchangeRegistry,
        key: ExchangeKey,
        expected_senders: usize,
        chunk_schema: ChunkSchemaRef,
        column_binding: ExchangeColumnBinding,
        reject_existing: bool,
        creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> Result<(), ExchangeFailure> {
        if !chunk_schema.is_typed() {
            return Err(exchange_contract(
                "exchange receiver requires its complete frozen execution schema",
            ));
        }
        if is_key_canceled(registry, &key) {
            return Err(exchange_stopped());
        }
        let receiver = get_or_create(registry, key);
        let mut st = receiver.mu.lock().expect("exchange receiver lock");
        if expected_senders == 0
            || (st.expected.is_some() && st.expected_senders != expected_senders)
        {
            return Err(exchange_contract(
                "exchange sender count differs from its frozen receiver declaration",
            ));
        }
        match st.expected.as_ref() {
            Some(_) if reject_existing => {
                return Err(exchange_contract(format_args!(
                    "exchange receiver already registered: finst={} node_id={}",
                    key.finst_uuid(),
                    key.node_id
                )));
            }
            Some(existing) if existing.chunk_schema.as_ref() != chunk_schema.as_ref() => {
                return Err(exchange_contract(format_args!(
                    "exchange expected chunk schema mismatch: finst={} node_id={}",
                    key.finst_uuid(),
                    key.node_id
                )));
            }
            Some(existing) if existing.column_binding != column_binding => {
                return Err(exchange_contract(format_args!(
                    "exchange column binding mismatch: finst={} node_id={} registered={:?} requested={:?}",
                    key.finst_uuid(),
                    key.node_id,
                    existing.column_binding,
                    column_binding
                )));
            }
            Some(_) => {}
            None => {
                if !st.chunks.is_empty() {
                    return Err(exchange_contract(
                        "exchange chunks arrived before their frozen receiver was registered",
                    ));
                }
                st.expected = Some(ExpectedReceiverSchema {
                    chunk_schema,
                    column_binding,
                    creation_factory,
                });
            }
        }
        st.expected_senders = expected_senders;
        Ok(())
    }

    pub fn ensure_receiver_mem_tracker(
        &self,
        key: ExchangeKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, ExchangeFailure> {
        if is_key_canceled(self, &key) {
            return Err(exchange_stopped());
        }
        let receiver = get_or_create(self, key);
        let mut st = receiver.mu.lock().expect("exchange receiver lock");
        if let Some(existing) = st.mem_tracker.as_ref() {
            return Ok(Arc::clone(existing));
        }
        let label = format!(
            "exchange receiver queue: finst={} node_id={}",
            key.finst_uuid(),
            key.node_id
        );
        let tracker = MemTracker::new_child(label, root);
        for chunk in st.chunks.iter_mut() {
            chunk.transfer_to(&tracker);
        }
        st.mem_tracker = Some(Arc::clone(&tracker));
        Ok(tracker)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ExchangeRecvStats {
    pub request_received: u128,
    pub bytes_received: u128,
    pub deserialize_ns: u128,
    pub chunks_received: u128,
    pub rows_received: u128,
}

pub enum SourcedExchangePopResult {
    Chunk(SourcedExchangeChunk),
    Finished(ExchangeRecvStats),
}

pub enum ExchangePopResult {
    Chunk(Chunk),
    Finished(ExchangeRecvStats),
}

pub struct ExchangeReceiverHandle {
    key: ExchangeKey,
    receiver: Arc<Receiver>,
}

impl ExchangeReceiverHandle {
    pub fn try_pop_next_with_stats(
        &self,
        expected_senders: usize,
    ) -> Result<Option<ExchangePopResult>, ExchangeFailure> {
        Ok(self
            .try_pop_sourced_with_stats(expected_senders)?
            .map(|item| match item {
                SourcedExchangePopResult::Chunk(item) => ExchangePopResult::Chunk(item.chunk),
                SourcedExchangePopResult::Finished(stats) => ExchangePopResult::Finished(stats),
            }))
    }

    pub fn try_pop_sourced_with_stats(
        &self,
        expected_senders: usize,
    ) -> Result<Option<SourcedExchangePopResult>, ExchangeFailure> {
        let owner = self.receiver.unit_owner().ok_or_else(|| {
            if self
                .receiver
                .mu
                .lock()
                .expect("exchange receiver lock")
                .canceled
            {
                exchange_stopped()
            } else {
                exchange_contract("exchange source has no installed Unit owner")
            }
        })?;
        self.try_pop_unit(&owner, expected_senders)
    }

    /// Called by the input owner only after its real source has exhausted
    /// data and End. ACK, Task outcome and readiness peeks do not call this.
    pub fn complete_input_consumption(
        &self,
        _expected_senders: usize,
    ) -> Result<(), ExchangeFailure> {
        let owner = self
            .receiver
            .unit_owner()
            .ok_or_else(|| exchange_contract("exchange consumption has no installed Unit owner"))?;
        self.complete_unit_input(&owner)
    }

    pub fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.receiver.observable)
    }

    pub fn has_output_or_finished(&self, expected_senders: usize) -> bool {
        // Missing construction authority is an immediately runnable error,
        // never an empty receiver on which a driver can wait indefinitely.
        if self.receiver.unit_owner().is_none() {
            return true;
        }
        let r = &self.receiver;
        let st = r.mu.lock().expect("exchange receiver lock");
        let canceled = st.canceled;
        let has_chunks = !st.chunks.is_empty() || st.units.has_pending_transfers();
        let expected = st.expected_senders.max(expected_senders);
        let finished = st.finished.len();
        let finished_ready = expected == 0 || finished >= expected;
        let res = canceled || has_chunks || finished_ready;
        if res && should_log_exchange_ready() {
            let queued_rows: usize = st.chunks.iter().map(|c| c.len()).sum();
            debug!(
                "exchange ready: finst={} node_id={} expected_param={} expected_senders={} finished_senders={} queued_chunks={} queued_rows={} canceled={} ready_chunks={} ready_finished={}",
                self.key.finst_uuid(),
                self.key.node_id,
                expected_senders,
                expected,
                finished,
                st.chunks.len(),
                queued_rows,
                canceled,
                has_chunks,
                finished_ready
            );
        } else if !res && should_log_exchange_not_ready() {
            let queued_rows: usize = st.chunks.iter().map(|c| c.len()).sum();
            debug!(
                "exchange not ready: finst={} node_id={} expected_senders={} finished_senders={} queued_chunks={} queued_rows={} canceled={}",
                self.key.finst_uuid(),
                self.key.node_id,
                expected,
                finished,
                st.chunks.len(),
                queued_rows,
                canceled
            );
        }
        res
    }
}

impl ExecutionExchangeRegistry {
    pub fn get_receiver_handle(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, ExchangeFailure> {
        if is_key_canceled(self, &key) {
            return Err(exchange_stopped());
        }
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("exchange receiver is not registered"))?;
        {
            let state = receiver.mu.lock().expect("exchange receiver lock");
            if state.canceled {
                return Err(exchange_stopped());
            }
            if expected_senders == 0
                || state.expected.is_none()
                || state.expected_senders != expected_senders
            {
                return Err(exchange_contract(
                    "exchange handle differs from its frozen receiver declaration",
                ));
            }
        }
        Ok(ExchangeReceiverHandle { key, receiver })
    }
}

#[derive(Clone, Debug)]
pub struct ExchangeReceiverSnapshot {
    pub expected_senders: usize,
    pub finished_senders: usize,
    pub queued_chunks: usize,
    pub queued_rows: usize,
    pub canceled: bool,
}

impl ExecutionExchangeRegistry {
    pub fn snapshot_receiver_state(&self, key: ExchangeKey) -> Option<ExchangeReceiverSnapshot> {
        let receiver = {
            let guard = self.receivers.lock().expect("exchange lock");
            guard.get(&key).cloned()
        }?;
        let st = receiver.mu.lock().expect("exchange receiver lock");
        let queued_rows = st.chunks.iter().map(|c| c.len()).sum::<usize>();
        Some(ExchangeReceiverSnapshot {
            expected_senders: st.expected_senders,
            finished_senders: st.finished.len(),
            queued_chunks: st.chunks.len(),
            queued_rows,
            canceled: st.canceled,
        })
    }
}

fn encode_exchange_payload_envelope(
    arrow_payload: &[u8],
    wire_meta: Option<&ExchangeWireMeta>,
) -> Result<Vec<u8>, ExchangeFailure> {
    if arrow_payload.is_empty() {
        return Ok(Vec::new());
    }
    let slots = wire_meta
        .map(|meta| meta.slot_ids_by_index.len())
        .unwrap_or(0);
    let slot_count = failure::envelope_slot_count(slots)?;
    let required = EXCHANGE_PAYLOAD_MAGIC.len() as u128
        + 2
        + 4
        + slots as u128 * std::mem::size_of::<u32>() as u128
        + arrow_payload.len() as u128;
    let capacity =
        failure::payload_storage(required).map_err(failure::PayloadWriterFailure::into_exchange)?;
    let mut out = Vec::new();
    out.try_reserve_exact(capacity)
        .map_err(failure::allocation)?;
    out.extend_from_slice(EXCHANGE_PAYLOAD_MAGIC);
    out.push(EXCHANGE_PAYLOAD_VERSION);
    out.push(if wire_meta.is_some() {
        EXCHANGE_PAYLOAD_FLAG_SLOT_IDS
    } else {
        0
    });
    out.extend_from_slice(&slot_count.to_le_bytes());
    if let Some(meta) = wire_meta {
        for slot_id in &meta.slot_ids_by_index {
            out.extend_from_slice(&slot_id.as_u32().to_le_bytes());
        }
    }
    out.extend_from_slice(arrow_payload);
    Ok(out)
}

/// One exchange payload encoded into a single bounded backing allocation.
///
/// `retained_bytes` is the capacity of the allocation transferred into
/// `Bytes`, rather than only its visible length. A result owner can therefore
/// retain the exact credit that still backs the payload after encoding.
#[derive(Debug)]
pub struct BoundedExchangePayload {
    bytes: Bytes,
    retained_bytes: usize,
}

impl BoundedExchangePayload {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn into_parts(self) -> (Bytes, usize) {
        (self.bytes, self.retained_bytes)
    }
}

struct BoundedPayloadWriter {
    buffer: Vec<u8>,
    limit: usize,
}

impl BoundedPayloadWriter {
    fn try_new(limit: usize, initial_capacity: usize) -> Result<Self, ExchangeFailure> {
        if initial_capacity > limit {
            return Err(failure::PayloadWriterFailure::Packet {
                required: initial_capacity as u128,
                limit,
            }
            .into_exchange());
        }
        failure::payload_storage(initial_capacity as u128)
            .map_err(failure::PayloadWriterFailure::into_exchange)?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(initial_capacity)
            .map_err(failure::allocation)?;
        if buffer.capacity() > limit {
            return Err(failure::PayloadWriterFailure::Packet {
                required: buffer.capacity() as u128,
                limit,
            }
            .into_exchange());
        }
        Ok(Self { buffer, limit })
    }

    fn into_payload(self) -> BoundedExchangePayload {
        let retained_bytes = self.buffer.capacity();
        BoundedExchangePayload {
            bytes: Bytes::from(self.buffer),
            retained_bytes,
        }
    }
}

impl Write for BoundedPayloadWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let required = self.buffer.len() as u128 + bytes.len() as u128;
        if required > self.limit as u128 {
            return Err(std::io::Error::other(
                failure::PayloadWriterFailure::Packet {
                    required,
                    limit: self.limit,
                },
            ));
        }
        let required = failure::payload_storage(required).map_err(std::io::Error::other)?;
        if required > self.buffer.capacity() {
            self.buffer
                .try_reserve_exact(required - self.buffer.len())
                .map_err(failure::PayloadWriterFailure::allocation)
                .map_err(std::io::Error::other)?;
            if self.buffer.capacity() > self.limit {
                return Err(std::io::Error::other(
                    failure::PayloadWriterFailure::Packet {
                        required: self.buffer.capacity() as u128,
                        limit: self.limit,
                    },
                ));
            }
        }
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct BorrowedExchangeWireMeta<'a> {
    words: &'a [u8],
}
impl<'a> BorrowedExchangeWireMeta<'a> {
    fn len(self) -> usize {
        self.words.len() / std::mem::size_of::<u32>()
    }
    fn ids(self) -> impl Iterator<Item = SlotId> + 'a {
        self.words
            .as_chunks::<4>()
            .0
            .iter()
            .map(|word| SlotId::new(u32::from_le_bytes(*word)))
    }
    fn matches(
        self,
        original: &ExchangeWireMeta,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    ) -> Result<bool, ExchangeFailure> {
        if self.len() != original.slot_ids_by_index.len() {
            return Ok(false);
        }
        for (actual, expected) in self.ids().zip(&original.slot_ids_by_index) {
            factory.observe_input_work(1)?;
            if actual != *expected {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
struct BorrowedExchangePayload<'a> {
    wire_meta: Option<BorrowedExchangeWireMeta<'a>>,
    arrow_payload: &'a [u8],
}
struct GuardedExchangeWireMeta {
    value: ExchangeWireMeta,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl GuardedExchangeWireMeta {
    fn try_from_source(
        source: BorrowedExchangeWireMeta<'_>,
        owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> Result<Arc<Self>, ExchangeFailure> {
        owner.request(
            std::alloc::Layout::array::<SlotId>(source.len()).map_err(|_| {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            })?,
        )?;
        let mut ids = Vec::with_capacity(source.len());
        for id in source.ids() {
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
            ids.push(id);
        }
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<Self>(),
            )?,
        )?;
        Ok(Arc::new(Self {
            value: ExchangeWireMeta {
                slot_ids_by_index: ids,
            },
            _storage: owner.storage_lifetime(),
        }))
    }
}
impl From<novarocks_type_contract::owned_resources::hashmap::HashMapResourceError>
    for ExchangeFailure
{
    fn from(
        error: novarocks_type_contract::owned_resources::hashmap::HashMapResourceError,
    ) -> Self {
        use novarocks_type_contract::owned_resources::hashmap::HashMapResourceError;
        match error {
            HashMapResourceError::Arithmetic(_) => {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
                    .into()
            }
            HashMapResourceError::SourceModel(detail) => Self::Execution(
                crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(detail.into())
                    .into(),
            ),
        }
    }
}

fn borrow_exchange_payload_envelope(
    bytes: &[u8],
) -> Result<BorrowedExchangePayload<'_>, ExchangeFailure> {
    if bytes.is_empty() {
        return Ok(BorrowedExchangePayload {
            wire_meta: None,
            arrow_payload: bytes,
        });
    }
    if bytes.len() < EXCHANGE_PAYLOAD_MAGIC.len()
        || &bytes[..EXCHANGE_PAYLOAD_MAGIC.len()] != EXCHANGE_PAYLOAD_MAGIC
    {
        return Ok(BorrowedExchangePayload {
            wire_meta: None,
            arrow_payload: bytes,
        });
    }

    let mut cursor = Cursor::new(&bytes[EXCHANGE_PAYLOAD_MAGIC.len()..]);
    let mut version = [0u8; 1];
    cursor
        .read_exact(&mut version)
        .map_err(|source| failure::read("read payload version", source))?;
    if version[0] != EXCHANGE_PAYLOAD_VERSION {
        return Err(exchange_payload(format_args!(
            "unsupported exchange payload version: expected={} actual={}",
            EXCHANGE_PAYLOAD_VERSION, version[0]
        )));
    }

    let mut flags = [0u8; 1];
    cursor
        .read_exact(&mut flags)
        .map_err(|source| failure::read("read payload flags", source))?;

    let mut slot_count_bytes = [0u8; 4];
    cursor
        .read_exact(&mut slot_count_bytes)
        .map_err(|source| failure::read("read payload slot count", source))?;
    let slot_count = u32::from_le_bytes(slot_count_bytes) as usize;

    let wire_meta = if flags[0] & EXCHANGE_PAYLOAD_FLAG_SLOT_IDS != 0 {
        let words_start = EXCHANGE_PAYLOAD_MAGIC.len() + cursor.position() as usize;
        let words_len = slot_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| exchange_payload("exchange slot word extent overflow"))?;
        let available = bytes.len().saturating_sub(words_start);
        if words_len > available {
            let idx = available / std::mem::size_of::<u32>();
            return Err(exchange_payload(format_args!(
                "failed to read exchange payload slot id at index {idx}: failed to fill whole buffer"
            )));
        }
        cursor.set_position(cursor.position() + words_len as u64);
        Some(BorrowedExchangeWireMeta {
            words: &bytes[words_start..words_start + words_len],
        })
    } else {
        if slot_count != 0 {
            return Err(exchange_payload(format_args!(
                "exchange payload slot count must be zero without slot-id flag, got {}",
                slot_count
            )));
        }
        None
    };
    let payload_offset = EXCHANGE_PAYLOAD_MAGIC.len() + cursor.position() as usize;
    Ok(BorrowedExchangePayload {
        wire_meta,
        arrow_payload: &bytes[payload_offset..],
    })
}

#[cfg(test)]
fn decode_exchange_payload_envelope(
    bytes: &[u8],
) -> Result<DecodedExchangePayload<'_>, ExchangeFailure> {
    let borrowed = borrow_exchange_payload_envelope(bytes)?;
    Ok(DecodedExchangePayload {
        wire_meta: borrowed.wire_meta.map(|source| ExchangeWireMeta {
            slot_ids_by_index: source.ids().collect(),
        }),
        arrow_payload: borrowed.arrow_payload,
    })
}

fn exchange_wire_schema_from_first_chunk(chunks: &[Chunk]) -> Result<SchemaRef, ExchangeFailure> {
    let first = chunks
        .first()
        .ok_or_else(|| exchange_contract("exchange chunks must not be empty"))?;
    Ok(first.chunk_schema().arrow_schema_ref())
}

fn normalize_exchange_array_for_field(
    array: &ArrayRef,
    field: &arrow::datatypes::Field,
) -> Result<ArrayRef, ExchangeFailure> {
    check_exchange_data_type(field.data_type(), array.data_type(), field.name())?;
    retag_column(array, field.data_type()).map_err(Into::into)
}

fn check_exchange_data_type(
    expected: &arrow::datatypes::DataType,
    actual: &arrow::datatypes::DataType,
    _root_label: &str,
) -> Result<(), ExchangeFailure> {
    check_exact(expected, actual).map_err(Into::into)
}

fn normalize_exchange_batch_for_schema(
    chunk: &Chunk,
    schema: &SchemaRef,
    chunk_index: usize,
) -> Result<RecordBatch, ExchangeFailure> {
    if chunk.record_batch().num_columns() != schema.fields().len() {
        return Err(exchange_contract(format_args!(
            "exchange chunk column count mismatch at index {}: batch_columns={} schema_fields={}",
            chunk_index,
            chunk.record_batch().num_columns(),
            schema.fields().len()
        )));
    }
    let columns = chunk
        .record_batch()
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(array, field)| normalize_exchange_array_for_field(array, field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let fields = schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(field, column)| {
            Arc::new(
                arrow::datatypes::Field::new(
                    field.name(),
                    column.data_type().clone(),
                    field.is_nullable() || column.null_count() > 0,
                )
                .with_metadata(field.metadata().clone()),
            )
        })
        .collect::<Vec<_>>();
    let runtime_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(runtime_schema, columns)
        .map_err(|source| failure::arrow(failure::ArrowOperation::Construct, source))
}

fn validate_exchange_dictionary_batch(batch: &RecordBatch) -> Result<(), ExchangeFailure> {
    for (idx, (field, column)) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .enumerate()
    {
        let field_type = field.data_type();
        let column_type = column.data_type();
        if !matches!(field_type, DataType::Dictionary(_, _))
            && !matches!(column_type, DataType::Dictionary(_, _))
        {
            continue;
        }
        if field_type != column_type {
            return Err(exchange_contract(format_args!(
                "exchange dictionary schema mismatch at column {idx} field {}: field_type={field_type:?} column_type={column_type:?}",
                field.name()
            )));
        }
        novarocks_value_arrays::carrier::validate_engine_carrier_observed(field_type, &mut |_| {
            Ok::<_, std::convert::Infallible>(())
        })
        .map_err(|failure| {
            ExchangeFailure::Source(crate::runtime::fragment::SharedDomainFailure::new(
                failure.into_error(),
            ))
        })?;
    }
    Ok(())
}

fn encode_arrow_ipc_chunks(chunks: &[Chunk]) -> Result<Vec<u8>, ExchangeFailure> {
    let mut buffer = Vec::new();
    encode_arrow_ipc_chunks_to(chunks, &mut buffer)?;
    Ok(buffer)
}

fn encode_arrow_ipc_chunks_to<W: std::io::Write>(
    chunks: &[Chunk],
    output: &mut W,
) -> Result<(), ExchangeFailure> {
    if chunks.is_empty() {
        return Ok(());
    }

    let schema = exchange_wire_schema_from_first_chunk(chunks)?;
    let mut batches = Vec::with_capacity(chunks.len());
    let writer_schema = if schema.fields().is_empty() {
        for chunk in chunks {
            batches.push(build_zero_column_marker_batch(chunk.len())?);
        }
        batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()))
    } else {
        for (i, chunk) in chunks.iter().enumerate() {
            batches.push(normalize_exchange_batch_for_schema(chunk, &schema, i)?);
        }
        batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or(schema)
    };
    let mut writer = StreamWriter::try_new(output, &writer_schema)
        .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;

    for batch in batches {
        validate_exchange_dictionary_batch(&batch)?;
        writer
            .write(&batch)
            .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;
    }

    writer
        .finish()
        .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;

    Ok(())
}

#[cfg(test)]
fn decode_arrow_ipc_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, ExchangeFailure> {
    if bytes.is_empty() {
        return Ok(vec![]);
    }

    let mut cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(&mut cursor, None)
        .map_err(|source| failure::arrow(failure::ArrowOperation::Decode, source))?;

    let mut batches = Vec::new();
    let mut expected_schema: Option<arrow::datatypes::SchemaRef> = None;
    for batch_result in reader {
        let batch = batch_result
            .map_err(|source| failure::arrow(failure::ArrowOperation::Decode, source))?;
        if let Some(s) = expected_schema.as_ref() {
            if batch.schema().as_ref() != s.as_ref() {
                return Err(exchange_payload(format_args!(
                    "exchange decode schema mismatch: expected={:?} actual={:?}",
                    s,
                    batch.schema()
                )));
            }
        } else {
            expected_schema = Some(batch.schema());
        }
        batches.push(batch);
    }

    Ok(batches)
}

fn build_zero_column_marker_batch(row_count: usize) -> Result<RecordBatch, ExchangeFailure> {
    let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
        EXCHANGE_ZERO_COLUMN_MARKER_FIELD,
        arrow::datatypes::DataType::Int8,
        false,
    )]));
    let values = Arc::new(Int8Array::from(vec![0_i8; row_count])) as ArrayRef;
    RecordBatch::try_new(schema, vec![values])
        .map_err(|source| failure::arrow(failure::ArrowOperation::Construct, source))
}

#[cfg(test)]
fn restore_zero_column_batch_if_needed(
    batch: RecordBatch,
    wire_meta: &ExchangeWireMeta,
) -> Result<RecordBatch, ExchangeFailure> {
    if !wire_meta.slot_ids_by_index.is_empty() {
        return Ok(batch);
    }
    if batch.num_columns() != 1
        || batch.schema().field(0).name() != EXCHANGE_ZERO_COLUMN_MARKER_FIELD
    {
        return Ok(batch);
    }
    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
        .map_err(|source| failure::arrow(failure::ArrowOperation::Construct, source))
}

#[cfg(test)]
fn chunk_from_exchange_batch(
    batch: RecordBatch,
    chunk_schema: ChunkSchemaRef,
) -> Result<Chunk, ExchangeFailure> {
    Chunk::try_new_with_chunk_schema(batch, chunk_schema).map_err(Into::into)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DecodeCarrierContract {
    Exchange,
    #[cfg(test)]
    RootResult,
}

enum ExchangeHeaderCreation<'a> {
    #[cfg(test)]
    Runtime(&'a dyn crate::exec::pipeline::operator::InputConstructionOwner),
    RuntimeWithSource {
        owner: &'a dyn crate::exec::pipeline::operator::InputConstructionOwner,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    },
    #[cfg(test)]
    PureInspection,
}
impl ExchangeHeaderCreation<'_> {
    fn storage(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        match self {
            #[cfg(test)]
            Self::Runtime(owner) => owner.storage_lifetime(),
            Self::RuntimeWithSource { storage, .. } => storage.clone(),
            #[cfg(test)]
            Self::PureInspection => novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        }
    }
    fn request(&self, layout: std::alloc::Layout) -> Result<(), ExchangeFailure> {
        match self {
            #[cfg(test)]
            Self::Runtime(owner) => owner.request(layout).map_err(Into::into),
            Self::RuntimeWithSource { owner, .. } => owner.request(layout).map_err(Into::into),
            #[cfg(test)]
            Self::PureInspection => {
                let _ = layout;
                Ok(())
            }
        }
    }
    fn observe(
        &self,
        ordinal: usize,
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ExchangeFailure> {
        match self {
            #[cfg(test)]
            Self::Runtime(owner) => owner.observe(ordinal, &[], step).map_err(Into::into),
            Self::RuntimeWithSource { owner, .. } => {
                owner.observe(ordinal, &[], step).map_err(Into::into)
            }
            #[cfg(test)]
            Self::PureInspection => {
                let _ = (ordinal, step);
                Ok(())
            }
        }
    }
    fn work(&self, units: usize) -> Result<(), ExchangeFailure> {
        self.observe(
            0,
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
        )
    }
}

/// The authenticated edge owns the complete declaration. Arrow fields supply
/// format/encoding facts only; no domain marker can import execution semantics.
/// The unchanged Arrow reader's payload/source allocation remains separately
/// governed; this body covers its newly derived schema and Chunk headers.
fn materialize_chunk_for_wire_meta(
    expected_chunk_schema: &ChunkSchemaRef,
    batch: RecordBatch,
    wire_meta: &ExchangeWireMeta,
    prefer_wire_slot_ids: bool,
    carrier_contract: DecodeCarrierContract,
    creation: &ExchangeHeaderCreation<'_>,
) -> Result<Chunk, ExchangeFailure> {
    if !expected_chunk_schema.is_typed() {
        return Err(exchange_contract(
            "exchange requires its complete frozen execution schema",
        ));
    }
    let count = expected_chunk_schema.slots().len();
    if count != wire_meta.slot_ids_by_index.len() {
        return Err(exchange_contract(
            "exchange frozen declaration differs from wire column count",
        ));
    }
    let rows = batch.num_rows();
    // Count-only keeps the existing marker's row count without manufacturing
    // another unowned empty Arrow schema. No logical column is imported.
    if count == 0 {
        if batch.num_columns() != 0
            && !(batch.num_columns() == 1
                && batch.schema().field(0).name() == EXCHANGE_ZERO_COLUMN_MARKER_FIELD)
        {
            return Err(exchange_contract("exchange count-only wire marker differs"));
        }
        return Chunk::try_new_typed_with_columns_owned_observed(
            expected_chunk_schema.clone(),
            Vec::new(),
            rows,
            creation.storage(),
            |step| creation.observe(0, step),
        );
    }
    if count != batch.num_columns() {
        return Err(exchange_contract("exchange decoded column count differs"));
    }
    let wire_ids_match = carrier_contract == DecodeCarrierContract::Exchange
        && prefer_wire_slot_ids
        && wire_meta
            .slot_ids_by_index
            .iter()
            .all(|id| expected_chunk_schema.slot(*id).is_some());
    creation.request(
        std::alloc::Layout::array::<crate::exec::chunk::ChunkSlotSchema>(count).map_err(|_| {
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
        })?,
    )?;
    let mut slots = Vec::with_capacity(count);
    creation.request(std::alloc::Layout::array::<ArrayRef>(count).map_err(|_| {
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
    })?)?;
    let mut columns = Vec::with_capacity(count);
    let source_schema = batch.schema();
    let mut reuses_prepared = true;
    for (ordinal, source) in batch.columns().iter().enumerate() {
        let expected_slot = if wire_ids_match {
            expected_chunk_schema
                .slot(wire_meta.slot_ids_by_index[ordinal])
                .expect("borrowed membership checked")
        } else {
            &expected_chunk_schema.slots()[ordinal]
        };
        let mut has_dictionary_occurrence = false;
        crate::exec::chunk::validate_chunk_carrier_encoding_observed(
            source.data_type(),
            &mut |carrier| {
                has_dictionary_occurrence |= matches!(carrier, DataType::Dictionary(..));
                creation.observe(
                    ordinal,
                    crate::exec::chunk::ArrayNormalizationStep::InspectType(carrier),
                )
            },
        )?;
        if !novarocks_type_contract::value_layout_matches_carrier_observed(
            expected_slot.typed_binding().layout(),
            source.data_type(),
            |units| creation.work(units),
        )? {
            return Err(exchange_contract(format_args!(
                "exchange column {ordinal} differs from its frozen value layout: expected={:?} actual={:?}",
                expected_slot.typed_binding().layout().data_type(),
                source.data_type()
            )));
        }
        let array = crate::exec::chunk::normalize_execution_array_observed(source, |step| {
            creation.observe(ordinal, step)
        })?;
        let field = if !has_dictionary_occurrence
            && array.data_type() == expected_slot.field().data_type()
        {
            Arc::clone(expected_slot.field_ref())
        } else {
            // A real IPC occurrence publishes its own dictionary descriptor.
            // It never borrows an ID/order fact from the receiver's value type.
            let original = source_schema.field(ordinal);
            creation.observe(
                ordinal,
                crate::exec::chunk::ArrayNormalizationStep::RebuildField {
                    source: original,
                    target: array.data_type(),
                    name: expected_slot.name(),
                },
            )?;
            let field = if matches!(array.data_type(), DataType::Dictionary(..)) {
                #[allow(deprecated)]
                arrow::datatypes::Field::new_dict(
                    expected_slot.name(),
                    array.data_type().clone(),
                    expected_slot.nullable(),
                    original.dict_id().ok_or_else(|| {
                        exchange_contract("exchange dictionary occurrence ID is missing")
                    })?,
                    original.dict_is_ordered().ok_or_else(|| {
                        exchange_contract("exchange dictionary occurrence order is missing")
                    })?,
                )
            } else {
                arrow::datatypes::Field::new(
                    expected_slot.name(),
                    array.data_type().clone(),
                    expected_slot.nullable(),
                )
            };
            reuses_prepared = false;
            Arc::new(field)
        };
        if expected_slot.slot_id() != expected_chunk_schema.slots()[ordinal].slot_id() {
            reuses_prepared = false;
        }
        slots.push(crate::exec::chunk::ChunkSlotSchema::try_new_typed_observed(
            expected_slot.slot_id(),
            field,
            expected_slot.typed_binding().clone(),
            expected_slot.unique_id(),
            creation.storage(),
            |step| creation.observe(ordinal, step),
        )?);
        columns.push(array);
    }
    let schema = if reuses_prepared {
        expected_chunk_schema.clone()
    } else {
        let schema = crate::exec::chunk::ChunkSchema::try_new_typed_observed(
            slots,
            creation.storage(),
            |layout| creation.request(layout),
            |units| creation.work(units),
        )?;
        creation.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<crate::exec::chunk::ChunkSchema>(),
            )?,
        )?;
        Arc::new(schema)
    };
    Chunk::try_new_typed_with_columns_owned_observed(
        schema,
        columns,
        rows,
        creation.storage(),
        |step| creation.observe(0, step),
    )
}

/// Existing count-only wire purpose: one Int8 marker per logical row. Its
/// unchanged primitive payload construction is distinct from these newly
/// observed public schema/array/batch headers.
fn build_zero_column_marker_batch_with_creation(
    rows: usize,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> crate::runtime::fragment::ExecutionResult<RecordBatch> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    fn checked_arc(layout: std::alloc::Layout) -> Result<std::alloc::Layout, ConstructionFailure> {
        arc_layout(layout).map_err(ConstructionFailure::from)
    }
    i64::try_from(rows)
        .map_err(|_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic))?;
    owner.request(
        std::alloc::Layout::array::<u8>(EXCHANGE_ZERO_COLUMN_MARKER_FIELD.len())
            .map_err(|_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic))?,
    )?;
    owner.request(checked_arc(std::alloc::Layout::new::<
        arrow::datatypes::Field,
    >())?)?;
    let field = Arc::new(arrow::datatypes::Field::new(
        EXCHANGE_ZERO_COLUMN_MARKER_FIELD,
        DataType::Int8,
        false,
    ));
    owner.request(
        std::alloc::Layout::array::<arrow::datatypes::FieldRef>(1)
            .map_err(|_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic))?,
    )?;
    let fields = vec![field];
    owner.request(checked_arc(
        std::alloc::Layout::array::<arrow::datatypes::FieldRef>(1)
            .map_err(|_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic))?,
    )?)?;
    let fields: arrow::datatypes::Fields = fields.into();
    owner.request(checked_arc(std::alloc::Layout::new::<Schema>())?)?;
    let schema = Arc::new(Schema::new(fields));
    build_zero_column_marker_batch_for_schema_with_creation(rows, schema, owner, control)
}

fn build_zero_column_marker_batch_for_schema_with_creation(
    rows: usize,
    schema: SchemaRef,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> crate::runtime::fragment::ExecutionResult<RecordBatch> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    );
    // Observe only the original primitive Buffer adoption structure. The
    // existing marker Vec payload remains with its original accounting.
    novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction(
        &mut creation,
        control,
    )
    .map_err(|cause| -> crate::runtime::fragment::ExecutionFailure {
        use novarocks_functions::selected_copy::CopyConstructionError;
        match cause {
            CopyConstructionError::Creator(cause) => cause.into(),
            CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
            CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
        }
    })?;
    owner.request(
        arc_layout(std::alloc::Layout::new::<Int8Array>()).map_err(ConstructionFailure::from)?,
    )?;
    let array = Arc::new(Int8Array::from(vec![0_i8; rows])) as ArrayRef;
    owner.request(
        std::alloc::Layout::array::<ArrayRef>(1)
            .map_err(|_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic))?,
    )?;
    Ok(RecordBatch::try_new(schema, vec![array])
        .map_err(|source| failure::arrow(failure::ArrowOperation::Construct, source))?)
}

/// Real BE sender schema/header preparation. Complete immutable declarations
/// remain authoritative; the writer's occurrence IDs are format facts only.
/// Existing serialized payload/backing accounting is unchanged.
fn encode_typed_arrow_ipc_chunks_to<W: std::io::Write>(
    chunks: &[Chunk],
    output: &mut W,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<(), ExchangeFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_functions::arrow_ipc_construction::{
        IpcConstructionError, IpcGeometryError, observe_stream_writer_batch_construction,
        observe_stream_writer_schema_construction,
    };
    let first = &chunks[0];
    let first_marker = if first.columns().is_empty() {
        Some(build_zero_column_marker_batch_with_creation(
            first.len(),
            owner,
            control,
        )?)
    } else {
        None
    };
    let schema = first_marker
        .as_ref()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| first.chunk_schema().arrow_schema_ref());
    for chunk in chunks {
        if !chunk.chunk_schema().is_typed() || chunk.columns().len() != first.columns().len() {
            return Err(ConstructionFailure::InvalidSource(
                "exchange sender source differs from its complete channel declaration".into(),
            )
            .into());
        }
        for (expected, actual) in first
            .chunk_schema()
            .slots()
            .iter()
            .zip(chunk.chunk_schema().slots())
        {
            if !expected
                .typed_binding()
                .borrowed()
                .exactly_equals_observed(actual.typed_binding().borrowed(), || {
                    owner.observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units: 1,
                        },
                    )
                })?
            {
                return Err(ConstructionFailure::InvalidSource(
                    "exchange sender source differs from its complete channel declaration".into(),
                )
                .into());
            }
        }
    }
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    );
    // This sole schema writer model excludes record-batch payload copying.
    // Batch-private structure/payload bounds remain a separately listed gap.
    let map_ipc = |cause: IpcConstructionError<ConstructionFailure>| -> crate::runtime::fragment::ExecutionFailure {
        match cause {
            IpcConstructionError::Creator(cause) => cause.into(),
            IpcConstructionError::Control(cause) => cause.into(),
            IpcConstructionError::Geometry(IpcGeometryError::Overflow)
                | IpcConstructionError::HashMap(novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_)) => ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic).into(),
            IpcConstructionError::Geometry(IpcGeometryError::SourceModel) => ConstructionFailure::InvalidSource("exchange IPC schema writer source model is unsupported".into()).into(),
            IpcConstructionError::Shape(cause) => ConstructionFailure::InvalidSource(format!("exchange IPC schema carrier structure is invalid: {cause:?}")).into(),
            IpcConstructionError::HashMap(novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::SourceModel(detail)) => ConstructionFailure::InvalidSource(detail.into()).into(),
        }
    };
    let schema_facts =
        observe_stream_writer_schema_construction(schema.as_ref(), &mut creation, control)
            .map_err(map_ipc)?;
    {
        let mut writer = StreamWriter::try_new(output, schema.as_ref())
            .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;
        for (index, chunk) in chunks.iter().enumerate() {
            if chunk.columns().is_empty() {
                let marker;
                let batch = if index == 0 {
                    first_marker
                        .as_ref()
                        .expect("count-only branch established")
                } else {
                    marker = build_zero_column_marker_batch_for_schema_with_creation(
                        chunk.len(),
                        schema.clone(),
                        owner,
                        control,
                    )?;
                    &marker
                };
                observe_stream_writer_batch_construction(
                    batch,
                    &schema_facts,
                    &mut creation,
                    control,
                )
                .map_err(map_ipc)?;
                writer
                    .write(batch)
                    .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;
            } else {
                // The original retag body refuses mixed Plain/Dictionary
                // representation. Keep that existing capability boundary;
                // root labels are purpose projections from the first declaration.
                for (field, array) in schema.fields().iter().zip(chunk.columns()) {
                    if field.data_type() != array.data_type() {
                        // Original retag refuses mixed Plain/Dictionary. Reject
                        // the borrowed representation before to_data could create
                        // unobserved temporary headers; no hydrate or type rewrite.
                        return Err(exchange_contract(format_args!(
                            "exchange chunk representation differs at index {index}"
                        )));
                    }
                }
                owner.request(std::alloc::Layout::array::<ArrayRef>(chunk.columns().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
                let mut columns = Vec::with_capacity(chunk.columns().len());
                columns.extend(chunk.columns().iter().cloned());
                let options =
                    arrow::array::RecordBatchOptions::new().with_row_count(Some(chunk.len()));
                let batch = RecordBatch::try_new_with_options(schema.clone(), columns, &options)
                    .map_err(|source| failure::arrow(failure::ArrowOperation::Construct, source))?;
                observe_stream_writer_batch_construction(
                    &batch,
                    &schema_facts,
                    &mut creation,
                    control,
                )
                .map_err(map_ipc)?;
                writer
                    .write(&batch)
                    .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;
            }
        }
        writer
            .finish()
            .map_err(|source| failure::arrow(failure::ArrowOperation::Encode, source))?;
    }
    Ok(())
}

pub(crate) fn encode_chunks_with_creation(
    chunks: &[Chunk],
    include_slot_ids: bool,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> crate::runtime::fragment::ExecutionResult<Vec<u8>> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    let first = &chunks[0];
    if !first.chunk_schema().is_typed() {
        return Err(ConstructionFailure::InvalidSource(
            "exchange sender requires complete value bindings".into(),
        )
        .into());
    }
    let owner = factory.begin_input_construction(first)?;
    let mut buffer = Vec::new();
    encode_typed_arrow_ipc_chunks_to(chunks, &mut buffer, owner.as_ref(), control)
        .map_err(ExchangeFailure::into_execution)?;
    // Envelope words are transient schema descriptors. Request the real header
    // before copying its immutable slot IDs, not the already-accounted payload.
    let wire_meta = if include_slot_ids {
        owner.request(std::alloc::Layout::array::<SlotId>(first.columns().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        ExchangeWireMeta::from_chunks(chunks)?
    } else {
        None
    };
    encode_exchange_payload_envelope(&buffer, wire_meta.as_ref())
        .map_err(ExchangeFailure::into_execution)
}

/// Encode a typed result directly into the original bounded payload backing.
/// Result credit covers that unchanged backing; the actual BE creator covers
/// only the new schema/writer structures, and remains independent of credit.
pub fn encode_chunks_bounded_with_creation(
    chunks: &[Chunk],
    include_slot_ids: bool,
    packet_cap: usize,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<BoundedExchangePayload, ExchangeFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let Some(first) = chunks.first() else {
        return Ok(BoundedExchangePayload {
            bytes: Bytes::new(),
            retained_bytes: 0,
        });
    };
    if !first.chunk_schema().is_typed() {
        return Err(ConstructionFailure::InvalidSource(
            "exchange sender requires complete value bindings".into(),
        )
        .into());
    }
    let owner = factory.begin_input_construction(first)?;
    let slots = first.chunk_schema().slots();
    let slot_count = if include_slot_ids { slots.len() } else { 0 };
    let encoded_slot_count = failure::envelope_slot_count(slot_count)?;
    let slot_id_bytes = slot_count.saturating_mul(std::mem::size_of::<u32>());
    let envelope_bytes = EXCHANGE_PAYLOAD_MAGIC
        .len()
        .saturating_add(2)
        .saturating_add(std::mem::size_of::<u32>())
        .saturating_add(slot_id_bytes);
    let logical_bytes = chunks.iter().fold(0usize, |total, chunk| {
        total.saturating_add(chunk.logical_bytes())
    });
    let initial_capacity = envelope_bytes
        .saturating_add(logical_bytes)
        .saturating_add(4096)
        .min(packet_cap);
    let mut output = BoundedPayloadWriter::try_new(packet_cap, initial_capacity)?;
    output
        .write_all(EXCHANGE_PAYLOAD_MAGIC)
        .map_err(|source| failure::write("write payload magic", source))?;
    output
        .write_all(&[EXCHANGE_PAYLOAD_VERSION])
        .map_err(|source| failure::write("write payload version", source))?;
    output
        .write_all(&[if include_slot_ids {
            EXCHANGE_PAYLOAD_FLAG_SLOT_IDS
        } else {
            0
        }])
        .map_err(|source| failure::write("write payload flags", source))?;
    output
        .write_all(&encoded_slot_count.to_le_bytes())
        .map_err(|source| failure::write("write payload slot count", source))?;
    if include_slot_ids {
        for slot in slots {
            output
                .write_all(&slot.slot_id().as_u32().to_le_bytes())
                .map_err(|source| failure::write("write payload slot id", source))?;
        }
    }
    encode_typed_arrow_ipc_chunks_to(chunks, &mut output, owner.as_ref(), control)?;
    Ok(output.into_payload())
}

/// Encode chunks to exchange payload format.
pub fn encode_chunks(chunks: &[Chunk], include_slot_ids: bool) -> Result<Vec<u8>, ExchangeFailure> {
    let arrow_payload = encode_arrow_ipc_chunks(chunks)?;
    let wire_meta = if include_slot_ids {
        ExchangeWireMeta::from_chunks(chunks)?
    } else {
        None
    };
    encode_exchange_payload_envelope(&arrow_payload, wire_meta.as_ref())
}

/// Encodes chunks directly into one allocation that cannot grow beyond
/// `packet_cap` through this writer.
///
/// This is used by retained result streams after the owner has reserved the
/// complete packet cap. It avoids the unbounded encoder's Arrow-payload plus
/// envelope copy and fails while writing as soon as the cap would be crossed.
#[cfg(test)]
pub fn encode_chunks_bounded(
    chunks: &[Chunk],
    include_slot_ids: bool,
    packet_cap: usize,
) -> Result<BoundedExchangePayload, ExchangeFailure> {
    if chunks.is_empty() {
        return Ok(BoundedExchangePayload {
            bytes: Bytes::new(),
            retained_bytes: 0,
        });
    }

    let wire_meta = if include_slot_ids {
        ExchangeWireMeta::from_chunks(chunks)?
    } else {
        None
    };
    let slot_id_bytes = wire_meta
        .as_ref()
        .map(|meta| {
            meta.slot_ids_by_index
                .len()
                .saturating_mul(std::mem::size_of::<u32>())
        })
        .unwrap_or(0);
    let envelope_bytes = EXCHANGE_PAYLOAD_MAGIC
        .len()
        .saturating_add(2)
        .saturating_add(std::mem::size_of::<u32>())
        .saturating_add(slot_id_bytes);
    let logical_bytes = chunks.iter().fold(0usize, |total, chunk| {
        total.saturating_add(chunk.logical_bytes())
    });
    let initial_capacity = envelope_bytes
        .saturating_add(logical_bytes)
        .saturating_add(4096)
        .min(packet_cap);
    let mut output = BoundedPayloadWriter::try_new(packet_cap, initial_capacity)?;

    output
        .write_all(EXCHANGE_PAYLOAD_MAGIC)
        .map_err(|source| failure::write("write payload magic", source))?;
    output
        .write_all(&[EXCHANGE_PAYLOAD_VERSION])
        .map_err(|source| failure::write("write payload version", source))?;
    output
        .write_all(&[if wire_meta.is_some() {
            EXCHANGE_PAYLOAD_FLAG_SLOT_IDS
        } else {
            0
        }])
        .map_err(|source| failure::write("write payload flags", source))?;
    let slot_count = failure::envelope_slot_count(
        wire_meta
            .as_ref()
            .map(|meta| meta.slot_ids_by_index.len())
            .unwrap_or(0),
    )?;
    output
        .write_all(&slot_count.to_le_bytes())
        .map_err(|source| failure::write("write payload slot count", source))?;
    if let Some(meta) = wire_meta.as_ref() {
        for slot_id in &meta.slot_ids_by_index {
            output
                .write_all(&slot_id.as_u32().to_le_bytes())
                .map_err(|source| failure::write("write payload slot id", source))?;
        }
    }
    encode_arrow_ipc_chunks_to(chunks, &mut output)?;
    Ok(output.into_payload())
}

#[cfg(test)]
fn decode_root_result_chunks(
    bytes: &[u8],
    expected_chunk_schema: &ChunkSchemaRef,
) -> Result<Vec<Chunk>, ExchangeFailure> {
    let exact_payload_limit = novarocks_execution_contract::ResultByteLimit::new(
        u64::try_from(bytes.len())
            .map_err(|source| failure::metadata("root fixture payload length", source))?,
    )
    .map_err(|source| failure::metadata("root fixture payload limit", source))?;
    preflight_typed_root_result_decode(bytes, exact_payload_limit)?;
    let DecodedExchangePayload {
        wire_meta,
        arrow_payload,
    } = decode_exchange_payload_envelope(bytes)?;
    if arrow_payload.is_empty() {
        return Ok(Vec::new());
    }
    let wire_meta = wire_meta.ok_or_else(|| {
        exchange_contract("typed root result payload missing exchange wire slot metadata")
    })?;
    let batches = decode_arrow_ipc_batches(arrow_payload)?;
    batches
        .into_iter()
        .map(|batch| {
            materialize_chunk_for_wire_meta(
                expected_chunk_schema,
                batch,
                &wire_meta,
                false,
                DecodeCarrierContract::RootResult,
                &ExchangeHeaderCreation::PureInspection,
            )
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn decode_chunks(bytes: &[u8]) -> Result<Vec<Chunk>, ExchangeFailure> {
    let DecodedExchangePayload {
        wire_meta,
        arrow_payload,
    } = decode_exchange_payload_envelope(bytes)?;
    let wire_meta = wire_meta.ok_or_else(|| exchange_payload("exchange wire meta missing"))?;
    let batches = decode_arrow_ipc_batches(arrow_payload)?;
    let mut chunks = Vec::with_capacity(batches.len());
    for batch in batches {
        let batch = restore_zero_column_batch_if_needed(batch, &wire_meta)?;
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &wire_meta.slot_ids_by_index,
        )?;
        chunks.push(chunk_from_exchange_batch(batch, chunk_schema)?);
    }
    Ok(chunks)
}

impl ExecutionExchangeRegistry {
    /// Snapshot only existing source-authored facts; an absent stream is not
    /// evidence of consumption. The binding owner supplies its frozen set.
    pub fn stream_close_facts(
        &self,
        key: ExchangeKey,
        expected: &[ExchangeStreamIdentity],
    ) -> Result<Vec<ExchangeStreamClose>, ExchangeFailure> {
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("exchange receiver is not registered"))?;
        let owner = receiver.unit_owner().ok_or_else(|| {
            exchange_contract("exchange close facts have no installed Unit owner")
        })?;
        expected
            .iter()
            .map(|stream| {
                let snapshot = owner
                    .slots
                    .stream_snapshot(*stream)
                    .map_err(ExchangeFailure::from)?;
                let disposition = match snapshot.observed_end {
                    Some(end) if snapshot.handed_off_through == end.get() => {
                        ExchangeCloseDisposition::Consumed {
                            final_sequence: end,
                        }
                    }
                    _ => ExchangeCloseDisposition::Withdrawn,
                };
                Ok(ExchangeStreamClose {
                    stream: *stream,
                    disposition,
                })
            })
            .collect()
    }

    pub fn decode_chunks_for_sender(
        &self,
        key: ExchangeKey,
        sender: ExchangeSenderIdentity,
        bytes: &[u8],
    ) -> Result<Vec<Chunk>, ExchangeFailure> {
        let BorrowedExchangePayload {
            wire_meta: source_meta,
            arrow_payload,
        } = borrow_exchange_payload_envelope(bytes)?;
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("exchange receiver is not registered"))?;
        let expected;
        let wire_meta;
        {
            let mut st = receiver.mu.lock().expect("exchange receiver lock");
            if st.canceled {
                return Err(exchange_stopped());
            }
            expected = st.expected.clone().ok_or_else(|| {
                exchange_contract("exchange receiver has no complete frozen declaration")
            })?;
            if arrow_payload.is_empty() {
                return Ok(Vec::new());
            }
            if let Some(source) = source_meta {
                if source.len() != expected.chunk_schema.slots().len() {
                    return Err(exchange_payload(
                        "exchange frozen declaration differs from wire column count",
                    ));
                }
                // Cache headers own a separate finite scope. Temporary reader
                // headers never become coupled to the cache's lifetime.
                if let Some(existing) = st.sender_wire_meta.get(&sender) {
                    if !source.matches(&existing.value, expected.creation_factory.as_ref())? {
                        return Err(exchange_payload(format_args!(
                            "exchange sender wire meta changed unexpectedly: finst={} node_id={} sender={}",
                            key.finst_uuid(),
                            key.node_id,
                            sender
                        )));
                    }
                    wire_meta = existing.clone();
                } else {
                    let metadata_owner = expected
                        .creation_factory
                        .begin_typed_construction(&expected.chunk_schema)?;
                    let metadata =
                        GuardedExchangeWireMeta::try_from_source(source, metadata_owner.as_ref())?;
                    let growth = novarocks_type_contract::owned_resources::hashmap::next_insert_allocation_upper_bound_observed::<ExchangeSenderIdentity, Arc<GuardedExchangeWireMeta>, ExchangeFailure>(
                        st.sender_wire_meta.len(), st.sender_wire_meta.capacity(),
                        || { metadata_owner.observe(0, &[], crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 })?; Ok(()) },
                    )?;
                    let table_owner = if growth != 0 {
                        let owner = expected
                            .creation_factory
                            .begin_typed_construction(&expected.chunk_schema)?;
                        owner.observe_opaque_upper_bound(growth)?;
                        Some(owner)
                    } else {
                        None
                    };
                    st.sender_wire_meta.insert(sender, metadata.clone());
                    // Insertion has completed and std dropped the old table.
                    // Replacing only its finite holder releases original table
                    // liability, not any payload or metadata-node custody.
                    if let Some(owner) = table_owner {
                        st.sender_wire_table_storage = Some(owner.storage_lifetime());
                    }
                    wire_meta = metadata;
                }
            } else {
                wire_meta = st.sender_wire_meta.get(&sender).cloned().ok_or_else(|| exchange_payload(format_args!("exchange wire meta missing before first data chunk: finst={} node_id={} sender={}", key.finst_uuid(), key.node_id, sender)))?;
            }
        }
        reader_structure::decode_owned(
            arrow_payload,
            &expected.chunk_schema,
            &wire_meta.value,
            expected.column_binding.prefers_wire_slot_ids(),
            expected.creation_factory.as_ref(),
        )
    }
}

#[cfg(test)]
fn test_registry() -> &'static ExecutionExchangeRegistry {
    static REGISTRY: OnceLock<ExecutionExchangeRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ExecutionExchangeRegistry::default)
}

#[cfg(test)]
pub fn cancel_exchange_key(key: ExchangeKey) {
    test_registry().cancel_exchange_key(key);
}

#[cfg(test)]
pub fn register_expected_chunk_schema(
    key: ExchangeKey,
    expected_senders: usize,
    chunk_schema: ChunkSchemaRef,
) -> Result<(), ExchangeFailure> {
    test_registry().register_expected_chunk_schema(
        key,
        expected_senders,
        tests::plain_declaration(&chunk_schema),
        crate::runtime::fragment::exchange::test_receiver_creation_factory(),
    )
}

#[cfg(test)]
pub fn get_receiver_handle(
    key: ExchangeKey,
    expected_senders: usize,
) -> Result<ExchangeReceiverHandle, ExchangeFailure> {
    test_registry().get_receiver_handle(key, expected_senders)
}

#[cfg(test)]
pub fn snapshot_receiver_state(key: ExchangeKey) -> Option<ExchangeReceiverSnapshot> {
    test_registry().snapshot_receiver_state(key)
}

#[cfg(test)]
pub fn decode_chunks_for_sender(
    key: ExchangeKey,
    sender_id: i32,
    backend_number: i32,
    bytes: &[u8],
) -> Result<Vec<Chunk>, ExchangeFailure> {
    test_registry().decode_chunks_for_sender(
        key,
        ExchangeSenderIdentity::local(sender_id, backend_number),
        bytes,
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod typed_receiver_header_tests {
    use super::*;
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };

    fn declaration() -> ChunkSchemaRef {
        // The fixture is a genuine pure frozen author. Its JSON declaration is
        // independent of the IPC field and of any arbitrary source attributes.
        let storage = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            false,
            storage.clone(),
        )
        .unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(&DataType::Utf8, storage.clone()).unwrap(),
        )
        .unwrap();
        let slot = crate::exec::chunk::ChunkSlotSchema::try_new_typed(
            SlotId::new(9),
            Arc::new(Field::new("payload", DataType::Utf8, false)),
            binding,
            None,
            storage.clone(),
        )
        .unwrap();
        Arc::new(
            crate::exec::chunk::ChunkSchema::try_new_typed_observed::<
                crate::runtime::fragment::ExecutionFailure,
            >(vec![slot], storage, |_| Ok(()), |_| Ok(()))
            .unwrap(),
        )
    }
    fn factory(
        tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    ) -> Arc<dyn InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap()
    }
    fn dictionary_batch() -> RecordBatch {
        // Only key 0 is selected; the unused NULL dictionary value is legal for
        // this required JSON source. The descriptor belongs to the real IPC
        // occurrence, not to JSON value identity.
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0]),
                Arc::new(StringArray::from(vec![Some("{}"), None])),
            )
            .unwrap(),
        );
        #[allow(deprecated)]
        let field = Field::new_dict("wire_payload", array.data_type().clone(), false, 271, false);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
    }
    fn key() -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: 19,
            finst_id_lo: 37,
            node_id: 7,
        }
    }

    #[test]
    fn typed_receiver_requires_frozen_registration_before_reader_and_cache() {
        let registry = ExecutionExchangeRegistry::default();
        // A valid envelope with deliberately invalid Arrow bytes cannot create
        // a schema or sender cache before an installed receiver exists.
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(33)],
        };
        let payload = encode_exchange_payload_envelope(b"not an IPC schema", Some(&wire))
            .expect("exchange envelope fixture");
        let refusal = registry
            .decode_chunks_for_sender(key(), ExchangeSenderIdentity::local(1, 1), &payload)
            .unwrap_err();
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&refusal);
        assert_eq!(
            fact.class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        assert_eq!(fact.reason().code(), 100);
        assert_eq!(refusal.to_string(), "exchange receiver is not registered");
        assert!(registry.receivers.lock().unwrap().is_empty());
    }

    #[test]
    fn typed_receiver_first_header_refusal_preserves_actual_tracker_cause() {
        let expected = declaration();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("typed receiver first header");
        let factory = factory(tracker.clone());
        let owner = factory.begin_typed_construction(&expected).unwrap();
        let baseline = tracker.current();
        assert!(baseline > 0);
        tracker.install_limit_once(baseline + 1).unwrap();
        let batch = dictionary_batch();
        let original_array = batch.column(0).clone();
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(33)],
        };
        let refusal = materialize_chunk_for_wire_meta(
            &expected,
            batch,
            &wire,
            false,
            DecodeCarrierContract::Exchange,
            &ExchangeHeaderCreation::Runtime(owner.as_ref()),
        )
        .unwrap_err();
        assert!(matches!(
            refusal,
            ExchangeFailure::Execution(error) if matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Construction(ConstructionFailure::Memory(_)))
        ));
        assert_eq!(tracker.current(), baseline);
        assert_eq!(original_array.len(), 1);
        assert_eq!(original_array.null_count(), 0);
        drop(owner);
        assert!(tracker.current() > 0, "the actual factory is still live");
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn typed_receiver_ipc_dictionary_preserves_json_and_last_schema_hold_only() {
        let expected = declaration();
        let batch = dictionary_batch();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        // StreamWriter owns the actual occurrence namespace. Compare the
        // receiver to its published descriptor, never to the source Field ID.
        let published = StreamReader::try_new(Cursor::new(&ipc), None)
            .unwrap()
            .schema();
        #[allow(deprecated)]
        let ipc_id = published.field(0).dict_id().unwrap();
        let ipc_order = published.field(0).dict_is_ordered().unwrap();
        let wire = ExchangeWireMeta {
            slot_ids_by_index: vec![SlotId::new(33)],
        };
        let payload =
            encode_exchange_payload_envelope(&ipc, Some(&wire)).expect("exchange envelope fixture");
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("typed receiver independent schema");
        tracker.install_limit_once(64 * 1024 * 1024).unwrap();
        let factory = factory(tracker.clone());
        let weak_factory = Arc::downgrade(&factory);
        let registry = ExecutionExchangeRegistry::default();
        registry
            .try_register_expected_chunk_schema_with_binding(
                key(),
                1,
                expected,
                ExchangeColumnBinding::Positional,
                factory.clone(),
            )
            .unwrap();
        let mut chunks = registry
            .decode_chunks_for_sender(key(), ExchangeSenderIdentity::local(1, 1), &payload)
            .unwrap();
        assert_eq!(chunks.len(), 1);
        let chunk = chunks.remove(0);
        let schema = chunk.chunk_schema_ref();
        assert_eq!(
            schema.slots()[0].typed_binding().value_type().domain(),
            Some(ValueLogicalType::Json)
        );
        assert!(!schema.slots()[0].nullable());
        assert!(
            matches!(chunk.columns()[0].data_type(), DataType::Dictionary(key, value) if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8)
        );
        #[allow(deprecated)]
        {
            assert_eq!(schema.slots()[0].field().dict_id(), Some(ipc_id));
            assert_eq!(schema.slots()[0].field().dict_is_ordered(), Some(ipc_order));
        }
        registry.remove_exchange_key(key());
        drop(registry);
        drop(factory);
        assert!(weak_factory.upgrade().is_none());
        drop(chunk);
        drop(chunks);
        assert!(tracker.current() > 0);
        drop(schema);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
mod bounded_root_header_tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    struct Control;
    impl novarocks_functions::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("IPC schema construction never waits");
        }
    }
    fn source(array: ArrayRef, value: ValueTypeRef, rows: usize) -> Chunk {
        let pure = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(84),
            Arc::new(arrow::datatypes::Field::new(
                "value",
                array.data_type().clone(),
                false,
            )),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
        Chunk::try_new_typed_with_columns_observed::<crate::runtime::fragment::ExecutionFailure, _>(
            schema,
            vec![array],
            rows,
            |_| Ok(()),
        )
        .unwrap()
    }
    #[test]
    fn bounded_root_sole_typed_writer_preserves_plain_nested_dictionary_and_count_only() {
        use arrow::array::{DictionaryArray, Int32Array, ListArray, StringArray};
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::Int32Type;
        let pure = ValueTypeStorageLifetime::pure();
        let plain = source(
            Arc::new(Int32Array::from(vec![7, 9])),
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, pure.clone()).unwrap(),
            2,
        );
        let dict: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 2]),
                Arc::new(StringArray::from(vec![Some("{}"), None, Some("[]")])),
            )
            .unwrap(),
        );
        #[allow(deprecated)]
        let item = Arc::new(arrow::datatypes::Field::new_dict(
            "item",
            dict.data_type().clone(),
            true,
            401,
            false,
        ));
        let nested = source(
            Arc::new(
                ListArray::try_new(
                    item,
                    OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                    dict,
                    None,
                )
                .unwrap(),
            ),
            ValueTypeRef::try_new(
                ValueKind::List(
                    ValueTypeRef::try_new(
                        ValueKind::Nominal(ValueLogicalType::Json),
                        true,
                        pure.clone(),
                    )
                    .unwrap(),
                ),
                false,
                pure.clone(),
            )
            .unwrap(),
            2,
        );
        let count = Chunk::try_new_typed_with_columns_observed::<
            crate::runtime::fragment::ExecutionFailure,
            _,
        >(
            Arc::new(ChunkSchema::try_new_typed(vec![], pure).unwrap()),
            vec![],
            3,
            |_| Ok(()),
        )
        .unwrap();
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("bounded root structures");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(), Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let baseline = tracker.current();
        for input in [&plain, &nested, &count] {
            let encoded = encode_chunks_bounded_with_creation(
                std::slice::from_ref(input),
                true,
                1024 * 1024,
                factory.as_ref(),
                &Control,
            )
            .unwrap();
            assert!(encoded.len() <= encoded.retained_bytes());
            assert!(encoded.retained_bytes() <= 1024 * 1024);
            assert_eq!(
                tracker.current(),
                baseline,
                "writer/header scratch must not escape in serialized backing"
            );
            let (bytes, _) = encoded.into_parts();
            let out = decode_root_result_chunks(&bytes, &input.chunk_schema_ref()).unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].len(), input.len());
            assert_eq!(out[0].columns().len(), input.columns().len());
            assert!(out[0].chunk_schema().is_typed());
            if !input.columns().is_empty() {
                assert!(
                    out[0]
                        .chunk_schema()
                        .slot(SlotId::new(84))
                        .unwrap()
                        .typed_binding()
                        .borrowed()
                        .exactly_equals_observed(
                            input
                                .chunk_schema()
                                .slot(SlotId::new(84))
                                .unwrap()
                                .typed_binding()
                                .borrowed(),
                            || Ok::<_, String>(())
                        )
                        .unwrap()
                );
                assert_eq!(out[0].columns()[0].to_data(), input.columns()[0].to_data());
            }
        }
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
