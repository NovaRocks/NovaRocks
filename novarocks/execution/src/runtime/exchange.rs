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
use std::sync::{Arc, Condvar, Mutex};
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
    ExchangeCloseDisposition, ExchangeFrameSequence, ExchangeSource, ExchangeStreamClose,
    ExchangeStreamIdentity,
};
use novarocks_types::format_uuid;
use novarocks_types::{SlotId, UniqueId};
use tracing::debug;

mod reader_structure;
mod result_ipc_preflight;
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
const EXCHANGE_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(5);
const EXCHANGE_LOCK_WAIT_WARN: Duration = Duration::from_millis(200);
const EXCHANGE_LOCK_HOLD_WARN: Duration = Duration::from_millis(200);
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
#[derive(Default)]
pub struct ExecutionExchangeRegistry {
    canceled_keys: Mutex<HashMap<ExchangeKey, Instant>>,
    receivers: Mutex<HashMap<ExchangeKey, Arc<Receiver>>>,
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
    fn local(chunk: Chunk) -> Self {
        Self {
            source: None,
            chunk,
        }
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
struct ReceivedExchangeStream {
    admission: Arc<Mutex<()>>,
    _admission_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    identity: ExchangeStreamIdentity,
    next: i64,
    end: Option<ExchangeFrameSequence>,
    consumed: bool,
}

#[derive(Default)]
struct ReceiverState {
    expected_senders: usize,
    #[cfg(novarocks_dev_controls)]
    test_observer: Option<Arc<dyn TestEventObserver>>,
    expected: Option<ExpectedReceiverSchema>,
    sender_wire_meta: HashMap<ExchangeSenderIdentity, Arc<GuardedExchangeWireMeta>>,
    sender_wire_table_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    finished: HashSet<ExchangeSenderIdentity>,
    chunks: VecDeque<SourcedExchangeChunk>,
    streams: HashMap<ExchangeSenderIdentity, ReceivedExchangeStream>,
    stream_table_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    recv_request_count: u128,
    recv_payload_bytes: u128,
    recv_deserialize_ns: u128,
    recv_chunks: u128,
    recv_rows: u128,
    canceled: bool,
    mem_tracker: Option<Arc<MemTracker>>,
}

struct Receiver {
    mu: Mutex<ReceiverState>,
    cv: Condvar,
    observable: Arc<Observable>,
}

/// Read-only loan of the exact receiver captured during registration. Removal
/// from the routing map never erases its original consumed/End observations.
/// This grants no ingress, normal-close permission or new queue capacity.
pub struct ExchangeReceiverCloseObserver {
    key: ExchangeKey,
    receiver: Arc<Receiver>,
}
impl ExchangeReceiverCloseObserver {
    pub fn key(&self) -> ExchangeKey {
        self.key
    }
    pub fn stream_close_facts(
        &self,
        expected: &[ExchangeStreamIdentity],
    ) -> Result<Vec<ExchangeStreamClose>, ExchangeFailure> {
        let st = self
            .receiver
            .mu
            .lock()
            .map_err(|_| failure::poisoned("receiver close observation"))?;
        // Cancellation closes production; it does not manufacture consumption
        // or prevent this original registration from reading existing facts.
        expected
            .iter()
            .map(|stream| {
                if stream.destination_fragment_instance_id()
                    != UniqueId::new(self.key.finst_id_hi, self.key.finst_id_lo)
                    || stream.destination_node().get() != self.key.node_id
                {
                    return Err(exchange_contract(
                        "close snapshot names another exchange input",
                    ));
                }
                let sender = ExchangeSenderIdentity::native(
                    stream.source().fragment_instance_id(),
                    stream.source().sender_ordinal(),
                );
                let disposition = match st.streams.get(&sender) {
                    Some(actual) if actual.identity != *stream => {
                        return Err(exchange_contract(
                            "close snapshot source differs from admitted stream",
                        ));
                    }
                    Some(actual) if actual.consumed => ExchangeCloseDisposition::Consumed {
                        final_sequence: actual
                            .end
                            .ok_or_else(|| exchange_contract("consumed stream has no End"))?,
                    },
                    _ => ExchangeCloseDisposition::Withdrawn,
                };
                Ok(ExchangeStreamClose {
                    stream: *stream,
                    disposition,
                })
            })
            .collect()
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn install_test_observer(
        &self,
        observer: Arc<dyn TestEventObserver>,
    ) -> Result<(), ExchangeFailure> {
        let mut state = self
            .receiver
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
}

fn get_or_create(registry: &ExecutionExchangeRegistry, key: ExchangeKey) -> Arc<Receiver> {
    use tracing::debug;

    let mut guard = registry.receivers.lock().expect("exchange lock");
    let existed = guard.contains_key(&key);
    let receiver = guard
        .entry(key)
        .or_insert_with(|| {
            Arc::new(Receiver {
                mu: Mutex::new(ReceiverState::default()),
                cv: Condvar::new(),
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
        let mut guard = self.receivers.lock().expect("exchange lock");
        let keys: Vec<ExchangeKey> = guard
            .keys()
            .copied()
            .filter(|k| k.finst_id_hi == finst_id_hi && k.finst_id_lo == finst_id_lo)
            .collect();
        for k in keys {
            mark_key_canceled(self, k);
            guard.remove(&k);
        }
    }

    pub fn remove_exchange_key(&self, key: ExchangeKey) {
        mark_key_canceled(self, key);
        self.receivers.lock().expect("exchange lock").remove(&key);
    }

    /// Cancel all exchange receivers for a fragment instance on error or FE cancel.
    /// Wakes blocked waiters so they can observe the cancellation, then removes
    /// entries from the global map.
    pub fn cancel_fragment(&self, finst_id_hi: i64, finst_id_lo: i64) {
        let mut guard = self.receivers.lock().expect("exchange lock");
        let keys: Vec<ExchangeKey> = guard
            .keys()
            .copied()
            .filter(|k| k.finst_id_hi == finst_id_hi && k.finst_id_lo == finst_id_lo)
            .collect();
        for k in keys {
            mark_key_canceled(self, k);
            if let Some(r) = guard.get(&k).cloned() {
                let notify = r.observable.defer_notify();
                let mut st = r.mu.lock().expect("exchange receiver lock");
                st.canceled = true;
                r.cv.notify_all();
                drop(st);
                notify.arm();
            }
            guard.remove(&k);
        }
    }

    pub fn cancel_exchange_key(&self, key: ExchangeKey) {
        mark_key_canceled(self, key);
        let r = {
            let guard = self.receivers.lock().expect("exchange lock");
            guard.get(&key).cloned()
        };
        if let Some(r) = r {
            let notify = r.observable.defer_notify();
            let mut st = r.mu.lock().expect("exchange receiver lock");
            st.canceled = true;
            r.cv.notify_all();
            drop(st);
            notify.arm();
        }
        self.receivers.lock().expect("exchange lock").remove(&key);
    }

    pub fn set_expected_senders(&self, key: ExchangeKey, expected_senders: usize) {
        use tracing::debug;

        if is_key_canceled(self, &key) {
            return;
        }
        let r = get_or_create(self, key);
        let mut st = r.mu.lock().expect("exchange receiver lock");
        let before = st.expected_senders;
        st.expected_senders = st.expected_senders.max(expected_senders);
        if st.expected_senders != before {
            debug!(
                "exchange expected_senders UPDATED: finst={} node_id={} before={} after={}",
                key.finst_uuid(),
                key.node_id,
                before,
                st.expected_senders
            );
        }
        r.cv.notify_all();
    }

    /// Register a legacy receiver: wire columns bind by slot id
    /// ([`ExchangeColumnBinding::BySlotId`]).
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
        .map(|_| ())
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
        .map(|_| ())
    }

    /// Return the original registered receiver before releasing its capture;
    /// concurrent cancellation may remove routing but cannot lose this loan.
    pub fn try_register_expected_chunk_schema_with_close_observer(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
        chunk_schema: ChunkSchemaRef,
        column_binding: ExchangeColumnBinding,
        creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> Result<ExchangeReceiverCloseObserver, ExchangeFailure> {
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
    ) -> Result<ExchangeReceiverCloseObserver, ExchangeFailure> {
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
        st.expected_senders = st.expected_senders.max(expected_senders);
        receiver.cv.notify_all();
        drop(st);
        Ok(ExchangeReceiverCloseObserver { key, receiver })
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

    pub fn push_chunks(
        &self,
        key: ExchangeKey,
        sender: ExchangeSenderIdentity,
        chunks: Vec<Chunk>,
        eos: bool,
    ) {
        self.push_chunks_with_stats(key, sender, chunks, eos, 0, 0);
    }

    pub fn push_chunks_with_stats(
        &self,
        key: ExchangeKey,
        sender: ExchangeSenderIdentity,
        mut chunks: Vec<Chunk>,
        eos: bool,
        payload_bytes: usize,
        deserialize_ns: u128,
    ) {
        use tracing::debug;
        if is_key_canceled(self, &key) {
            return;
        }
        let chunks_len = chunks.len();
        let row_count: usize = chunks.iter().map(|c| c.len()).sum();
        let should_notify = chunks_len != 0 || eos;

        debug!(
            "push_chunks: finst={} node_id={} sender={} chunks={} rows={} eos={}",
            key.finst_uuid(),
            key.node_id,
            sender,
            chunks_len,
            row_count,
            eos
        );

        let r = get_or_create(self, key);

        let lock_start = Instant::now();
        let mut st = r.mu.lock().expect("exchange receiver lock");
        let lock_wait = lock_start.elapsed();
        if lock_wait >= EXCHANGE_LOCK_WAIT_WARN {
            debug!(
                "exchange receiver lock WAIT: finst={} node_id={} wait_ms={}",
                key.finst_uuid(),
                key.node_id,
                lock_wait.as_millis()
            );
        }
        let hold_start = Instant::now();
        if st.canceled {
            debug!(
                "push_chunks: CANCELED, dropping {} chunks ({} rows) from sender={}",
                chunks_len, row_count, sender
            );
            return;
        }
        st.recv_request_count = st.recv_request_count.saturating_add(1);
        st.recv_payload_bytes = st.recv_payload_bytes.saturating_add(payload_bytes as u128);
        st.recv_deserialize_ns = st.recv_deserialize_ns.saturating_add(deserialize_ns);
        st.recv_chunks = st.recv_chunks.saturating_add(chunks_len as u128);
        st.recv_rows = st.recv_rows.saturating_add(row_count as u128);
        if chunks_len != 0 {
            if let Some(tracker) = st.mem_tracker.as_ref() {
                for chunk in chunks.iter_mut() {
                    chunk.transfer_to(tracker);
                }
            }
            st.chunks
                .extend(chunks.into_iter().map(SourcedExchangeChunk::local));
        }
        if eos {
            st.finished.insert(sender);
            debug!(
                "push_chunks: sender={} marked as FINISHED, total finished={}/{}",
                sender,
                st.finished.len(),
                st.expected_senders
            );
        }
        #[cfg(novarocks_dev_controls)]
        let eos_snapshot = if eos {
            st.test_observer
                .clone()
                .filter(|observer| observer.is_active())
                .map(|observer| {
                    let mut finished = st.finished.iter().copied().collect::<Vec<_>>();
                    finished.sort_unstable();
                    let queued_rows = st.chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
                    (
                        observer,
                        st.expected_senders,
                        finished,
                        st.chunks.len(),
                        queued_rows,
                        r.observable.generation(),
                    )
                })
        } else {
            None
        };
        r.cv.notify_all();
        drop(st);
        let hold_time = hold_start.elapsed();
        #[cfg(novarocks_dev_controls)]
        if let Some((
            observer,
            expected_senders,
            finished,
            queued_chunks,
            queued_rows,
            receiver_generation_before_notify,
        )) = eos_snapshot
        {
            emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                format!(
                    "event=push_eos finst={} node_id={} sender={} expected_senders={} finished_senders={:?} queued_chunks={} queued_rows={} receiver_generation_before_notify={}",
                    key.finst_uuid(),
                    key.node_id,
                    sender,
                    expected_senders,
                    finished,
                    queued_chunks,
                    queued_rows,
                    receiver_generation_before_notify,
                )
            });
        }
        if hold_time >= EXCHANGE_LOCK_HOLD_WARN {
            debug!(
                "exchange receiver lock HOLD: finst={} node_id={} chunks={} rows={} eos={} hold_ms={}",
                key.finst_uuid(),
                key.node_id,
                chunks_len,
                row_count,
                eos,
                hold_time.as_millis()
            );
        }
        if should_notify {
            let notify = r.observable.defer_notify();
            notify.arm();
        }
    }
}

impl ExecutionExchangeRegistry {
    pub fn take_all_chunks_blocking(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
        timeout: Duration,
    ) -> Result<Vec<Chunk>, ExchangeFailure> {
        let (chunks, _stats) =
            self.take_all_chunks_with_stats_blocking(key, expected_senders, timeout)?;
        Ok(chunks)
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

impl ExecutionExchangeRegistry {
    pub fn take_all_chunks_with_stats_blocking(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
        timeout: Duration,
    ) -> Result<(Vec<Chunk>, ExchangeRecvStats), ExchangeFailure> {
        use tracing::debug;
        if is_key_canceled(self, &key) {
            return Err(exchange_stopped());
        }

        debug!(
            "take_all_chunks_blocking START: finst={} node_id={} expected_senders={} timeout={:?}",
            key.finst_uuid(),
            key.node_id,
            expected_senders,
            timeout
        );

        self.set_expected_senders(key, expected_senders);
        let r = get_or_create(self, key);

        let start = Instant::now();
        let mut st = r.mu.lock().expect("exchange receiver lock");
        loop {
            if st.canceled {
                debug!(
                    "take_all_chunks_blocking CANCELED: finst={} node_id={} elapsed={:?}",
                    key.finst_uuid(),
                    key.node_id,
                    start.elapsed()
                );
                return Err(exchange_stopped());
            }
            let expected = st.expected_senders.max(expected_senders);
            if expected == 0 || st.finished.len() >= expected {
                let row_count: usize = st.chunks.iter().map(|c| c.len()).sum();
                debug!(
                    "take_all_chunks_blocking COMPLETE: finst={} node_id={} expected={} finished={} chunks={} rows={} elapsed={:?}",
                    key.finst_uuid(),
                    key.node_id,
                    expected,
                    st.finished.len(),
                    st.chunks.len(),
                    row_count,
                    start.elapsed()
                );
                break;
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                debug!(
                    "take_all_chunks_blocking TIMEOUT: finst={} node_id={} expected={} finished={} elapsed={:?} timeout={:?}",
                    key.finst_uuid(),
                    key.node_id,
                    expected,
                    st.finished.len(),
                    elapsed,
                    timeout
                );
                return Err(exchange_contract(format_args!(
                    "exchange timeout waiting for senders: finst_id={} node_id={} expected={} finished={}",
                    key.finst_uuid(),
                    key.node_id,
                    expected,
                    st.finished.len()
                )));
            }
            let remain = timeout - elapsed;

            let wait_step = remain.min(EXCHANGE_WAIT_LOG_INTERVAL);
            let (next, wait_res) =
                r.cv.wait_timeout(st, wait_step)
                    .map_err(|_| failure::poisoned("receiver wait"))?;
            st = next;
            if wait_res.timed_out() && wait_step < remain {
                let elapsed_after = start.elapsed();
                let remain_after = timeout.saturating_sub(elapsed_after);
                debug!(
                    "take_all_chunks_blocking WAITING: finst={} node_id={} expected={} finished={} elapsed={:?} remain={:?}",
                    key.finst_uuid(),
                    key.node_id,
                    expected,
                    st.finished.len(),
                    elapsed_after,
                    remain_after
                );
            }
        }

        let chunks: Vec<Chunk> = st.chunks.drain(..).map(|item| item.chunk).collect();
        let stats = ExchangeRecvStats {
            request_received: st.recv_request_count,
            bytes_received: st.recv_payload_bytes,
            deserialize_ns: st.recv_deserialize_ns,
            chunks_received: st.recv_chunks,
            rows_received: st.recv_rows,
        };
        drop(st);
        // DO NOT remove the receiver here.
        // let mut guard = exchange().lock().expect("exchange lock");
        // guard.remove(&key);
        Ok((chunks, stats))
    }
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
        use tracing::debug;

        let r = &self.receiver;
        let mut st = r.mu.lock().expect("exchange receiver lock");
        if st.canceled {
            debug!(
                "exchange try_pop_next CANCELED: finst={} node_id={}",
                self.key.finst_uuid(),
                self.key.node_id
            );
            return Err(exchange_stopped());
        }

        if let Some(chunk) = st.chunks.pop_front() {
            return Ok(Some(SourcedExchangePopResult::Chunk(chunk)));
        }

        let expected = st.expected_senders.max(expected_senders);
        if expected == 0 || st.finished.len() >= expected {
            let stats = ExchangeRecvStats {
                request_received: st.recv_request_count,
                bytes_received: st.recv_payload_bytes,
                deserialize_ns: st.recv_deserialize_ns,
                chunks_received: st.recv_chunks,
                rows_received: st.recv_rows,
            };
            drop(st);
            // DO NOT remove the receiver here.
            // In high-DOP scenarios, multiple drivers share the same receiver.
            // If the first driver finishes and removes it, subsequent drivers may create a new empty receiver and hang.
            // Cleanup relies on explicit cancel_fragment() or TTL.
            // exchange().lock().expect("exchange lock").remove(&self.key);
            return Ok(Some(SourcedExchangePopResult::Finished(stats)));
        }

        Ok(None)
    }

    /// Called by the input owner only after its real source has exhausted
    /// data and End. ACK, Task outcome and readiness peeks do not call this.
    pub fn complete_input_consumption(
        &self,
        expected_senders: usize,
    ) -> Result<(), ExchangeFailure> {
        let mut st = self.receiver.mu.lock().expect("exchange receiver lock");
        if st.canceled {
            return Err(exchange_stopped());
        }
        let expected = st.expected_senders.max(expected_senders);
        if !st.chunks.is_empty() || st.finished.len() < expected {
            return Err(exchange_contract(
                "exchange input has not exhausted every sender's data and End",
            ));
        }
        if st.streams.values().any(|stream| stream.end.is_none()) {
            return Err(exchange_contract("exchange stream has no admitted End"));
        }
        for stream in st.streams.values_mut() {
            stream.consumed = true;
        }
        drop(st);
        self.receiver.observable.defer_notify().arm();
        Ok(())
    }

    pub fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.receiver.observable)
    }

    pub fn has_output_or_finished(&self, expected_senders: usize) -> bool {
        let r = &self.receiver;
        let st = r.mu.lock().expect("exchange receiver lock");
        let canceled = st.canceled;
        let has_chunks = !st.chunks.is_empty();
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

    pub fn pop_next_with_stats_blocking(
        &self,
        expected_senders: usize,
        start: Instant,
        timeout: Duration,
    ) -> Result<ExchangePopResult, ExchangeFailure> {
        use tracing::debug;

        let r = &self.receiver;
        let mut st = r.mu.lock().expect("exchange receiver lock");
        loop {
            if st.canceled {
                debug!(
                    "exchange pop_next CANCELED: finst={} node_id={}",
                    self.key.finst_uuid(),
                    self.key.node_id
                );
                return Err(exchange_stopped());
            }

            if let Some(chunk) = st.chunks.pop_front() {
                return Ok(ExchangePopResult::Chunk(chunk.chunk));
            }

            let expected = st.expected_senders.max(expected_senders);
            if expected == 0 || st.finished.len() >= expected {
                let stats = ExchangeRecvStats {
                    request_received: st.recv_request_count,
                    bytes_received: st.recv_payload_bytes,
                    deserialize_ns: st.recv_deserialize_ns,
                    chunks_received: st.recv_chunks,
                    rows_received: st.recv_rows,
                };
                drop(st);
                // DO NOT remove the receiver here.
                // exchange().lock().expect("exchange lock").remove(&self.key);
                return Ok(ExchangePopResult::Finished(stats));
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                debug!(
                    "exchange pop_next TIMEOUT: finst={} node_id={} expected={} finished={} elapsed={:?} timeout={:?}",
                    self.key.finst_uuid(),
                    self.key.node_id,
                    expected,
                    st.finished.len(),
                    elapsed,
                    timeout
                );
                return Err(exchange_contract(format_args!(
                    "exchange timeout waiting for senders: finst_id={} node_id={} expected={} finished={}",
                    self.key.finst_uuid(),
                    self.key.node_id,
                    expected,
                    st.finished.len()
                )));
            }
            let remain = timeout - elapsed;
            let wait_step = remain.min(EXCHANGE_WAIT_LOG_INTERVAL);
            let (next, wait_res) =
                r.cv.wait_timeout(st, wait_step)
                    .map_err(|_| failure::poisoned("receiver wait"))?;
            st = next;
            if wait_res.timed_out() && wait_step < remain {
                let elapsed_after = start.elapsed();
                let remain_after = timeout.saturating_sub(elapsed_after);
                debug!(
                    "exchange pop_next WAITING: finst={} node_id={} expected={} finished={} elapsed={:?} remain={:?}",
                    self.key.finst_uuid(),
                    self.key.node_id,
                    expected,
                    st.finished.len(),
                    elapsed_after,
                    remain_after
                );
            }
        }
    }
}

impl ExecutionExchangeRegistry {
    pub fn get_receiver_handle(
        &self,
        key: ExchangeKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, ExchangeFailure> {
        use tracing::debug;

        if is_key_canceled(self, &key) {
            return Err(exchange_stopped());
        }
        let receiver = get_or_create(self, key);
        {
            let mut st = receiver.mu.lock().expect("exchange receiver lock");
            let before = st.expected_senders;
            st.expected_senders = st.expected_senders.max(expected_senders);
            if st.expected_senders != before {
                debug!(
                    "exchange get_receiver_handle UPDATED expected_senders: finst={} node_id={} before={} after={}",
                    key.finst_uuid(),
                    key.node_id,
                    before,
                    st.expected_senders
                );
            }
            receiver.cv.notify_all();
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
    pub fn admit_native_frame(
        &self,
        key: ExchangeKey,
        frame: crate::runtime::fragment::io::ExchangeReceiverFrame,
    ) -> Result<(), ExchangeFailure> {
        let stream = frame.stream;
        if stream.destination_fragment_instance_id()
            != UniqueId::new(key.finst_id_hi, key.finst_id_lo)
            || stream.destination_node().get() != key.node_id
            || stream.source().fragment_instance_id() != frame.source_fragment_instance_id
            || stream.source().sender_ordinal() != frame.sender_ordinal
            || frame.sender_count == 0
            || frame.sender_ordinal >= frame.sender_count
        {
            return Err(exchange_contract(
                "exchange frame differs from its exact admitted stream",
            ));
        }
        let sequence = ExchangeFrameSequence::try_new(frame.sequence).map_err(exchange_contract)?;
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("exchange receiver is not registered"))?;
        let sender =
            ExchangeSenderIdentity::native(frame.source_fragment_instance_id, frame.sender_ordinal);
        let admission = {
            let mut st = receiver.mu.lock().expect("exchange receiver lock");
            if st.canceled {
                return Err(exchange_stopped());
            }
            if st.expected_senders != frame.sender_count as usize {
                return Err(exchange_contract(
                    "exchange sender count differs from its frozen receiver",
                ));
            }
            if let Some(prior) = st.streams.get(&sender) {
                if prior.identity != stream {
                    return Err(exchange_contract("exchange stream identity changed"));
                }
                prior.admission.clone()
            } else {
                if sequence != ExchangeFrameSequence::FIRST
                    || st.streams.len() >= st.expected_senders
                {
                    return Err(exchange_contract(
                        "exchange first frame is not the exact stream's first sequence",
                    ));
                }
                let expected = st
                    .expected
                    .as_ref()
                    .ok_or_else(|| {
                        exchange_contract("exchange receiver has no frozen declaration")
                    })?
                    .clone();
                let owner = expected
                    .creation_factory
                    .begin_typed_construction(&expected.chunk_schema)?;
                let growth = novarocks_type_contract::owned_resources::hashmap::next_insert_allocation_upper_bound_observed::<ExchangeSenderIdentity, ReceivedExchangeStream, ExchangeFailure>(
                    st.streams.len(), st.streams.capacity(), || {
                        owner.observe(0, &[], crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 })?; Ok(())
                    })?;
                let table_owner = if growth != 0 {
                    let owner = expected
                        .creation_factory
                        .begin_typed_construction(&expected.chunk_schema)?;
                    owner.observe_opaque_upper_bound(growth)?;
                    Some(owner)
                } else {
                    None
                };
                owner.request(
                    novarocks_type_contract::owned_resources::layout::arc_layout(
                        std::alloc::Layout::new::<Mutex<()>>(),
                    )
                    .map_err(crate::exec::pipeline::operator::ConstructionFailure::from)?,
                )?;
                let admission = Arc::new(Mutex::new(()));
                st.streams.insert(
                    sender,
                    ReceivedExchangeStream {
                        admission: admission.clone(),
                        _admission_storage: owner.storage_lifetime(),
                        identity: stream,
                        next: 0,
                        end: None,
                        consumed: false,
                    },
                );
                if let Some(owner) = table_owner {
                    st.stream_table_storage = Some(owner.storage_lifetime());
                }
                admission
            }
        };
        // Never wait for this stream's decoder while holding receiver.mu.
        // Independent streams decode concurrently; each stream publishes in order.
        let _admission = admission.lock().expect("exchange stream admission lock");
        {
            let st = receiver.mu.lock().expect("exchange receiver lock");
            if st.canceled {
                return Err(exchange_stopped());
            }
            let prior = st
                .streams
                .get(&sender)
                .ok_or_else(|| exchange_contract("exchange stream admission disappeared"))?;
            if prior.end.is_some() || prior.next != frame.sequence {
                return Err(exchange_contract(
                    "exchange sequence continuity or End invariant failed",
                ));
            }
        }
        let next = if frame.eos {
            frame.sequence
        } else {
            sequence.checked_next().map_err(exchange_contract)?.get()
        };
        let start = Instant::now();
        let mut chunks = self.decode_chunks_for_sender(key, sender, &frame.payload)?;
        let notify = receiver.observable.defer_notify();
        let mut st = receiver.mu.lock().expect("exchange receiver lock");
        if st.canceled {
            return Err(exchange_stopped());
        }
        let rows: usize = chunks.iter().map(Chunk::len).sum();
        if let Some(tracker) = st.mem_tracker.as_ref() {
            for chunk in &mut chunks {
                chunk.transfer_to(tracker);
            }
        }
        st.recv_request_count = st.recv_request_count.saturating_add(1);
        st.recv_payload_bytes = st
            .recv_payload_bytes
            .saturating_add(frame.payload.len() as u128);
        st.recv_deserialize_ns = st
            .recv_deserialize_ns
            .saturating_add(start.elapsed().as_nanos());
        st.recv_chunks = st.recv_chunks.saturating_add(chunks.len() as u128);
        st.recv_rows = st.recv_rows.saturating_add(rows as u128);
        st.chunks
            .extend(chunks.into_iter().map(|chunk| SourcedExchangeChunk {
                source: Some(stream.source()),
                chunk,
            }));
        let accepted = st
            .streams
            .get_mut(&sender)
            .ok_or_else(|| exchange_contract("exchange stream admission disappeared"))?;
        accepted.next = next;
        accepted.end = frame.eos.then_some(sequence);
        if frame.eos {
            st.finished.insert(sender);
        }
        #[cfg(novarocks_dev_controls)]
        let eos_snapshot = if frame.eos {
            st.test_observer
                .clone()
                .filter(|o| o.is_active())
                .map(|observer| {
                    let mut finished = st.finished.iter().copied().collect::<Vec<_>>();
                    finished.sort_unstable();
                    (
                        observer,
                        st.expected_senders,
                        finished,
                        st.chunks.len(),
                        st.chunks.iter().map(|c| c.len()).sum::<usize>(),
                        receiver.observable.generation(),
                    )
                })
        } else {
            None
        };
        drop(st);
        receiver.cv.notify_all();
        #[cfg(novarocks_dev_controls)]
        if let Some((observer, expected, finished, queued_chunks, queued_rows, generation)) =
            eos_snapshot
        {
            emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                format!(
                    "event=push_eos finst={} node_id={} sender={} expected_senders={} finished_senders={:?} queued_chunks={} queued_rows={} receiver_generation_before_notify={}",
                    key.finst_uuid(),
                    key.node_id,
                    sender,
                    expected,
                    finished,
                    queued_chunks,
                    queued_rows,
                    generation
                )
            });
        }
        notify.arm();
        Ok(())
    }

    /// Borrow an existing original receiver at registration time. This never
    /// creates a receiver or replaces one canceled under the same numeric key.
    pub fn close_observer(
        &self,
        key: ExchangeKey,
    ) -> Result<ExchangeReceiverCloseObserver, ExchangeFailure> {
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("exchange receiver is not registered"))?;
        Ok(ExchangeReceiverCloseObserver { key, receiver })
    }
    /// Routing-time observation; lifecycle owners capture close_observer once
    /// during registration so cancellation cannot remove their read capability.
    pub fn stream_close_facts(
        &self,
        key: ExchangeKey,
        expected: &[ExchangeStreamIdentity],
    ) -> Result<Vec<ExchangeStreamClose>, ExchangeFailure> {
        self.close_observer(key)?.stream_close_facts(expected)
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
pub fn set_expected_senders(key: ExchangeKey, expected_senders: usize) {
    test_registry().set_expected_senders(key, expected_senders);
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
pub fn push_chunks(
    key: ExchangeKey,
    sender_id: i32,
    backend_number: i32,
    chunks: Vec<Chunk>,
    eos: bool,
) {
    test_registry().push_chunks(
        key,
        ExchangeSenderIdentity::local(sender_id, backend_number),
        chunks,
        eos,
    );
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
mod tests {
    // These fixtures explicitly declare ordinary physical values. Arrow
    // annotations are source facts and do not author any nominal domain.
    pub(super) fn plain_declaration(schema: &ChunkSchema) -> ChunkSchemaRef {
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let slots = schema
            .slots()
            .iter()
            .map(|slot| {
                let value = FunctionValueType::new(slot.data_type().clone(), slot.nullable());
                ChunkSlotSchema::try_new_typed(
                    slot.slot_id(),
                    Arc::new(Field::new(
                        slot.name(),
                        value.data_type().clone(),
                        value.nullable(),
                    )),
                    value.typed_binding().clone(),
                    slot.unique_id(),
                    ValueTypeStorageLifetime::pure(),
                )
                .expect("explicit plain fixture declaration")
            })
            .collect();
        Arc::new(ChunkSchema::try_new_typed(slots, ValueTypeStorageLifetime::pure()).unwrap())
    }

    use arrow::array::{
        Array, ArrayRef, Decimal128Array, DictionaryArray, Int32Array, Int64Array, MapArray,
        StringArray, StructArray, make_array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::record_batch::RecordBatchOptions;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::result_ipc_preflight::{
        ARROW_IPC_CONTINUATION_MARKER, MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES,
        MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS,
    };
    use super::{
        BoundedPayloadWriter, ExchangeKey, ExchangePopResult, ExchangeSenderIdentity,
        ExchangeWireMeta, ExecutionExchangeRegistry, cancel_exchange_key, decode_chunks,
        decode_chunks_for_sender, decode_root_result_chunks, encode_chunks, encode_chunks_bounded,
        encode_exchange_payload_envelope, get_receiver_handle, preflight_typed_root_result_decode,
        push_chunks, register_expected_chunk_schema, set_expected_senders, snapshot_receiver_state,
    };
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
    use novarocks_types::{SlotId, UniqueId};
    use std::io::Write as _;

    const EXCHANGE_TEST_SLOT_IDS: [SlotId; 4] = [
        SlotId::new(33),
        SlotId::new(34),
        SlotId::new(31),
        SlotId::new(32),
    ];

    fn exchange_test_chunk(last_name: &str) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int32, true),
            Field::new("v2", DataType::Int32, true),
            Field::new("k1", DataType::Int32, true),
            Field::new(last_name, DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(Int32Array::from(vec![Some(2)])),
            Arc::new(Int32Array::from(vec![Some(3)])),
            Arc::new(StringArray::from(vec![Some("x")])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &EXCHANGE_TEST_SLOT_IDS,
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn exchange_test_chunk_without_metadata(last_name: &str) -> Chunk {
        let chunk = exchange_test_chunk(last_name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int32, true),
            Field::new("v2", DataType::Int32, true),
            Field::new("k1", DataType::Int32, true),
            Field::new(last_name, DataType::Utf8, true),
        ]));
        let batch =
            RecordBatch::try_new(schema, chunk.record_batch().columns().to_vec()).expect("batch");
        Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref()).expect("chunk")
    }

    fn exchange_test_zero_column_chunk(row_count: usize) -> Chunk {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
            .expect("empty batch");
        Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty())).unwrap()
    }

    fn decimal128_chunk_with_over_precision_value(value: i128) -> Chunk {
        let wide = Decimal128Array::from(vec![Some(value)])
            .with_precision_and_scale(38, 2)
            .expect("wide decimal");
        let data = wide
            .to_data()
            .into_builder()
            .data_type(DataType::Decimal128(20, 2))
            .build()
            .expect("decimal20 array data");
        let array = make_array(data);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Decimal128(20, 2),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(55)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn decimal_slot_schema(
        slot_id: SlotId,
        name: &str,
        nullable: bool,
        precision: u8,
        scale: i8,
        unique_id: Option<i32>,
    ) -> ChunkSlotSchema {
        ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new(name, DataType::Decimal128(precision, scale), nullable),
            None,
            unique_id,
        )
    }

    fn dictionary_status_chunk(
        slot_id: SlotId,
        keys: Vec<Option<i32>>,
        values: Vec<&str>,
    ) -> Chunk {
        let dict = DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys),
            Arc::new(StringArray::from(values)),
        );
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot_id,
                Field::new("status", DataType::Utf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_columns(schema, vec![Arc::new(dict) as ArrayRef])
            .expect("dictionary chunk")
    }

    fn logical_status_schema(slot_id: SlotId) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot_id,
                Field::new("status", DataType::Utf8, true),
                None,
                None,
            )])
            .expect("logical status schema"),
        )
    }

    fn dictionary_chunk_with_type(slot_id: SlotId, data_type: DataType, column: ArrayRef) -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot_id,
                Field::new("bad_dict", data_type, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_columns(schema, vec![column]).expect("dictionary chunk")
    }

    fn logical_utf8_values(array: &ArrayRef) -> Vec<Option<String>> {
        let flat =
            arrow::compute::cast(array.as_ref(), &DataType::Utf8).expect("cast dictionary to utf8");
        let strings = flat
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 array");
        (0..strings.len())
            .map(|idx| {
                if strings.is_null(idx) {
                    None
                } else {
                    Some(strings.value(idx).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn encode_chunks_round_trips_dictionary_batches_with_replaced_values() {
        let first = dictionary_status_chunk(
            SlotId::new(71),
            vec![Some(0), Some(1), None, Some(0)],
            vec!["PAID", "NEW"],
        );
        let second = dictionary_status_chunk(
            SlotId::new(71),
            vec![Some(0), Some(1), Some(0), None],
            vec!["CANCELLED", "PAID"],
        );

        let bytes = encode_chunks(&[first, second], true).expect("encode dictionary chunks");
        let decoded = decode_chunks(&bytes).expect("decode dictionary chunks");

        assert_eq!(decoded.len(), 2);
        assert!(matches!(
            decoded[0].columns()[0].data_type(),
            DataType::Dictionary(_, _)
        ));
        assert!(matches!(
            decoded[1].columns()[0].data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(
            logical_utf8_values(&decoded[0].columns()[0]),
            vec![
                Some("PAID".to_string()),
                Some("NEW".to_string()),
                None,
                Some("PAID".to_string()),
            ]
        );
        assert_eq!(
            logical_utf8_values(&decoded[1].columns()[0]),
            vec![
                Some("CANCELLED".to_string()),
                Some("PAID".to_string()),
                Some("CANCELLED".to_string()),
                None,
            ]
        );
    }

    #[test]
    fn bounded_encode_uses_one_capped_backing_payload() {
        let chunk = exchange_test_chunk("name");
        let expected = encode_chunks(std::slice::from_ref(&chunk), true).expect("encode payload");
        let packet_cap = expected.len() + 1024;
        let encoded = encode_chunks_bounded(&[chunk], true, packet_cap).expect("bounded encode");
        assert!(!encoded.is_empty());
        let (payload, retained_bytes) = encoded.into_parts();

        assert_eq!(payload.as_ref(), expected.as_slice());
        assert!(retained_bytes >= payload.len());
        assert!(retained_bytes <= packet_cap);
    }

    #[test]
    fn bounded_encode_reports_empty_payload() {
        let encoded = encode_chunks_bounded(&[], true, 0).expect("empty bounded encode");

        assert!(encoded.is_empty());
        assert_eq!(encoded.retained_bytes(), 0);
    }

    #[test]
    fn bounded_encode_fails_while_crossing_packet_cap() {
        let error = encode_chunks_bounded(&[exchange_test_chunk("name")], true, 32)
            .expect_err("Arrow payload must exceed the bounded packet cap");

        assert!(error.to_string().contains("exceeds packet cap"), "{error}");
    }

    fn preflight_typed_test_payload(
        payload: &[u8],
    ) -> Result<super::TypedRootResultDecodeBounds, super::ExchangeFailure> {
        let limit = novarocks_execution_contract::ResultByteLimit::new(
            u64::try_from(payload.len()).expect("fixture payload length fits u64"),
        )
        .expect("fixture payload is nonempty");
        preflight_typed_root_result_decode(payload, limit)
    }

    #[test]
    fn typed_root_result_preflight_bounds_encoder_round_trip_backing_bytes() {
        let cases = vec![
            exchange_test_chunk("name"),
            exchange_test_zero_column_chunk(7),
            decimal128_chunk_with_over_precision_value(123_456_789),
            dictionary_status_chunk(
                SlotId::new(91),
                vec![Some(0), Some(1), None, Some(0)],
                vec!["READY", "RUNNING"],
            ),
        ];

        for chunk in cases {
            let expected = plain_declaration(chunk.chunk_schema());
            let payload = encode_chunks(&[chunk], true).expect("encode typed root result");
            let bounds = preflight_typed_test_payload(&payload)
                .expect("current encoder output must pass preflight");
            let decoded =
                decode_root_result_chunks(&payload, &expected).expect("decode root result");
            let actual_backing = decoded.iter().fold(0usize, |total, chunk| {
                total.saturating_add(chunk.logical_bytes())
            });

            assert!(
                u64::try_from(actual_backing).unwrap() <= bounds.retained_backing_upper_bound(),
                "decoded backing {actual_backing} exceeds {:?}",
                bounds
            );
            assert!(bounds.decode_operation_upper_bound() >= bounds.retained_backing_upper_bound());
        }
    }

    #[test]
    fn typed_root_result_preflight_rejects_non_current_stream_shapes() {
        let chunk = exchange_test_chunk("name");

        let mut missing_slot_flag = encode_chunks(std::slice::from_ref(&chunk), true).unwrap();
        missing_slot_flag[5] = 0;
        let error = preflight_typed_test_payload(&missing_slot_flag)
            .expect_err("slot metadata is mandatory");
        assert!(
            error.to_string().contains("requires exactly slot-id flag"),
            "{error}"
        );

        let multiple_batches = encode_chunks(&[chunk.clone(), chunk.clone()], true).unwrap();
        let error = preflight_typed_test_payload(&multiple_batches)
            .expect_err("one fetch packet cannot contain multiple record batches");
        assert!(
            error.to_string().contains("exactly one record batch"),
            "{error}"
        );

        let mut trailing = encode_chunks(&[chunk], true).unwrap();
        trailing.push(0);
        let error = preflight_typed_test_payload(&trailing)
            .expect_err("terminal marker must end the stream");
        assert!(
            error
                .to_string()
                .contains("terminal marker has trailing bytes"),
            "{error}"
        );
    }

    #[test]
    fn typed_root_result_preflight_enforces_caller_payload_limit() {
        let payload = encode_chunks(&[exchange_test_chunk("name")], true).unwrap();
        let limit = novarocks_execution_contract::ResultByteLimit::new(
            u64::try_from(payload.len() - 1).unwrap(),
        )
        .unwrap();

        let error = preflight_typed_root_result_decode(&payload, limit)
            .expect_err("caller payload limit must remain authoritative");
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
        assert_eq!(
            fact.class(),
            novarocks_failure_contract::FailureClass::Representation
        );
        assert_eq!(fact.reason().code(), 115);
        let extent = fact.representation().unwrap();
        assert_eq!(extent.required, Some(payload.len() as u64));
        assert_eq!(extent.extent_limit, Some(limit.get()));
        assert_eq!(extent.representable_prefix, None);
        assert!(fact.resource().is_none());
    }

    #[test]
    fn typed_root_result_preflight_rejects_compressed_stream() {
        let chunk = exchange_test_chunk("name");
        let schema = chunk.record_batch().schema();
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .expect("compression support");
        let mut arrow_payload = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
            &mut arrow_payload,
            schema.as_ref(),
            options,
        )
        .expect("compressed stream writer");
        writer
            .write(chunk.record_batch())
            .expect("compressed batch");
        writer.finish().expect("finish compressed stream");
        let wire_meta = ExchangeWireMeta::from_chunks(std::slice::from_ref(&chunk))
            .expect("wire metadata")
            .expect("typed wire metadata");
        let payload = encode_exchange_payload_envelope(&arrow_payload, Some(&wire_meta))
            .expect("exchange envelope fixture");

        let error = preflight_typed_test_payload(&payload)
            .expect_err("compressed root result must fail closed");
        assert!(
            error.to_string().contains("compression is not allowed"),
            "{error}"
        );
    }

    #[test]
    fn typed_root_result_preflight_rejects_exaggerated_and_negative_lengths() {
        let payload = encode_chunks(&[exchange_test_chunk("name")], true).unwrap();
        let arrow_offset = typed_test_arrow_payload_offset(&payload);

        let mut exaggerated_metadata = payload.clone();
        exaggerated_metadata[arrow_offset + 4..arrow_offset + 8].copy_from_slice(
            &u32::try_from(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES + 64)
                .unwrap()
                .to_le_bytes(),
        );
        let error = preflight_typed_test_payload(&exaggerated_metadata)
            .expect_err("exaggerated metadata must fail closed");
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
        assert_eq!(fact.reason().code(), 117);
        let extent = fact.representation().unwrap();
        assert_eq!(
            extent.required,
            Some((MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES + 64) as u64)
        );
        assert_eq!(
            extent.extent_limit,
            Some(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES as u64)
        );

        let mut negative_body = payload;
        mutate_record_batch_body_length(&mut negative_body, -1);
        let error = preflight_typed_test_payload(&negative_body)
            .expect_err("negative body length must fail closed");
        assert!(
            error.to_string().contains("body length is negative"),
            "{error}"
        );
        let error = decode_root_result_chunks(
            &negative_body,
            &plain_declaration(exchange_test_chunk("name").chunk_schema()),
        )
        .expect_err("the production decode entry must enforce preflight");
        assert!(
            error.to_string().contains("body length is negative"),
            "{error}"
        );

        let mut oversized_body =
            encode_chunks(&[exchange_test_chunk("name")], true).expect("fresh payload");
        let oversized_body_length = i64::try_from(oversized_body.len() + 64).unwrap();
        mutate_record_batch_body_length(&mut oversized_body, oversized_body_length);
        let error = preflight_typed_test_payload(&oversized_body)
            .expect_err("oversized body length must fail closed");
        let extent = novarocks_failure_contract::FailureProjection::failure_fact(&error)
            .representation()
            .unwrap();
        assert_eq!(extent.required, Some(oversized_body_length as u64));
        assert_eq!(extent.extent_limit, Some(oversized_body.len() as u64));

        let mut negative_buffer =
            encode_chunks(&[exchange_test_chunk("name")], true).expect("fresh payload");
        mutate_unique_record_batch_buffer_length(&mut negative_buffer, -1);
        let error = preflight_typed_test_payload(&negative_buffer)
            .expect_err("negative buffer length must fail closed");
        assert!(
            error.to_string().contains("buffer") && error.to_string().contains("negative"),
            "{error}"
        );

        let mut excessive_structure =
            encode_chunks(&[exchange_test_chunk("name")], true).expect("fresh payload");
        excessive_structure[6..10].copy_from_slice(
            &u32::try_from(MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS + 1)
                .unwrap()
                .to_le_bytes(),
        );
        let error = preflight_typed_test_payload(&excessive_structure)
            .expect_err("excessive slot structure must fail closed");
        assert!(
            novarocks_failure_contract::FailureProjection::failure_fact(&error)
                .reason()
                .code()
                == 116,
            "{error}"
        );
    }

    #[test]
    fn typed_root_result_preflight_preserves_frame_error_precedence() {
        use novarocks_arrow_ipc_frame::{PrefixError, RangeError};
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        fn prefix(error: &super::ExchangeFailure) -> PrefixError {
            let mut source: &(dyn std::error::Error + 'static) = error;
            loop {
                if let Some(source) = source.downcast_ref::<PrefixError>() {
                    return *source;
                }
                source = source.source().expect("actual prefix author is retained");
            }
        }
        let payload = encode_chunks(&[exchange_test_chunk("name")], true).unwrap();
        let start = typed_test_arrow_payload_offset(&payload);
        let declared = u32::try_from(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES + 1).unwrap();
        // Oversize, alignment and truncation coincide; the original first
        // guard is still the message metadata extent, now a machine fact.
        let mut oversized = payload.clone();
        oversized[start + 4..start + 8].copy_from_slice(&declared.to_le_bytes());
        let error = preflight_typed_test_payload(&oversized).unwrap_err();
        assert_eq!(error.failure_fact().reason().code(), 117);
        let extent = error.failure_fact().representation().unwrap();
        assert_eq!(extent.required, Some(declared as u64));
        assert_eq!(
            extent.extent_limit,
            Some(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES as u64)
        );
        let mut bad_marker = oversized;
        bad_marker[start] = 0;
        let error = preflight_typed_test_payload(&bad_marker).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(prefix(&error), PrefixError::NotContinuation);
        bad_marker.truncate(start + 7);
        assert_eq!(
            prefix(&preflight_typed_test_payload(&bad_marker).unwrap_err()),
            PrefixError::Range(RangeError::OutOfBounds)
        );
        // EOS trailing bytes still fail before missing schema/record batch.
        let mut early_end = payload[..start].to_vec();
        early_end.extend_from_slice(&ARROW_IPC_CONTINUATION_MARKER);
        early_end.extend_from_slice(&0_u32.to_le_bytes());
        early_end.push(0);
        assert_eq!(
            preflight_typed_test_payload(&early_end)
                .unwrap_err()
                .to_string(),
            "Arrow IPC terminal marker has trailing bytes"
        );
    }

    fn typed_test_arrow_payload_offset(payload: &[u8]) -> usize {
        let slot_count = u32::from_le_bytes(payload[6..10].try_into().unwrap()) as usize;
        10 + slot_count * std::mem::size_of::<u32>()
    }

    fn mutate_record_batch_body_length(payload: &mut [u8], replacement: i64) {
        let mut offset = typed_test_arrow_payload_offset(payload);
        loop {
            assert_eq!(payload[offset..offset + 4], ARROW_IPC_CONTINUATION_MARKER);
            let metadata_len =
                u32::from_le_bytes(payload[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert_ne!(metadata_len, 0, "record batch must precede terminal marker");
            let metadata_start = offset + 8;
            let metadata_end = metadata_start + metadata_len;
            let message = arrow::ipc::root_as_message(&payload[metadata_start..metadata_end])
                .expect("valid metadata before mutation");
            let body_len = usize::try_from(message.bodyLength()).unwrap();
            if message.header_type() == arrow::ipc::MessageHeader::RecordBatch {
                let needle = message.bodyLength().to_le_bytes();
                let matches = payload[metadata_start..metadata_end]
                    .windows(needle.len())
                    .enumerate()
                    .filter_map(|(index, bytes)| (bytes == needle).then_some(index))
                    .collect::<Vec<_>>();
                assert_eq!(
                    matches.len(),
                    1,
                    "body length must have one metadata encoding"
                );
                let start = metadata_start + matches[0];
                payload[start..start + 8].copy_from_slice(&replacement.to_le_bytes());
                return;
            }
            offset = metadata_end + body_len;
        }
    }

    fn mutate_unique_record_batch_buffer_length(payload: &mut [u8], replacement: i64) {
        let mut offset = typed_test_arrow_payload_offset(payload);
        loop {
            assert_eq!(payload[offset..offset + 4], ARROW_IPC_CONTINUATION_MARKER);
            let metadata_len =
                u32::from_le_bytes(payload[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert_ne!(metadata_len, 0, "record batch must precede terminal marker");
            let metadata_start = offset + 8;
            let metadata_end = metadata_start + metadata_len;
            let message = arrow::ipc::root_as_message(&payload[metadata_start..metadata_end])
                .expect("valid metadata before mutation");
            let body_len = usize::try_from(message.bodyLength()).unwrap();
            if let Some(batch) = message.header_as_record_batch() {
                for descriptor in batch.buffers().expect("record-batch buffers").iter() {
                    let needle = descriptor.0;
                    let matches = payload[metadata_start..metadata_end]
                        .windows(needle.len())
                        .enumerate()
                        .filter_map(|(index, bytes)| (bytes == needle).then_some(index))
                        .collect::<Vec<_>>();
                    if matches.len() == 1 {
                        let length_start = metadata_start + matches[0] + 8;
                        payload[length_start..length_start + 8]
                            .copy_from_slice(&replacement.to_le_bytes());
                        return;
                    }
                }
                panic!("record batch must contain one uniquely encoded buffer descriptor");
            }
            offset = metadata_end + body_len;
        }
    }

    #[test]
    fn bounded_writer_grows_from_spare_capacity_without_unbounded_extend() {
        let mut writer = BoundedPayloadWriter::try_new(10, 8).expect("bounded writer");
        writer.write_all(&[1; 4]).expect("initial write");
        assert_eq!(writer.buffer.len(), 4);
        assert_eq!(writer.buffer.capacity(), 8);

        writer.write_all(&[2; 6]).expect("bounded growth");
        assert_eq!(writer.buffer.len(), 10);
        assert_eq!(writer.buffer.capacity(), 10);
    }

    #[test]
    fn bounded_writer_rejects_over_cap_without_mutating_buffer() {
        let mut writer = BoundedPayloadWriter::try_new(10, 8).expect("bounded writer");
        writer.write_all(&[1; 4]).expect("initial write");
        let before_len = writer.buffer.len();
        let before_capacity = writer.buffer.capacity();

        let error = writer
            .write_all(&[2; 7])
            .expect_err("write beyond cap must fail");
        assert!(error.to_string().contains("exceeds packet cap"));
        assert_eq!(writer.buffer.len(), before_len);
        assert_eq!(writer.buffer.capacity(), before_capacity);
    }

    #[test]
    fn root_ipc_source_annotations_cannot_change_frozen_nested_domains() {
        use arrow::array::new_null_array;
        use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY};
        fn marked(field: Field, marker: Option<&str>) -> Field {
            match marker {
                Some(value) => {
                    field.with_metadata(HashMap::from([(NR_LOGICAL_TYPE_KEY.into(), value.into())]))
                }
                None => field,
            }
        }
        fn nested(kind: usize, marker: Option<&str>) -> Field {
            let leaf = || Arc::new(marked(Field::new("value", DataType::Binary, true), marker));
            let data_type = match kind {
                0 => DataType::Struct(vec![leaf()].into()),
                1 => DataType::List(leaf()),
                4 => DataType::LargeList(leaf()),
                2 | 3 => {
                    let value = if kind == 2 {
                        leaf()
                    } else {
                        Arc::new(Field::new("value", DataType::Binary, true))
                    };
                    let entries = Field::new(
                        "entries",
                        DataType::Struct(
                            vec![Arc::new(Field::new("key", DataType::Int32, false)), value].into(),
                        ),
                        false,
                    );
                    DataType::Map(
                        Arc::new(if kind == 3 {
                            marked(entries, marker)
                        } else {
                            entries
                        }),
                        false,
                    )
                }
                _ => unreachable!(),
            };
            Field::new("root", data_type, true)
        }
        for kind in 0..5 {
            let declared = nested(
                kind,
                if kind == 3 {
                    None
                } else {
                    Some(LogicalType::Bitmap.metadata_value())
                },
            );
            use novarocks_type_contract::{
                TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType,
                ValueTypeRef, ValueTypeStorageLifetime,
            };
            let storage = ValueTypeStorageLifetime::pure();
            let leaf = ValueTypeRef::try_new(
                if kind == 3 {
                    ValueKind::Binary
                } else {
                    ValueKind::Nominal(ValueLogicalType::Bitmap)
                },
                true,
                storage.clone(),
            )
            .unwrap();
            let root = match kind {
                0 => ValueKind::Struct(
                    vec![ValueField::new("value", leaf, storage.clone())].into_boxed_slice(),
                ),
                1 | 4 => ValueKind::List(leaf),
                2 | 3 => ValueKind::Map {
                    key: ValueTypeRef::try_new(
                        ValueKind::Scalar(DataType::Int32),
                        false,
                        storage.clone(),
                    )
                    .unwrap(),
                    value: leaf,
                },
                _ => unreachable!(),
            };
            let value = ValueTypeRef::try_new(root, true, storage.clone()).unwrap();
            let layout = ValueLayout::try_new(declared.data_type(), storage.clone()).unwrap();
            let field = Arc::new(Field::new("root", layout.data_type().clone(), true));
            let binding = TypedValueBinding::try_new(value, layout).unwrap();
            let slot = ChunkSlotSchema::try_new_typed(
                SlotId::new(74),
                field,
                binding,
                None,
                storage.clone(),
            )
            .unwrap();
            let expected = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap());
            for marker in [Some("hll"), Some("unknown-domain"), None, Some("bitmap")] {
                let source = nested(kind, marker);
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![source.clone()])),
                    vec![new_null_array(source.data_type(), 1)],
                )
                .unwrap();
                let mut arrow = Vec::new();
                let mut writer =
                    arrow::ipc::writer::StreamWriter::try_new(&mut arrow, batch.schema().as_ref())
                        .unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
                drop(writer);
                let bytes = encode_exchange_payload_envelope(
                    &arrow,
                    Some(&ExchangeWireMeta {
                        slot_ids_by_index: vec![SlotId::new(73)],
                    }),
                )
                .expect("exchange envelope fixture");
                let decoded = decode_root_result_chunks(&bytes, &expected).unwrap();
                assert_eq!(decoded[0].len(), 1);
                assert_eq!(
                    decoded[0].columns()[0].data_type(),
                    expected.slots()[0].data_type()
                );
                assert_eq!(decoded[0].chunk_schema().slot_ids(), &[SlotId::new(74)]);
                assert_eq!(
                    decoded[0].chunk_schema().slots()[0].typed_binding(),
                    expected.slots()[0].typed_binding()
                );
            }
        }
    }

    #[test]
    fn m07_root_ipc_preserves_large_string_dictionary_without_changing_exchange_admission() {
        use arrow::array::{DictionaryArray, Int32Array, LargeStringArray};
        use arrow::datatypes::Int32Type;
        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), None, Some(0)]),
                Arc::new(LargeStringArray::from(vec!["{}", "[]"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("wire", array.data_type().clone(), true);
        let chunk = Chunk::try_new_with_columns(
            Arc::new(
                crate::exec::chunk::ChunkSchema::try_new(vec![
                    crate::exec::chunk::ChunkSlotSchema::new_with_field(
                        SlotId::new(72),
                        field,
                        None,
                        None,
                    ),
                ])
                .unwrap(),
            ),
            vec![array],
        )
        .unwrap();
        let bytes = encode_chunks(&[chunk], true).unwrap();
        let expected = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(73),
                Field::new("status", DataType::LargeUtf8, true),
                None,
                None,
            )])
            .unwrap(),
        );
        let decoded = decode_root_result_chunks(&bytes, &plain_declaration(&expected)).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].len(), 3);
        assert_eq!(decoded[0].chunk_schema().slot_ids(), &[SlotId::new(73)]);
        assert_eq!(
            decoded[0].columns()[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8))
        );
        assert_eq!(
            decoded[0].record_batch().schema().field(0).data_type(),
            decoded[0].columns()[0].data_type()
        );
        let key = ExchangeKey {
            finst_id_hi: 1703,
            finst_id_lo: 1704,
            node_id: 78,
        };
        register_expected_chunk_schema(key, 1, logical_status_schema(SlotId::new(72))).unwrap();
        let error = decode_chunks_for_sender(key, 9, 1, &bytes).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from its frozen value layout"),
            "{error}"
        );
    }

    #[test]
    fn decode_chunks_for_sender_preserves_dictionary_carrier_with_logical_expected_schema() {
        let key = ExchangeKey {
            finst_id_hi: 1701,
            finst_id_lo: 1702,
            node_id: 77,
        };
        let chunk = dictionary_status_chunk(
            SlotId::new(72),
            vec![Some(0), Some(1), Some(0)],
            vec!["SHIPPED", "PAID"],
        );
        register_expected_chunk_schema(key, 1, logical_status_schema(SlotId::new(72)))
            .expect("register schema");

        let payload = encode_chunks(&[chunk], true).expect("encode dictionary payload");
        let decoded = decode_chunks_for_sender(key, 9, 1, &payload).expect("decode sender payload");

        assert_eq!(decoded.len(), 1);
        assert!(matches!(
            decoded[0].columns()[0].data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(
            logical_utf8_values(&decoded[0].columns()[0]),
            vec![
                Some("SHIPPED".to_string()),
                Some("PAID".to_string()),
                Some("SHIPPED".to_string()),
            ]
        );

        cancel_exchange_key(key);
    }

    fn assert_value_dictionary_refusal(error: &super::ExchangeFailure) {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_value_arrays::{ValueError, ValueViolation};
        assert_eq!(
            error.failure_fact(),
            ValueError::Invariant(ValueViolation::Dictionary).failure_fact()
        );
        let mut cause = Some(error as &dyn std::error::Error);
        while let Some(original) = cause {
            if let Some(value) = original.downcast_ref::<ValueError>() {
                assert!(matches!(
                    value,
                    ValueError::Invariant(ValueViolation::Dictionary)
                ));
                return;
            }
            cause = original.source();
        }
        panic!("exchange refusal lost its original value error");
    }

    #[test]
    fn encode_chunks_rejects_dictionary_with_non_int32_key() {
        let dict = arrow::array::DictionaryArray::<arrow::datatypes::Int8Type>::new(
            arrow::array::Int8Array::from(vec![Some(0_i8), Some(1_i8)]),
            Arc::new(StringArray::from(vec!["PAID", "NEW"])),
        );
        let data_type = dict.data_type().clone();
        let chunk =
            dictionary_chunk_with_type(SlotId::new(73), data_type, Arc::new(dict) as ArrayRef);

        let err = encode_chunks(&[chunk], true).expect_err("unsupported key type");

        assert_value_dictionary_refusal(&err);
    }

    #[test]
    fn encode_chunks_rejects_dictionary_with_non_string_values() {
        let dict = DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(0), Some(1)]),
            Arc::new(Int32Array::from(vec![10, 20])),
        );
        let data_type = dict.data_type().clone();
        let chunk =
            dictionary_chunk_with_type(SlotId::new(74), data_type, Arc::new(dict) as ArrayRef);

        let err = encode_chunks(&[chunk], true).expect_err("unsupported value type");

        assert_value_dictionary_refusal(&err);
    }

    #[test]
    fn encode_chunks_normalizes_field_name_only_schema_differences() {
        let chunks = vec![exchange_test_chunk("_cse_0"), exchange_test_chunk("_cse_2")];
        let bytes = encode_chunks(&chunks, true).expect("encode");
        let decoded = decode_chunks(&bytes).expect("decode");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].schema(), decoded[1].schema());
        assert_eq!(decoded[0].schema().field(3).name(), "_cse_0");
    }

    #[test]
    fn encode_chunks_round_trip_zero_column_row_count() {
        let chunk = exchange_test_zero_column_chunk(1);
        let bytes = encode_chunks(&[chunk], true).expect("encode");
        let decoded = decode_chunks(&bytes).expect("decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].len(), 1);
        assert_eq!(decoded[0].record_batch().num_columns(), 0);
    }

    #[test]
    fn encode_chunks_normalizes_runtime_nullable_widening() {
        let key = ExchangeKey {
            finst_id_hi: 299,
            finst_id_lo: 300,
            node_id: 27,
        };
        let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
        register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

        let strict = exchange_test_chunk("_cse_0");
        let schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int32, true),
            Field::new("v2", DataType::Int32, true),
            Field::new("k1", DataType::Int32, true),
            Field::new("_cse_0", DataType::Utf8, true),
        ]));
        let widened =
            RecordBatch::try_new(schema, strict.record_batch().columns().to_vec()).expect("batch");
        let widened_chunk_schema = Arc::new(
            crate::exec::chunk::ChunkSchema::try_new(
                widened
                    .schema()
                    .fields()
                    .iter()
                    .zip(strict.chunk_schema().slots().iter())
                    .map(|(field, slot)| slot.with_field(field.as_ref().clone()))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("slot schemas"),
            )
            .expect("chunk schema"),
        );
        let widened =
            Chunk::try_new_with_chunk_schema(widened, widened_chunk_schema).expect("chunk");

        let bytes = encode_chunks(&[strict, widened], true).expect("encode");
        let decoded = decode_chunks_for_sender(key, 3, 1, &bytes).expect("decode widened payload");

        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].schema().field(3).is_nullable());
        assert!(decoded[1].schema().field(3).is_nullable());

        cancel_exchange_key(key);
    }

    #[test]
    fn encode_chunks_preserves_decimal128_values_exceeding_declared_precision() {
        // Descriptor-authoritative exchange (pillar P3): the sender writes its
        // OWN type verbatim — it no longer widens decimal precision to 38. The
        // i128 payload is preserved regardless of the precision tag. With no
        // registered receiver descriptor (`decode_chunks`), the decoded type is
        // the sender's wire type; a registered descriptor must match exactly.
        let value = 100_000_000_000_000_000_000_i128;
        let chunk = decimal128_chunk_with_over_precision_value(value);

        let bytes = encode_chunks(&[chunk], true).expect("encode");
        let decoded = decode_chunks(&bytes).expect("decode");

        assert_eq!(decoded.len(), 1);
        let array = decoded[0]
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128 array");
        assert_eq!(array.data_type(), &DataType::Decimal128(20, 2));
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), value);
    }

    #[test]
    fn decode_root_result_chunks_rejects_decimal_precision_drift() {
        let value = 100_000_000_000_000_000_000_i128;
        let chunk = decimal128_chunk_with_over_precision_value(value);
        let payload = encode_chunks(&[chunk], true).expect("encode");
        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![decimal_slot_schema(
                SlotId::new(55),
                "price",
                true,
                38,
                2,
                None,
            )])
            .expect("expected chunk schema"),
        );

        let err = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
            .expect_err("root result decode must reject decimal precision drift");

        assert!(err.to_string().contains("frozen value layout"), "err={err}");
        assert!(err.to_string().contains("column 0"), "err={err}");
        assert!(err.to_string().contains("Decimal128(38, 2)"), "err={err}");
        assert!(err.to_string().contains("Decimal128(20, 2)"), "err={err}");
    }

    #[test]
    fn decode_chunks_for_sender_rejects_decimal_precision_drift() {
        let key = ExchangeKey {
            finst_id_hi: 601,
            finst_id_lo: 602,
            node_id: 33,
        };
        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![decimal_slot_schema(
                SlotId::new(55),
                "price",
                true,
                38,
                2,
                None,
            )])
            .expect("expected chunk schema"),
        );
        register_expected_chunk_schema(key, 1, expected_schema).expect("register expected schema");

        let chunk = decimal128_chunk_with_over_precision_value(1234_i128);
        let payload = encode_chunks(&[chunk], true).expect("encode drifted decimal chunk");

        let err = decode_chunks_for_sender(key, 3, 1, &payload)
            .expect_err("exchange decode must reject decimal precision drift");

        assert!(err.to_string().contains("frozen value layout"), "err={err}");
        assert!(err.to_string().contains("column 0"), "err={err}");
        assert!(err.to_string().contains("Decimal128(38, 2)"), "err={err}");
        assert!(err.to_string().contains("Decimal128(20, 2)"), "err={err}");
        cancel_exchange_key(key);
    }

    #[test]
    fn decode_root_result_chunks_rejects_utf8_binary_type_drift() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("abc")])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(56)],
        )
        .expect("chunk schema");
        let payload = encode_chunks(&[Chunk::new_with_chunk_schema(batch, chunk_schema)], true)
            .expect("encode");
        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(56),
                Field::new("payload", DataType::Binary, true),
                None,
                None,
            )])
            .expect("expected chunk schema"),
        );

        let err = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
            .expect_err("root result decode must reject Utf8/Binary drift");

        assert!(err.to_string().contains("frozen value layout"), "err={err}");
        assert!(err.to_string().contains("column 0"), "err={err}");
        assert!(err.to_string().contains("Binary"), "err={err}");
        assert!(err.to_string().contains("Utf8"), "err={err}");
    }

    #[test]
    fn decode_root_result_chunks_preserves_exact_decimal_descriptor_type() {
        let value = 1234_i128;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Decimal128(38, 2),
            true,
        )]));
        let array = Decimal128Array::from(vec![Some(value)])
            .with_precision_and_scale(38, 2)
            .expect("decimal array");
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(55)],
        )
        .expect("chunk schema");
        let payload = encode_chunks(&[Chunk::new_with_chunk_schema(batch, chunk_schema)], true)
            .expect("encode");
        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![decimal_slot_schema(
                SlotId::new(55),
                "price",
                true,
                38,
                2,
                None,
            )])
            .expect("expected chunk schema"),
        );

        let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
            .expect("decode root");

        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].record_batch().schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
        assert_eq!(
            decoded[0].chunk_schema().slots()[0].slot_id(),
            SlotId::new(55)
        );
    }

    #[test]
    fn decode_root_result_chunks_uses_expected_schema_positionally() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("file", DataType::Utf8, false),
                Field::new("row_id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a.parquet"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
            ],
        )
        .expect("record batch");
        let wire_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(3), SlotId::new(1), SlotId::new(2)],
        )
        .expect("wire chunk schema");
        let payload = encode_chunks(&[Chunk::new_with_chunk_schema(batch, wire_schema)], true)
            .expect("encode root result");
        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("file", DataType::Utf8, false),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    SlotId::new(2),
                    Field::new("row_id", DataType::Int64, false),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    SlotId::new(3),
                    Field::new("value", DataType::Utf8, true),
                    None,
                    None,
                ),
            ])
            .expect("expected root schema"),
        );

        let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
            .expect("decode root");

        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(decoded[0].record_batch().schema().field(0).name(), "file");
        assert_eq!(decoded[0].record_batch().schema().field(1).name(), "row_id");
        assert_eq!(decoded[0].record_batch().schema().field(2).name(), "value");
    }

    #[test]
    fn native_exchange_publication_retains_descriptor_metadata_ownership() {
        use crate::exec::chunk::{RootArrayStorageLimits, borrowed_root_chunk_storage};
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let owner = ArrowMetadataOwner::try_new(
            Vec::new(),
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .unwrap()
        .into_field("value".into(), DataType::Int64, false);
        let expected = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::try_new_with_metadata_origins(
                    SlotId::new(73),
                    Arc::clone(owner.field()),
                    FieldMetadataOrigins::try_new(vec![owner], 1).unwrap(),
                    None,
                    None,
                )
                .unwrap(),
            ])
            .unwrap(),
        );
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "wire_value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();
        let unknown = ChunkSchema::try_ref_from_schema_and_slot_ids(
            source.schema().as_ref(),
            &[SlotId::new(5)],
        )
        .unwrap();
        let original = Chunk::new_with_chunk_schema(source, unknown);
        let limits = RootArrayStorageLimits {
            bytes: 96 * 1024 * 1024,
            nodes: 65536,
            depth: 64,
        };
        assert!(borrowed_root_chunk_storage(&original, limits).is_err());
        let payload = encode_chunks(&[original], true).unwrap();
        let key = ExchangeKey {
            finst_id_hi: 91_735,
            finst_id_lo: 91_736,
            node_id: 73,
        };
        let expected = plain_declaration(&expected);
        let registry = ExecutionExchangeRegistry::default();
        registry
            .register_expected_chunk_schema(
                key,
                1,
                Arc::clone(&expected),
                crate::runtime::fragment::exchange::test_receiver_creation_factory(),
            )
            .unwrap();
        let decoded = registry
            .decode_chunks_for_sender(key, ExchangeSenderIdentity::local(3, 1), &payload)
            .unwrap();
        assert_eq!(decoded[0].chunk_schema().slot_ids(), &[SlotId::new(73)]);
        assert_eq!(
            decoded[0]
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert!(borrowed_root_chunk_storage(&decoded[0], limits).is_ok());
        assert!(Arc::ptr_eq(
            decoded[0].chunk_schema().slots()[0].field_ref(),
            expected.slots()[0].field_ref()
        ));
    }

    #[test]
    fn decode_root_result_chunks_accepts_nested_map_field_metadata_drift() {
        let source_entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, true),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as ArrayRef,
            ],
            None,
        );
        let source_entries_field = Arc::new(Field::new(
            "entries",
            source_entries.data_type().clone(),
            false,
        ));
        let map = Arc::new(MapArray::new(
            source_entries_field.clone(),
            OffsetBuffer::new(vec![0, 2].into()),
            source_entries,
            None,
            false,
        )) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "m",
                DataType::Map(source_entries_field, false),
                true,
            )])),
            vec![map],
        )
        .expect("record batch");
        let wire_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(5)],
        )
        .expect("wire chunk schema");
        let payload = encode_chunks(&[Chunk::new_with_chunk_schema(batch, wire_schema)], true)
            .expect("encode");

        let key_field = Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "12".to_string(),
        )]));
        let value_field = Field::new("value", DataType::Int32, true).with_metadata(HashMap::from(
            [("PARQUET:field_id".to_string(), "13".to_string())],
        ));
        let target_entries = Arc::new(Field::new(
            "key_value",
            DataType::Struct(Fields::from(vec![key_field, value_field])),
            false,
        ));
        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(5),
                Field::new("m", DataType::Map(target_entries, false), true),
                None,
                None,
            )])
            .expect("expected chunk schema"),
        );

        let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
            .expect("decode map field metadata drift");

        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].chunk_schema().slots()[0].slot_id(),
            SlotId::new(5)
        );
        let decoded_schema = decoded[0].record_batch().schema();
        let DataType::Map(entries, false) = decoded_schema.field(0).data_type() else {
            panic!("expected map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries struct");
        };
        assert!(
            !fields[0].is_nullable(),
            "Map keys retain the frozen non-NULL contract"
        );
        assert_eq!(decoded[0].record_batch().column(0).len(), 1);
    }

    #[test]
    fn decode_root_result_chunks_preserves_zero_column_row_count() {
        let chunk = exchange_test_zero_column_chunk(3);
        let payload = encode_chunks(&[chunk], true).expect("encode");
        let expected_schema = Arc::new(ChunkSchema::empty());

        let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
            .expect("decode root");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].len(), 3);
        assert_eq!(decoded[0].record_batch().num_columns(), 0);
    }

    #[test]
    fn encode_chunks_rejects_slot_id_mismatch() {
        let first = exchange_test_chunk("_cse_0");
        let schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int32, true),
            Field::new("v2", DataType::Int32, true),
            Field::new("k1", DataType::Int32, true),
            Field::new("_cse_2", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(Int32Array::from(vec![Some(2)])),
            Arc::new(Int32Array::from(vec![Some(3)])),
            Arc::new(StringArray::from(vec![Some("x")])),
        ];
        let second = {
            let batch = RecordBatch::try_new(schema, columns).expect("record batch");
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[
                    SlotId::new(33),
                    SlotId::new(34),
                    SlotId::new(31),
                    SlotId::new(99),
                ],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let err =
            encode_chunks(&[first, second], true).expect_err("should reject mismatched slot id");
        assert!(err.to_string().contains("exchange encode schema mismatch"));
    }

    #[test]
    fn decode_chunks_for_sender_uses_slot_id_map_without_field_metadata() {
        let key = ExchangeKey {
            finst_id_hi: 99,
            finst_id_lo: 100,
            node_id: 7,
        };

        let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
        register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

        let first = exchange_test_chunk_without_metadata("_cse_0");
        let first_bytes = encode_chunks(&[first], true).expect("encode first");
        let first_decoded =
            decode_chunks_for_sender(key, 3, 1, &first_bytes).expect("decode first");
        assert_eq!(first_decoded.len(), 1);
        assert_eq!(
            first_decoded[0].chunk_schema().slots()[3].slot_id(),
            SlotId::new(32)
        );
        assert_eq!(first_decoded[0].schema().field(3).name(), "_cse_0");

        let second = exchange_test_chunk_without_metadata("_cse_2");
        let second_bytes = encode_chunks(&[second], false).expect("encode second");
        let second_decoded =
            decode_chunks_for_sender(key, 3, 1, &second_bytes).expect("decode second");
        assert_eq!(second_decoded.len(), 1);
        assert_eq!(
            second_decoded[0].chunk_schema().slots()[3].slot_id(),
            SlotId::new(32)
        );
        assert_eq!(second_decoded[0].schema().field(3).name(), "_cse_0");

        cancel_exchange_key(key);
    }

    #[test]
    fn decode_chunks_for_sender_rejects_partial_expected_schema_match() {
        let key = ExchangeKey {
            finst_id_hi: 1099,
            finst_id_lo: 1100,
            node_id: 77,
        };
        let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
        register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

        let schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(33)],
        )
        .expect("chunk schema");
        let payload = encode_chunks(&[Chunk::new_with_chunk_schema(batch, chunk_schema)], true)
            .expect("encode");

        let err = decode_chunks_for_sender(key, 3, 1, &payload)
            .expect_err("partial wire schema must not match registered expected schema");
        assert!(err.to_string().contains("wire column count"), "err={err}");

        cancel_exchange_key(key);
    }

    #[test]
    fn decode_chunks_for_sender_accepts_runtime_nullable_widening() {
        let key = ExchangeKey {
            finst_id_hi: 199,
            finst_id_lo: 200,
            node_id: 17,
        };
        let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
        register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

        let strict = exchange_test_chunk_without_metadata("_cse_0");
        let widened_schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int32, true),
            Field::new("v2", DataType::Int32, true),
            Field::new("k1", DataType::Int32, true),
            Field::new("_cse_0", DataType::Utf8, true),
        ]));
        let widened =
            RecordBatch::try_new(widened_schema, strict.record_batch().columns().to_vec())
                .expect("batch");
        let widened_chunk_schema = Arc::new(
            crate::exec::chunk::ChunkSchema::try_new(
                widened
                    .schema()
                    .fields()
                    .iter()
                    .zip(strict.chunk_schema().slots().iter())
                    .map(|(field, slot)| slot.with_field(field.as_ref().clone()))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("slot schemas"),
            )
            .expect("chunk schema"),
        );
        let widened =
            Chunk::try_new_with_chunk_schema(widened, widened_chunk_schema).expect("chunk");

        let payload = encode_chunks(&[widened], true).expect("encode");
        let decoded = decode_chunks_for_sender(key, 3, 1, &payload).expect("decode widened chunk");

        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].schema().field(3).is_nullable());
        assert!(decoded[0].chunk_schema().slots()[3].nullable());

        cancel_exchange_key(key);
    }

    #[test]
    fn decode_chunks_for_sender_rejects_numeric_for_opaque_descriptor() {
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};

        let key = ExchangeKey {
            finst_id_hi: 501,
            finst_id_lo: 502,
            node_id: 31,
        };

        let wire_schema = Arc::new(Schema::new(vec![Field::new(
            "__opaque_state",
            DataType::Int64,
            true,
        )]));
        let wire_batch = RecordBatch::try_new(
            wire_schema,
            vec![Arc::new(Int64Array::from(vec![Some(7_i64)])) as ArrayRef],
        )
        .expect("wire batch");
        let wire_chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            wire_batch.schema().as_ref(),
            &[SlotId::new(41)],
        )
        .expect("wire chunk schema");
        let wire_chunk = Chunk::new_with_chunk_schema(wire_batch, wire_chunk_schema);

        let expected_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(41),
                Field::new("__opaque_state", DataType::Binary, true),
                None,
                Some(41),
            )])
            .expect("expected chunk schema"),
        );
        register_expected_chunk_schema(key, 1, expected_schema).expect("register expected schema");

        let payload = encode_chunks(&[wire_chunk], true).expect("encode drifted chunk");
        let err = decode_chunks_for_sender(key, 3, 1, &payload)
            .expect_err("numeric payload must not pass as an opaque aggregate state");
        assert!(err.to_string().contains("frozen value layout"), "err={err}");

        cancel_exchange_key(key);
    }

    #[test]
    fn decode_chunks_for_sender_decodes_with_wire_meta_before_schema_registration() {
        let key = ExchangeKey {
            finst_id_hi: 201,
            finst_id_lo: 202,
            node_id: 19,
        };
        let payload = encode_chunks(&[exchange_test_chunk("_cse_0")], true).expect("encode");
        assert!(decode_chunks_for_sender(key, 7, 1, &payload).is_err());
        register_expected_chunk_schema(
            key,
            1,
            plain_declaration(exchange_test_chunk("_cse_0").chunk_schema()),
        )
        .unwrap();
        let decoded =
            decode_chunks_for_sender(key, 7, 1, &payload).expect("decode after registration");
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].chunk_schema().slots()[3].slot_id(),
            SlotId::new(32)
        );
        assert_eq!(decoded[0].schema().field(3).name(), "_cse_0");

        cancel_exchange_key(key);
    }

    #[test]
    fn installed_expected_schema_retags_queued_wire_chunks() {
        let key = ExchangeKey {
            finst_id_hi: 203,
            finst_id_lo: 204,
            node_id: 21,
        };
        let payload = encode_chunks(&[exchange_test_chunk("_cse_0")], true).expect("encode");
        assert!(decode_chunks_for_sender(key, 7, 1, &payload).is_err());

        let expected_slots = [
            SlotId::new(133),
            SlotId::new(134),
            SlotId::new(131),
            SlotId::new(132),
        ];
        let expected_chunk = exchange_test_chunk("_cse_0");
        let expected_chunk_schema =
            crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                expected_chunk.record_batch().schema().as_ref(),
                &expected_slots,
            )
            .expect("expected chunk schema");
        register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

        let decoded = decode_chunks_for_sender(key, 7, 1, &payload)
            .expect("decode after installed declaration");
        push_chunks(key, 7, 1, decoded, false);

        let handle = get_receiver_handle(key, 1).expect("receiver handle");
        let Some(ExchangePopResult::Chunk(chunk)) =
            handle.try_pop_next_with_stats(1).expect("pop queued chunk")
        else {
            panic!("expected queued chunk");
        };
        let actual_slots = chunk
            .chunk_schema()
            .slots()
            .iter()
            .map(|slot| slot.slot_id())
            .collect::<Vec<_>>();
        assert_eq!(actual_slots, expected_slots);

        cancel_exchange_key(key);
    }

    #[test]
    fn installed_expected_schema_preserves_queued_zero_column_row_count() {
        let key = ExchangeKey {
            finst_id_hi: 205,
            finst_id_lo: 206,
            node_id: 23,
        };
        let row_count = 3;
        let payload = encode_chunks(&[exchange_test_zero_column_chunk(row_count)], true)
            .expect("encode zero-column chunk");
        assert!(decode_chunks_for_sender(key, 7, 1, &payload).is_err());

        register_expected_chunk_schema(key, 1, Arc::new(crate::exec::chunk::ChunkSchema::empty()))
            .expect("register empty schema");

        let decoded = decode_chunks_for_sender(key, 7, 1, &payload)
            .expect("decode after installed declaration");
        push_chunks(key, 7, 1, decoded, false);

        let handle = get_receiver_handle(key, 1).expect("receiver handle");
        let Some(ExchangePopResult::Chunk(chunk)) =
            handle.try_pop_next_with_stats(1).expect("pop queued chunk")
        else {
            panic!("expected queued zero-column chunk");
        };
        assert_eq!(chunk.len(), row_count);
        assert_eq!(chunk.record_batch().num_columns(), 0);

        cancel_exchange_key(key);
    }

    #[test]
    fn eos_completion_counts_unique_sender_instances() {
        let key = ExchangeKey {
            finst_id_hi: 401,
            finst_id_lo: 402,
            node_id: 29,
        };

        set_expected_senders(key, 2);
        push_chunks(key, 7, 11, Vec::new(), true);
        let snapshot = snapshot_receiver_state(key).expect("receiver snapshot after first eos");
        assert_eq!(snapshot.expected_senders, 2);
        assert_eq!(snapshot.finished_senders, 1);

        push_chunks(key, 7, 11, Vec::new(), true);
        let snapshot =
            snapshot_receiver_state(key).expect("receiver snapshot after duplicate sender eos");
        assert_eq!(snapshot.finished_senders, 1);

        push_chunks(key, 8, 11, Vec::new(), true);
        let snapshot = snapshot_receiver_state(key).expect("receiver snapshot after second sender");
        assert_eq!(snapshot.finished_senders, 2);

        cancel_exchange_key(key);
    }

    #[test]
    fn native_senders_keep_independent_wire_metadata_by_exact_identity() {
        let key = ExchangeKey {
            finst_id_hi: 501,
            finst_id_lo: 502,
            node_id: 31,
        };
        let registry = ExecutionExchangeRegistry::default();
        let first = exchange_test_chunk("_cse_0");
        let second_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            first.record_batch().schema().as_ref(),
            &[
                SlotId::new(133),
                SlotId::new(134),
                SlotId::new(131),
                SlotId::new(132),
            ],
        )
        .expect("second sender schema");
        let second = Chunk::new_with_chunk_schema(first.record_batch().clone(), second_schema);
        registry
            .register_expected_chunk_schema(
                key,
                2,
                plain_declaration(first.chunk_schema()),
                crate::runtime::fragment::exchange::test_receiver_creation_factory(),
            )
            .unwrap();
        let first_payload = encode_chunks(&[first], true).expect("first sender payload");
        let second_payload = encode_chunks(&[second], true).expect("second sender payload");
        let first_sender = ExchangeSenderIdentity::native(UniqueId::new(1, 1), 0);
        let second_sender = ExchangeSenderIdentity::native(UniqueId::new(2, 2), 1);

        registry
            .decode_chunks_for_sender(key, first_sender, &first_payload)
            .expect("first sender establishes its own wire metadata");
        registry
            .decode_chunks_for_sender(key, second_sender, &second_payload)
            .expect("second sender may carry different wire metadata");
        assert!(
            registry
                .decode_chunks_for_sender(key, first_sender, &second_payload)
                .expect_err("one exact sender cannot change its wire metadata")
                .to_string()
                .contains("sender wire meta changed unexpectedly")
        );
    }

    // Compiled fragments allocate slot ids per fragment. The sender projects
    // (b, a) from its own slots [1, 0]; the receiver imports (b', a') into its
    // slots [0, 1]. Every wire id exists in the receiver schema with the same
    // type, so a slot-id binding silently swaps the two columns.
    #[test]
    fn positional_receiver_binds_a_reversed_slot_sender_by_wire_position() {
        use super::ExchangeColumnBinding;
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };

        let sender_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
        ]));
        let sender_batch = RecordBatch::try_new(
            Arc::clone(&sender_schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .expect("sender batch");
        let sender_chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            sender_schema.as_ref(),
            &[SlotId::new(1), SlotId::new(0)],
        )
        .expect("sender chunk schema");
        let payload = encode_chunks(
            &[Chunk::new_with_chunk_schema(
                sender_batch,
                sender_chunk_schema,
            )],
            true,
        )
        .expect("encode sender payload");
        let receiver_schema = Schema::new(vec![
            Field::new("b_import", DataType::Int64, false),
            Field::new("a_import", DataType::Int64, false),
        ]);
        let storage = ValueTypeStorageLifetime::pure();
        let slots = receiver_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                let value = ValueTypeRef::try_new(
                    ValueKind::Scalar(DataType::Int64),
                    false,
                    storage.clone(),
                )
                .unwrap();
                let binding = TypedValueBinding::try_new(
                    value,
                    ValueLayout::try_new(&DataType::Int64, storage.clone()).unwrap(),
                )
                .unwrap();
                ChunkSlotSchema::try_new_typed(
                    SlotId::new(ordinal as u32),
                    Arc::clone(field),
                    binding,
                    None,
                    storage.clone(),
                )
                .unwrap()
            })
            .collect();
        let expected = Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap());
        let sender = ExchangeSenderIdentity::native(UniqueId::new(1, 1), 0);
        let column = |chunk: &Chunk, slot: u32| -> Vec<i64> {
            chunk
                .column_by_slot_id(SlotId::new(slot))
                .expect("slot column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .values()
                .to_vec()
        };
        let registry = ExecutionExchangeRegistry::default();

        let positional = ExchangeKey {
            finst_id_hi: 601,
            finst_id_lo: 602,
            node_id: 3,
        };
        registry
            .try_register_expected_chunk_schema_with_binding(
                positional,
                1,
                Arc::clone(&expected),
                ExchangeColumnBinding::Positional,
                crate::runtime::fragment::exchange::test_receiver_creation_factory(),
            )
            .expect("register positional receiver");
        let decoded = registry
            .decode_chunks_for_sender(positional, sender, &payload)
            .expect("positional decode");
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].chunk_schema().is_typed());
        assert!(Arc::ptr_eq(&decoded[0].chunk_schema_ref(), &expected));
        assert_eq!(
            decoded[0]
                .chunk_schema()
                .slots()
                .iter()
                .map(|slot| slot.slot_id())
                .collect::<Vec<_>>(),
            vec![SlotId::new(0), SlotId::new(1)]
        );
        assert_eq!(column(&decoded[0], 0), vec![10, 20]);
        assert_eq!(column(&decoded[0], 1), vec![1, 2]);

        // A chunk queued before registration keeps its wire ids; the late
        // positional registration retags it by position as well.
        let late = ExchangeKey {
            node_id: 5,
            ..positional
        };
        assert!(
            registry
                .decode_chunks_for_sender(late, sender, &payload)
                .is_err()
        );
        registry
            .try_register_expected_chunk_schema_with_binding(
                late,
                1,
                Arc::clone(&expected),
                ExchangeColumnBinding::Positional,
                crate::runtime::fragment::exchange::test_receiver_creation_factory(),
            )
            .expect("late positional registration");
        let queued = registry
            .decode_chunks_for_sender(late, sender, &payload)
            .expect("decode installed positional receiver");
        registry.push_chunks(late, sender, queued, false);
        let handle = registry
            .get_receiver_handle(late, 1)
            .expect("receiver handle");
        let Some(ExchangePopResult::Chunk(retagged)) =
            handle.try_pop_next_with_stats(1).expect("pop queued chunk")
        else {
            panic!("expected the queued chunk");
        };
        assert_eq!(column(&retagged, 0), vec![10, 20]);
        assert_eq!(column(&retagged, 1), vec![1, 2]);
        assert!(retagged.chunk_schema().is_typed());
        assert!(Arc::ptr_eq(&retagged.chunk_schema_ref(), &expected));

        // The same payload under the legacy slot-id binding is exactly the
        // hazard the positional registration exists to avoid.
        let by_slot = ExchangeKey {
            node_id: 4,
            ..positional
        };
        registry
            .try_register_expected_chunk_schema(
                by_slot,
                1,
                expected.clone(),
                crate::runtime::fragment::exchange::test_receiver_creation_factory(),
            )
            .expect("register slot-id receiver");
        let swapped = registry
            .decode_chunks_for_sender(by_slot, sender, &payload)
            .expect("slot-id decode");
        assert_eq!(column(&swapped[0], 0), vec![1, 2]);
        assert_eq!(column(&swapped[0], 1), vec![10, 20]);

        // One receiver has one binding; a conflicting re-registration fails.
        assert!(
            registry
                .register_expected_chunk_schema(
                    positional,
                    1,
                    expected,
                    crate::runtime::fragment::exchange::test_receiver_creation_factory(),
                )
                .expect_err("binding conflict")
                .to_string()
                .contains("exchange column binding mismatch")
        );
    }

    fn native_stream_fixture() -> (
        ExecutionExchangeRegistry,
        ExchangeKey,
        crate::runtime::fragment::io::ExchangeReceiverFrame,
        Chunk,
    ) {
        let registry = ExecutionExchangeRegistry::default();
        let key = ExchangeKey {
            finst_id_hi: 88001,
            finst_id_lo: 88002,
            node_id: 3,
        };
        let chunk = exchange_test_chunk("k2");
        registry
            .register_expected_chunk_schema(
                key,
                1,
                plain_declaration(&chunk.chunk_schema_ref()),
                crate::runtime::fragment::exchange::test_receiver_creation_factory(),
            )
            .unwrap();
        let source_kernel = UniqueId::new(88003, 88004);
        let stream = crate::runtime::fragment::io::exchange::fixture_stream(
            source_kernel,
            UniqueId::new(key.finst_id_hi, key.finst_id_lo),
            key.node_id,
            0,
        );
        let frame = crate::runtime::fragment::io::ExchangeReceiverFrame {
            stream,
            source_fragment_instance_id: source_kernel,
            sender_ordinal: 0,
            sender_count: 1,
            sender_id: 0,
            backend_number: 0,
            sequence: 0,
            eos: false,
            payload: encode_chunks(std::slice::from_ref(&chunk), true).unwrap(),
        };
        (registry, key, frame, chunk)
    }

    #[test]
    fn native_exchange_decode_failure_does_not_advance_and_gap_duplicate_after_end_fail() {
        let (registry, key, frame, _) = native_stream_fixture();
        let mut invalid = frame.clone();
        invalid.payload = vec![0xff];
        assert!(registry.admit_native_frame(key, invalid).is_err());
        assert_eq!(
            registry
                .receivers
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .mu
                .lock()
                .unwrap()
                .recv_request_count,
            0
        );
        let mut gap = frame.clone();
        gap.sequence = 1;
        assert!(registry.admit_native_frame(key, gap).is_err());
        registry.admit_native_frame(key, frame.clone()).unwrap();
        assert!(registry.admit_native_frame(key, frame.clone()).is_err());
        let mut end = frame.clone();
        end.sequence = 1;
        end.eos = true;
        end.payload.clear();
        registry.admit_native_frame(key, end.clone()).unwrap();
        assert!(registry.admit_native_frame(key, end).is_err());
        let mut after = frame;
        after.sequence = 2;
        assert!(registry.admit_native_frame(key, after).is_err());
        assert_eq!(
            registry
                .receivers
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .mu
                .lock()
                .unwrap()
                .recv_request_count,
            2
        );
    }

    #[test]
    fn native_exchange_consumed_requires_source_exhaustion_and_retains_final_data_end_sequence() {
        use super::{ExchangeCloseDisposition, ExchangeFrameSequence, SourcedExchangePopResult};
        let (registry, key, mut frame, chunk) = native_stream_fixture();
        frame.eos = true;
        let stream = frame.stream;
        registry.admit_native_frame(key, frame).unwrap();
        let handle = registry.get_receiver_handle(key, 1).unwrap();
        assert!(handle.has_output_or_finished(1));
        assert_eq!(
            registry.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Withdrawn
        );
        assert!(handle.complete_input_consumption(1).is_err());
        let Some(SourcedExchangePopResult::Chunk(data)) =
            handle.try_pop_sourced_with_stats(1).unwrap()
        else {
            panic!("source data is retained");
        };
        assert_eq!(data.source(), Some(stream.source()));
        assert_eq!(data.len(), chunk.len());
        assert!(matches!(
            handle.try_pop_sourced_with_stats(1).unwrap(),
            Some(SourcedExchangePopResult::Finished(_))
        ));
        // Even a non-consuming EOF peek cannot author a close fact.
        assert_eq!(
            registry.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Withdrawn
        );
        handle.complete_input_consumption(1).unwrap();
        assert_eq!(
            registry.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeFrameSequence::FIRST
            }
        );
    }

    #[test]
    fn native_exchange_empty_end_is_consumed_only_at_input_exhaustion() {
        use super::{ExchangeCloseDisposition, ExchangeFrameSequence};
        let (registry, key, mut frame, _) = native_stream_fixture();
        frame.eos = true;
        frame.payload.clear();
        let stream = frame.stream;
        registry.admit_native_frame(key, frame).unwrap();
        assert_eq!(
            registry.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Withdrawn
        );
        registry
            .get_receiver_handle(key, 1)
            .unwrap()
            .complete_input_consumption(1)
            .unwrap();
        assert_eq!(
            registry.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeFrameSequence::FIRST
            }
        );
    }

    #[test]
    fn native_exchange_concurrent_same_sequence_has_one_admission() {
        let (registry, key, frame, _) = native_stream_fixture();
        let registry = Arc::new(registry);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let registry = registry.clone();
                let barrier = barrier.clone();
                let frame = frame.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    registry.admit_native_frame(key, frame).is_ok()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        assert_eq!(
            handles
                .into_iter()
                .map(|h| usize::from(h.join().unwrap()))
                .sum::<usize>(),
            1
        );
        assert_eq!(
            registry
                .receivers
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .mu
                .lock()
                .unwrap()
                .recv_request_count,
            1
        );
    }
    #[test]
    fn native_exchange_one_stream_decoder_does_not_serialize_other_sources() {
        let (registry, key, mut first, _) = native_stream_fixture();
        // Re-register this explicit fixture with two actual frozen senders.
        registry.set_expected_senders(key, 2);
        first.sender_count = 2;
        registry.admit_native_frame(key, first.clone()).unwrap();
        let registry = Arc::new(registry);
        let fence = registry
            .receivers
            .lock()
            .unwrap()
            .get(&key)
            .unwrap()
            .mu
            .lock()
            .unwrap()
            .streams
            .get(&ExchangeSenderIdentity::native(
                first.source_fragment_instance_id,
                0,
            ))
            .unwrap()
            .admission
            .clone();
        let guard = fence.lock().unwrap();
        let mut second = first;
        second.sender_ordinal = 1;
        second.source_fragment_instance_id = UniqueId::new(88005, 88006);
        second.stream = crate::runtime::fragment::io::exchange::fixture_stream(
            second.source_fragment_instance_id,
            UniqueId::new(key.finst_id_hi, key.finst_id_lo),
            key.node_id,
            1,
        );
        let (sent, received) = std::sync::mpsc::channel();
        let worker_registry = registry.clone();
        let worker = std::thread::spawn(move || {
            sent.send(worker_registry.admit_native_frame(key, second))
                .unwrap();
        });
        let result = received.recv_timeout(std::time::Duration::from_secs(2));
        drop(guard);
        worker.join().unwrap();
        result
            .expect("another source progresses while one decoder is occupied")
            .unwrap();
        assert_eq!(
            registry.snapshot_receiver_state(key).unwrap().queued_chunks,
            2
        );
    }
}

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
