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
use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::common::config::exchange_wait_ms;
use crate::common::ids::SlotId;
use crate::common::types::format_uuid;
use crate::exec::chunk::{Chunk, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::novarocks_logging::debug;
use crate::runtime::mem_tracker::MemTracker;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExchangeKey {
    pub finst_id_hi: i64,
    pub finst_id_lo: i64,
    pub node_id: i32,
}

impl ExchangeKey {
    #[inline]
    pub(crate) fn finst_uuid(&self) -> String {
        format_uuid(self.finst_id_hi, self.finst_id_lo)
    }
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

static EXCHANGE_NOT_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);
static EXCHANGE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_not_ready() -> bool {
    EXCHANGE_NOT_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed) % EXCHANGE_NOT_READY_LOG_EVERY == 0
}

fn should_log_exchange_ready() -> bool {
    EXCHANGE_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed) % EXCHANGE_READY_LOG_EVERY == 0
}

static CANCELED_KEYS: OnceLock<Mutex<HashMap<ExchangeKey, Instant>>> = OnceLock::new();

fn canceled_keys() -> &'static Mutex<HashMap<ExchangeKey, Instant>> {
    CANCELED_KEYS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cleanup_canceled_keys_locked(keys: &mut HashMap<ExchangeKey, Instant>, now: Instant) {
    keys.retain(|_, ts| now.duration_since(*ts) <= CANCELED_KEYS_TTL);
    if keys.len() > CANCELED_KEYS_MAX_SIZE {
        keys.clear();
    }
}

fn mark_key_canceled(key: ExchangeKey) {
    let now = Instant::now();
    let mut guard = canceled_keys().lock().expect("exchange canceled keys lock");
    cleanup_canceled_keys_locked(&mut guard, now);
    guard.insert(key, now);
}

fn is_key_canceled(key: &ExchangeKey) -> bool {
    let now = Instant::now();
    let mut guard = canceled_keys().lock().expect("exchange canceled keys lock");
    cleanup_canceled_keys_locked(&mut guard, now);
    guard.contains_key(key)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExchangeWireMeta {
    slot_ids_by_index: Vec<SlotId>,
}

impl ExchangeWireMeta {
    fn from_chunks(chunks: &[Chunk]) -> Result<Option<Self>, String> {
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
                return Err(format!(
                    "exchange wire slot id mismatch at chunk index {}: expected={:?} actual={:?}",
                    idx, slot_ids_by_index, slot_ids
                ));
            }
        }
        Ok(Some(Self { slot_ids_by_index }))
    }
}

struct DecodedExchangePayload<'a> {
    wire_meta: Option<ExchangeWireMeta>,
    arrow_payload: &'a [u8],
}

#[derive(Default)]
struct ReceiverState {
    expected_senders: usize,
    expected_chunk_schema: Option<ChunkSchemaRef>,
    sender_wire_meta: HashMap<(i32, i32), ExchangeWireMeta>,
    finished: HashSet<(i32, i32)>, // (sender_id, be_number)
    chunks: VecDeque<Chunk>,
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

static EXCHANGE: OnceLock<Mutex<HashMap<ExchangeKey, Arc<Receiver>>>> = OnceLock::new();

fn exchange() -> &'static Mutex<HashMap<ExchangeKey, Arc<Receiver>>> {
    EXCHANGE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_or_create(key: ExchangeKey) -> Arc<Receiver> {
    use crate::novarocks_logging::debug;

    let mut guard = exchange().lock().expect("exchange lock");
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

pub fn cancel_fragment(finst_id_hi: i64, finst_id_lo: i64) {
    let mut guard = exchange().lock().expect("exchange lock");
    let keys: Vec<ExchangeKey> = guard
        .keys()
        .copied()
        .filter(|k| k.finst_id_hi == finst_id_hi && k.finst_id_lo == finst_id_lo)
        .collect();
    for k in keys {
        mark_key_canceled(k);
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

pub fn cancel_exchange_key(key: ExchangeKey) {
    mark_key_canceled(key);
    let r = {
        let guard = exchange().lock().expect("exchange lock");
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
    exchange().lock().expect("exchange lock").remove(&key);
}

pub fn set_expected_senders(key: ExchangeKey, expected_senders: usize) {
    use crate::novarocks_logging::debug;

    if is_key_canceled(&key) {
        return;
    }
    let r = get_or_create(key);
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

pub fn register_expected_chunk_schema(
    key: ExchangeKey,
    expected_senders: usize,
    chunk_schema: ChunkSchemaRef,
) -> Result<(), String> {
    if is_key_canceled(&key) {
        return Err("exchange canceled".to_string());
    }
    let receiver = get_or_create(key);
    let mut st = receiver.mu.lock().expect("exchange receiver lock");
    st.expected_senders = st.expected_senders.max(expected_senders);
    match st.expected_chunk_schema.as_ref() {
        Some(existing) if existing.as_ref() != chunk_schema.as_ref() => {
            return Err(format!(
                "exchange expected chunk schema mismatch: finst={} node_id={}",
                key.finst_uuid(),
                key.node_id
            ));
        }
        Some(_) => {}
        None => {
            st.expected_chunk_schema = Some(chunk_schema);
        }
    }
    receiver.cv.notify_all();
    Ok(())
}

pub fn ensure_receiver_mem_tracker(
    key: ExchangeKey,
    root: &Arc<MemTracker>,
) -> Result<Arc<MemTracker>, String> {
    if is_key_canceled(&key) {
        return Err("exchange canceled".to_string());
    }
    let receiver = get_or_create(key);
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
    key: ExchangeKey,
    sender_id: i32,
    be_number: i32,
    chunks: Vec<Chunk>,
    eos: bool,
) {
    push_chunks_with_stats(key, sender_id, be_number, chunks, eos, 0, 0);
}

pub fn push_chunks_with_stats(
    key: ExchangeKey,
    sender_id: i32,
    be_number: i32,
    mut chunks: Vec<Chunk>,
    eos: bool,
    payload_bytes: usize,
    deserialize_ns: u128,
) {
    use crate::novarocks_logging::debug;
    if is_key_canceled(&key) {
        return;
    }
    let chunks_len = chunks.len();
    let row_count: usize = chunks.iter().map(|c| c.len()).sum();
    let should_notify = chunks_len != 0 || eos;

    debug!(
        "push_chunks: finst={} node_id={} sender_id={} be_number={} chunks={} rows={} eos={}",
        key.finst_uuid(),
        key.node_id,
        sender_id,
        be_number,
        chunks_len,
        row_count,
        eos
    );

    let r = get_or_create(key);

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
            "push_chunks: CANCELED, dropping {} chunks ({} rows) from sender_id={}",
            chunks_len, row_count, sender_id
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
        st.chunks.extend(chunks);
    }
    if eos {
        st.finished.insert((sender_id, be_number));
        debug!(
            "push_chunks: sender_id={} marked as FINISHED, total finished={}/{}",
            sender_id,
            st.finished.len(),
            st.expected_senders
        );
    }
    r.cv.notify_all();
    drop(st);
    let hold_time = hold_start.elapsed();
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

pub fn take_all_chunks_blocking(
    key: ExchangeKey,
    expected_senders: usize,
    timeout: Duration,
) -> Result<Vec<Chunk>, String> {
    let (chunks, _stats) = take_all_chunks_with_stats_blocking(key, expected_senders, timeout)?;
    Ok(chunks)
}

#[derive(Clone, Debug, Default)]
pub struct ExchangeRecvStats {
    pub request_received: u128,
    pub bytes_received: u128,
    pub deserialize_ns: u128,
    pub chunks_received: u128,
    pub rows_received: u128,
}

pub fn take_all_chunks_with_stats_blocking(
    key: ExchangeKey,
    expected_senders: usize,
    timeout: Duration,
) -> Result<(Vec<Chunk>, ExchangeRecvStats), String> {
    use crate::novarocks_logging::debug;
    if is_key_canceled(&key) {
        return Err("exchange canceled".to_string());
    }

    debug!(
        "take_all_chunks_blocking START: finst={} node_id={} expected_senders={} timeout={:?}",
        key.finst_uuid(),
        key.node_id,
        expected_senders,
        timeout
    );

    set_expected_senders(key, expected_senders);
    let r = get_or_create(key);

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
            return Err("exchange canceled".to_string());
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
            return Err(format!(
                "exchange timeout waiting for senders: finst_id={} node_id={} expected={} finished={}",
                key.finst_uuid(),
                key.node_id,
                expected,
                st.finished.len()
            ));
        }
        let remain = timeout - elapsed;

        let wait_step = remain.min(EXCHANGE_WAIT_LOG_INTERVAL);
        let (next, wait_res) =
            r.cv.wait_timeout(st, wait_step)
                .map_err(|_| "exchange wait poisoned".to_string())?;
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

    let chunks: Vec<Chunk> = st.chunks.drain(..).collect();
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
    ) -> Result<Option<ExchangePopResult>, String> {
        use crate::novarocks_logging::debug;

        let r = &self.receiver;
        let mut st = r.mu.lock().expect("exchange receiver lock");
        if st.canceled {
            debug!(
                "exchange try_pop_next CANCELED: finst={} node_id={}",
                self.key.finst_uuid(),
                self.key.node_id
            );
            return Err("exchange canceled".to_string());
        }

        if let Some(chunk) = st.chunks.pop_front() {
            return Ok(Some(ExchangePopResult::Chunk(chunk)));
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
            return Ok(Some(ExchangePopResult::Finished(stats)));
        }

        Ok(None)
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
    ) -> Result<ExchangePopResult, String> {
        use crate::novarocks_logging::debug;

        let r = &self.receiver;
        let mut st = r.mu.lock().expect("exchange receiver lock");
        loop {
            if st.canceled {
                debug!(
                    "exchange pop_next CANCELED: finst={} node_id={}",
                    self.key.finst_uuid(),
                    self.key.node_id
                );
                return Err("exchange canceled".to_string());
            }

            if let Some(chunk) = st.chunks.pop_front() {
                return Ok(ExchangePopResult::Chunk(chunk));
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
                return Err(format!(
                    "exchange timeout waiting for senders: finst_id={} node_id={} expected={} finished={}",
                    self.key.finst_uuid(),
                    self.key.node_id,
                    expected,
                    st.finished.len()
                ));
            }
            let remain = timeout - elapsed;
            let wait_step = remain.min(EXCHANGE_WAIT_LOG_INTERVAL);
            let (next, wait_res) =
                r.cv.wait_timeout(st, wait_step)
                    .map_err(|_| "exchange wait poisoned".to_string())?;
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

pub fn get_receiver_handle(
    key: ExchangeKey,
    expected_senders: usize,
) -> Result<ExchangeReceiverHandle, String> {
    use crate::novarocks_logging::debug;

    if is_key_canceled(&key) {
        return Err("exchange canceled".to_string());
    }
    let receiver = get_or_create(key);
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

#[derive(Clone, Debug)]
pub struct ExchangeReceiverSnapshot {
    pub expected_senders: usize,
    pub finished_senders: usize,
    pub queued_chunks: usize,
    pub queued_rows: usize,
    pub canceled: bool,
}

pub fn snapshot_receiver_state(key: ExchangeKey) -> Option<ExchangeReceiverSnapshot> {
    let receiver = {
        let guard = exchange().lock().expect("exchange lock");
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

fn encode_exchange_payload_envelope(
    arrow_payload: &[u8],
    wire_meta: Option<&ExchangeWireMeta>,
) -> Vec<u8> {
    if arrow_payload.is_empty() {
        return Vec::new();
    }
    let slot_id_bytes = wire_meta
        .map(|meta| {
            meta.slot_ids_by_index
                .len()
                .saturating_mul(std::mem::size_of::<u32>())
        })
        .unwrap_or(0);
    let mut out = Vec::with_capacity(
        EXCHANGE_PAYLOAD_MAGIC.len() + 2 + 4 + slot_id_bytes + arrow_payload.len(),
    );
    out.extend_from_slice(EXCHANGE_PAYLOAD_MAGIC);
    out.push(EXCHANGE_PAYLOAD_VERSION);
    out.push(if wire_meta.is_some() {
        EXCHANGE_PAYLOAD_FLAG_SLOT_IDS
    } else {
        0
    });
    let slot_count = wire_meta
        .map(|meta| meta.slot_ids_by_index.len() as u32)
        .unwrap_or(0);
    out.extend_from_slice(&slot_count.to_le_bytes());
    if let Some(meta) = wire_meta {
        for slot_id in &meta.slot_ids_by_index {
            out.extend_from_slice(&slot_id.as_u32().to_le_bytes());
        }
    }
    out.extend_from_slice(arrow_payload);
    out
}

fn decode_exchange_payload_envelope(bytes: &[u8]) -> Result<DecodedExchangePayload<'_>, String> {
    if bytes.is_empty() {
        return Ok(DecodedExchangePayload {
            wire_meta: None,
            arrow_payload: bytes,
        });
    }
    if bytes.len() < EXCHANGE_PAYLOAD_MAGIC.len()
        || &bytes[..EXCHANGE_PAYLOAD_MAGIC.len()] != EXCHANGE_PAYLOAD_MAGIC
    {
        return Ok(DecodedExchangePayload {
            wire_meta: None,
            arrow_payload: bytes,
        });
    }

    let mut cursor = Cursor::new(&bytes[EXCHANGE_PAYLOAD_MAGIC.len()..]);
    let mut version = [0u8; 1];
    cursor
        .read_exact(&mut version)
        .map_err(|e| format!("failed to read exchange payload version: {e}"))?;
    if version[0] != EXCHANGE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported exchange payload version: expected={} actual={}",
            EXCHANGE_PAYLOAD_VERSION, version[0]
        ));
    }

    let mut flags = [0u8; 1];
    cursor
        .read_exact(&mut flags)
        .map_err(|e| format!("failed to read exchange payload flags: {e}"))?;

    let mut slot_count_bytes = [0u8; 4];
    cursor
        .read_exact(&mut slot_count_bytes)
        .map_err(|e| format!("failed to read exchange payload slot count: {e}"))?;
    let slot_count = u32::from_le_bytes(slot_count_bytes) as usize;

    let wire_meta = if flags[0] & EXCHANGE_PAYLOAD_FLAG_SLOT_IDS != 0 {
        let mut slot_ids_by_index = Vec::with_capacity(slot_count);
        for idx in 0..slot_count {
            let mut slot_bytes = [0u8; 4];
            cursor.read_exact(&mut slot_bytes).map_err(|e| {
                format!(
                    "failed to read exchange payload slot id at index {}: {e}",
                    idx
                )
            })?;
            slot_ids_by_index.push(SlotId::new(u32::from_le_bytes(slot_bytes)));
        }
        Some(ExchangeWireMeta { slot_ids_by_index })
    } else {
        if slot_count != 0 {
            return Err(format!(
                "exchange payload slot count must be zero without slot-id flag, got {}",
                slot_count
            ));
        }
        None
    };
    let payload_offset = EXCHANGE_PAYLOAD_MAGIC.len() + cursor.position() as usize;
    Ok(DecodedExchangePayload {
        wire_meta,
        arrow_payload: &bytes[payload_offset..],
    })
}

fn exchange_schema_compatible(expected: &Chunk, actual: &Chunk) -> Result<bool, String> {
    if expected.schema().fields().len() != actual.schema().fields().len() {
        return Ok(false);
    }
    if expected.chunk_schema().slots().len() != actual.chunk_schema().slots().len() {
        return Ok(false);
    }

    for ((expected_field, actual_field), (expected_slot, actual_slot)) in expected
        .schema()
        .fields()
        .iter()
        .zip(actual.schema().fields().iter())
        .zip(
            expected
                .chunk_schema()
                .slots()
                .iter()
                .zip(actual.chunk_schema().slots().iter()),
        )
    {
        if expected_slot.slot_id() != actual_slot.slot_id()
            || expected_field.data_type() != actual_field.data_type()
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn merged_exchange_schema(chunks: &[Chunk]) -> Result<SchemaRef, String> {
    let first = chunks
        .first()
        .ok_or_else(|| "exchange chunks must not be empty".to_string())?;
    let mut fields = first
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();

    for (chunk_idx, chunk) in chunks.iter().enumerate().skip(1) {
        if !exchange_schema_compatible(first, chunk)? {
            return Err(format!(
                "exchange encode schema mismatch at chunk index {}: expected={:?} actual={:?}",
                chunk_idx,
                first.schema(),
                chunk.schema()
            ));
        }
        for (field_idx, actual_field) in chunk.schema().fields().iter().enumerate() {
            let merged = fields.get_mut(field_idx).ok_or_else(|| {
                format!(
                    "exchange merged schema missing field {} for chunk index {}",
                    field_idx, chunk_idx
                )
            })?;
            if actual_field.is_nullable() && !merged.is_nullable() {
                *merged = merged.clone().with_nullable(true);
            }
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn encode_arrow_ipc_chunks(chunks: &[Chunk]) -> Result<Vec<u8>, String> {
    if chunks.is_empty() {
        return Ok(vec![]);
    }

    let mut buffer = Vec::new();
    let schema = merged_exchange_schema(chunks)?;
    let mut batches = Vec::with_capacity(chunks.len());
    for (i, chunk) in chunks.iter().enumerate() {
        if chunk.schema().as_ref() == schema.as_ref() {
            batches.push(chunk.batch.clone());
            continue;
        }
        let normalized = RecordBatch::try_new(Arc::clone(&schema), chunk.batch.columns().to_vec())
            .map_err(|e| {
                format!(
                    "failed to normalize exchange chunk schema at index {}: {e}",
                    i
                )
            })?;
        batches.push(normalized);
    }
    let mut writer = StreamWriter::try_new(&mut buffer, &schema)
        .map_err(|e| format!("failed to create Arrow IPC writer: {e}"))?;

    for batch in batches {
        writer
            .write(&batch)
            .map_err(|e| format!("failed to write batch: {e}"))?;
    }

    writer
        .finish()
        .map_err(|e| format!("failed to finish Arrow IPC writer: {e}"))?;

    Ok(buffer)
}

fn decode_arrow_ipc_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, String> {
    if bytes.is_empty() {
        return Ok(vec![]);
    }

    let mut cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| format!("failed to create Arrow IPC reader: {e}"))?;

    let mut batches = Vec::new();
    let mut expected_schema: Option<arrow::datatypes::SchemaRef> = None;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("failed to read batch: {e}"))?;
        if let Some(s) = expected_schema.as_ref() {
            if batch.schema().as_ref() != s.as_ref() {
                return Err(format!(
                    "exchange decode schema mismatch: expected={:?} actual={:?}",
                    s,
                    batch.schema()
                ));
            }
        } else {
            expected_schema = Some(batch.schema());
        }
        batches.push(batch);
    }

    Ok(batches)
}

fn chunk_schema_for_wire_meta(
    expected_chunk_schema: &ChunkSchemaRef,
    batch: &RecordBatch,
    wire_meta: &ExchangeWireMeta,
) -> Result<ChunkSchemaRef, String> {
    if wire_meta.slot_ids_by_index.len() != batch.num_columns() {
        return Err(format!(
            "exchange wire slot id count mismatch: batch_columns={} slot_ids={}",
            batch.num_columns(),
            wire_meta.slot_ids_by_index.len()
        ));
    }
    let mut slots = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for (idx, slot_id) in wire_meta.slot_ids_by_index.iter().enumerate() {
        let expected_slot = expected_chunk_schema.slot(*slot_id).ok_or_else(|| {
            format!(
                "exchange wire slot id {} not found in expected chunk schema at index {}",
                slot_id, idx
            )
        })?;
        let field = batch_schema.field(idx);
        if let Some(type_desc) = expected_slot.type_desc() {
            if let Some(expected_arrow_type) = arrow_type_from_desc(type_desc) {
                if field.data_type() != &expected_arrow_type {
                    return Err(format!(
                        "exchange decoded arrow type mismatch at index {} for slot {}: batch={:?} expected={:?}",
                        idx,
                        slot_id,
                        field.data_type(),
                        expected_arrow_type
                    ));
                }
            }
        }
        slots.push(ChunkSlotSchema::try_new(
            *slot_id,
            field.name().clone(),
            field.is_nullable(),
            expected_slot.type_desc().cloned(),
            expected_slot.unique_id(),
        )?);
    }
    Ok(Arc::new(crate::exec::chunk::ChunkSchema::try_new(slots)?))
}

/// Encode chunks to exchange payload format.
pub fn encode_chunks(chunks: &[Chunk], include_slot_ids: bool) -> Result<Vec<u8>, String> {
    let arrow_payload = encode_arrow_ipc_chunks(chunks)?;
    let wire_meta = if include_slot_ids {
        ExchangeWireMeta::from_chunks(chunks)?
    } else {
        None
    };
    Ok(encode_exchange_payload_envelope(
        &arrow_payload,
        wire_meta.as_ref(),
    ))
}

#[cfg(test)]
/// Decode chunks from exchange payload format using field metadata on the Arrow schema.
///
/// This remains as a compatibility helper for code paths/tests that still operate on
/// self-describing Arrow batches. Exchange runtime should use `decode_chunks_for_sender`.
pub fn decode_chunks(bytes: &[u8]) -> Result<Vec<Chunk>, String> {
    let decoded = decode_exchange_payload_envelope(bytes)?;
    let batches = decode_arrow_ipc_batches(decoded.arrow_payload)?;
    let mut chunks = Vec::new();
    for batch in batches {
        let chunk = Chunk::try_new(batch)?;
        chunks.push(chunk);
    }
    Ok(chunks)
}

pub fn decode_chunks_for_sender(
    key: ExchangeKey,
    sender_id: i32,
    be_number: i32,
    bytes: &[u8],
) -> Result<Vec<Chunk>, String> {
    let DecodedExchangePayload {
        wire_meta: decoded_wire_meta,
        arrow_payload,
    } = decode_exchange_payload_envelope(bytes)?;
    if arrow_payload.is_empty() {
        return Ok(Vec::new());
    }

    let receiver = get_or_create(key);
    let expected_chunk_schema;
    let wire_meta;
    {
        let wait_start = Instant::now();
        let wait_timeout = Duration::from_millis(exchange_wait_ms());
        let mut st = receiver.mu.lock().expect("exchange receiver lock");
        if let Some(meta) = decoded_wire_meta {
            match st.sender_wire_meta.get(&(sender_id, be_number)) {
                Some(existing) if existing != &meta => {
                    return Err(format!(
                        "exchange sender wire meta changed unexpectedly: finst={} node_id={} sender_id={} be_number={}",
                        key.finst_uuid(),
                        key.node_id,
                        sender_id,
                        be_number
                    ));
                }
                Some(_) => {}
                None => {
                    st.sender_wire_meta
                        .insert((sender_id, be_number), meta.clone());
                }
            }
            wire_meta = meta;
        } else {
            wire_meta = st
                .sender_wire_meta
                .get(&(sender_id, be_number))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "exchange wire meta missing before first data chunk: finst={} node_id={} sender_id={} be_number={}",
                        key.finst_uuid(),
                        key.node_id,
                        sender_id,
                        be_number
                    )
                })?;
        }
        loop {
            if let Some(schema) = st.expected_chunk_schema.clone() {
                expected_chunk_schema = schema;
                break;
            }
            if st.canceled {
                return Err("exchange canceled".to_string());
            }
            let elapsed = wait_start.elapsed();
            if elapsed >= wait_timeout {
                return Err(format!(
                    "exchange expected chunk schema not registered: finst={} node_id={}",
                    key.finst_uuid(),
                    key.node_id
                ));
            }
            let remain = wait_timeout - elapsed;
            let wait_step = remain.min(EXCHANGE_WAIT_LOG_INTERVAL);
            let (next, wait_res) = receiver
                .cv
                .wait_timeout(st, wait_step)
                .map_err(|_| "exchange wait poisoned".to_string())?;
            st = next;
            if wait_res.timed_out() && wait_step < remain {
                let elapsed_after = wait_start.elapsed();
                debug!(
                    "exchange decode waiting for schema: finst={} node_id={} sender_id={} be_number={} elapsed={:?} remain={:?}",
                    key.finst_uuid(),
                    key.node_id,
                    sender_id,
                    be_number,
                    elapsed_after,
                    wait_timeout.saturating_sub(elapsed_after)
                );
            }
        }
    }

    let batches = decode_arrow_ipc_batches(arrow_payload)?;
    let mut chunks = Vec::with_capacity(batches.len());
    for batch in batches {
        let chunk_schema = chunk_schema_for_wire_meta(&expected_chunk_schema, &batch, &wire_meta)?;
        chunks.push(Chunk::try_new_with_chunk_schema(batch, chunk_schema)?);
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        ExchangeKey, cancel_exchange_key, decode_chunks, decode_chunks_for_sender, encode_chunks,
        register_expected_chunk_schema,
    };
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSlotSchema, field_with_slot_id};

    fn exchange_test_chunk(last_name: &str) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("v1", DataType::Int32, true), SlotId::new(33)),
            field_with_slot_id(Field::new("v2", DataType::Int32, true), SlotId::new(34)),
            field_with_slot_id(Field::new("k1", DataType::Int32, true), SlotId::new(31)),
            field_with_slot_id(Field::new(last_name, DataType::Utf8, true), SlotId::new(32)),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(Int32Array::from(vec![Some(2)])),
            Arc::new(Int32Array::from(vec![Some(3)])),
            Arc::new(StringArray::from(vec![Some("x")])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("record batch");
        Chunk::try_new(batch).expect("chunk")
    }

    fn exchange_test_chunk_without_metadata(last_name: &str) -> Chunk {
        let chunk = exchange_test_chunk(last_name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int32, true),
            Field::new("v2", DataType::Int32, true),
            Field::new("k1", DataType::Int32, true),
            Field::new(last_name, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, chunk.batch.columns().to_vec()).expect("batch");
        Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref()).expect("chunk")
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
        let widened = RecordBatch::try_new(schema, strict.batch.columns().to_vec()).expect("batch");
        let widened_chunk_schema = Arc::new(
            crate::exec::chunk::ChunkSchema::try_new(
                widened
                    .schema()
                    .fields()
                    .iter()
                    .zip(strict.chunk_schema().slots().iter())
                    .map(|(field, slot)| {
                        ChunkSlotSchema::try_new(
                            slot.slot_id(),
                            field.name(),
                            field.is_nullable(),
                            slot.type_desc().cloned(),
                            slot.unique_id(),
                        )
                    })
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
    fn encode_chunks_rejects_slot_id_mismatch() {
        let first = exchange_test_chunk("_cse_0");
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("v1", DataType::Int32, true), SlotId::new(33)),
            field_with_slot_id(Field::new("v2", DataType::Int32, true), SlotId::new(34)),
            field_with_slot_id(Field::new("k1", DataType::Int32, true), SlotId::new(31)),
            field_with_slot_id(Field::new("_cse_2", DataType::Utf8, true), SlotId::new(99)),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(Int32Array::from(vec![Some(2)])),
            Arc::new(Int32Array::from(vec![Some(3)])),
            Arc::new(StringArray::from(vec![Some("x")])),
        ];
        let second = Chunk::try_new(RecordBatch::try_new(schema, columns).expect("record batch"))
            .expect("chunk");

        let err =
            encode_chunks(&[first, second], true).expect_err("should reject mismatched slot id");
        assert!(err.contains("exchange encode schema mismatch"));
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
        assert_eq!(second_decoded[0].schema().field(3).name(), "_cse_2");

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
            RecordBatch::try_new(widened_schema, strict.batch.columns().to_vec()).expect("batch");
        let widened_chunk_schema = Arc::new(
            crate::exec::chunk::ChunkSchema::try_new(
                widened
                    .schema()
                    .fields()
                    .iter()
                    .zip(strict.chunk_schema().slots().iter())
                    .map(|(field, slot)| {
                        ChunkSlotSchema::try_new(
                            slot.slot_id(),
                            field.name(),
                            field.is_nullable(),
                            slot.type_desc().cloned(),
                            slot.unique_id(),
                        )
                    })
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
    fn decode_chunks_for_sender_waits_for_schema_registration() {
        let key = ExchangeKey {
            finst_id_hi: 201,
            finst_id_lo: 202,
            node_id: 19,
        };
        let payload = encode_chunks(&[exchange_test_chunk("_cse_0")], true).expect("encode");
        let decode_key = key;
        let handle = std::thread::spawn(move || {
            decode_chunks_for_sender(decode_key, 7, 1, &payload).expect("decode after wait")
        });

        std::thread::sleep(Duration::from_millis(50));
        let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
        register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

        let decoded = handle.join().expect("join decode thread");
        assert_eq!(decoded.len(), 1);

        cancel_exchange_key(key);
    }
}
