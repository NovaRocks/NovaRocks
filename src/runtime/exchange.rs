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
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

use crate::common::types::format_uuid;
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::schedule::observer::Observable;
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

#[derive(Default)]
struct ReceiverState {
    expected_senders: usize,
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

/// Encode chunks to Arrow IPC stream format
pub fn encode_chunks(chunks: &[Chunk]) -> Result<Vec<u8>, String> {
    if chunks.is_empty() {
        return Ok(vec![]);
    }

    let mut buffer = Vec::new();

    // Use the schema from the first chunk.
    let schema = chunks[0].schema();
    for (i, c) in chunks.iter().enumerate().skip(1) {
        if c.schema().as_ref() != schema.as_ref() {
            return Err(format!(
                "exchange encode schema mismatch at chunk index {}: expected={:?} actual={:?}",
                i,
                schema,
                c.schema()
            ));
        }
    }
    let mut writer = StreamWriter::try_new(&mut buffer, &schema)
        .map_err(|e| format!("failed to create Arrow IPC writer: {e}"))?;

    for chunk in chunks {
        writer
            .write(&chunk.batch)
            .map_err(|e| format!("failed to write batch: {e}"))?;
    }

    writer
        .finish()
        .map_err(|e| format!("failed to finish Arrow IPC writer: {e}"))?;

    Ok(buffer)
}

/// Decode chunks from Arrow IPC stream format
pub fn decode_chunks(bytes: &[u8]) -> Result<Vec<Chunk>, String> {
    if bytes.is_empty() {
        return Ok(vec![]);
    }

    let mut cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| format!("failed to create Arrow IPC reader: {e}"))?;

    let mut chunks = Vec::new();
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
        let chunk = Chunk::try_new(batch)?;
        chunks.push(chunk);
    }

    Ok(chunks)
}
