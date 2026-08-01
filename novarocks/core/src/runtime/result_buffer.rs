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
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use crate::common::result_batch::ResultBatch;
use crate::common::types::{FetchResult, UniqueId};
use crate::runtime::fragment::io::ResultAbort;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ResultBufferMode {
    Legacy,
    Typed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResultBufferWriteState {
    Open,
    Finished,
    Aborted,
}

/// Observable result-buffer state changes for an adapter-owned publication boundary.
///
/// Core always wakes its in-process waiters after a mutation. Protocol adapters decide whether a
/// particular state transition needs an external notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultPublication {
    DataReady,
    TerminalReady,
    Removed,
    NoChange,
}

/// Fragment-scoped ownership of a single Result Buffer sender registration.
///
/// The handle intentionally owns terminal publication: callers cannot finish through one path
/// and later discard the same finished result through another cleanup path.
pub struct ResultBufferWriteHandle {
    fragment_instance_id: UniqueId,
    mode: ResultBufferMode,
    state: Mutex<ResultBufferWriteState>,
}

impl ResultBufferWriteHandle {
    pub fn open(
        fragment_instance_id: UniqueId,
        typed: bool,
        mem_tracker: Option<&Arc<MemTracker>>,
    ) -> Result<Self, String> {
        let mode = if typed {
            ResultBufferMode::Typed
        } else {
            ResultBufferMode::Legacy
        };
        try_create_sender_with_mode(fragment_instance_id, mode)?;
        if let Some(root) = mem_tracker {
            let tracker =
                MemTracker::new_child(format!("ResultBuffer: finst={fragment_instance_id}"), root);
            set_mem_tracker(fragment_instance_id, tracker);
        }
        Ok(Self {
            fragment_instance_id,
            mode,
            state: Mutex::new(ResultBufferWriteState::Open),
        })
    }

    pub fn write_legacy(&self, result: FetchResult) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Legacy {
            return Err("typed result buffer cannot accept legacy result batches".to_string());
        }
        Ok(insert(self.fragment_instance_id, result))
    }

    pub fn write_typed(&self, payload: Vec<u8>) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Typed {
            return Err("legacy result buffer cannot accept typed payloads".to_string());
        }
        insert_typed(self.fragment_instance_id, payload)
    }

    pub fn finish(&self) -> Result<ResultPublication, String> {
        let mut state = self.state.lock().expect("result buffer write handle lock");
        match *state {
            ResultBufferWriteState::Open => {
                let publication = close_ok(self.fragment_instance_id);
                *state = ResultBufferWriteState::Finished;
                Ok(publication)
            }
            ResultBufferWriteState::Finished => Ok(ResultPublication::NoChange),
            ResultBufferWriteState::Aborted => {
                Err("result buffer session was aborted before finish".to_string())
            }
        }
    }

    pub fn abort(&self, reason: ResultAbort) -> ResultPublication {
        let mut state = self.state.lock().expect("result buffer write handle lock");
        if *state != ResultBufferWriteState::Open {
            return ResultPublication::NoChange;
        }
        let publication = match reason {
            ResultAbort::PrepareRollback | ResultAbort::NeverStarted => {
                discard(self.fragment_instance_id)
            }
            ResultAbort::Failed(error) => close_error(self.fragment_instance_id, error),
            ResultAbort::Cancelled(reason) => {
                cancel_with_message(self.fragment_instance_id, reason)
            }
        };
        *state = ResultBufferWriteState::Aborted;
        publication
    }

    fn require_open(&self) -> Result<(), String> {
        match *self.state.lock().expect("result buffer write handle lock") {
            ResultBufferWriteState::Open => Ok(()),
            ResultBufferWriteState::Finished => {
                Err("result buffer session was already finished".to_string())
            }
            ResultBufferWriteState::Aborted => {
                Err("result buffer session was already aborted".to_string())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum FetchErrorKind {
    NotFound,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone)]
pub struct FetchError {
    pub kind: FetchErrorKind,
    pub message: String,
}

#[derive(Debug)]
struct BufferControlBlock {
    queue: VecDeque<TrackedFetchResult>,
    typed_queue: VecDeque<TrackedTypedFetchResult>,
    mode: Option<ResultBufferMode>,
    closed_ok: bool,
    eos_sent: bool,
    status_error: Option<String>,
    cancelled: bool,
    cancel_message: Option<String>,
    next_packet_seq: i64,
    mem_tracker: Option<Arc<MemTracker>>,
    eos_template: Option<ResultBatch>,
}

impl BufferControlBlock {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            typed_queue: VecDeque::new(),
            mode: None,
            closed_ok: false,
            eos_sent: false,
            status_error: None,
            cancelled: false,
            cancel_message: None,
            next_packet_seq: 0,
            mem_tracker: None,
            eos_template: None,
        }
    }

    fn make_eos_result(&mut self) -> FetchResult {
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;
        FetchResult {
            packet_seq: seq,
            eos: true,
            result_batch: self.eos_template.clone().unwrap_or_else(ResultBatch::empty),
        }
    }

    fn make_typed_eos_result(&mut self) -> TypedFetchResult {
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;
        TypedFetchResult {
            packet_seq: seq,
            eos: true,
            payload: Vec::new(),
        }
    }

    fn pop_next(&mut self) -> Option<FetchResult> {
        let out = self.queue.pop_front()?;
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;
        Some(out.into_result(seq))
    }

    fn pop_next_typed(&mut self) -> Option<TypedFetchResult> {
        let out = self.typed_queue.pop_front()?;
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;
        Some(out.into_result(seq))
    }

    fn set_mode(&mut self, mode: ResultBufferMode) -> Result<(), String> {
        match self.mode {
            None => {
                self.mode = Some(mode);
                Ok(())
            }
            Some(existing) if existing == mode => Ok(()),
            Some(existing) => Err(format!(
                "result buffer mode mismatch: existing={existing:?} requested={mode:?}"
            )),
        }
    }

    fn fail_mode_mismatch(&mut self, mode: ResultBufferMode) {
        if let Err(err) = self.set_mode(mode) {
            self.status_error = Some(err);
            self.queue.clear();
            self.typed_queue.clear();
        }
    }
}

#[derive(Debug)]
struct TrackedFetchResult {
    result: FetchResult,
    accounting: Option<TrackedBytes>,
}

impl TrackedFetchResult {
    fn new(result: FetchResult, tracker: Option<&Arc<MemTracker>>) -> Self {
        let accounting = tracker.map(|tracker| {
            let bytes = fetch_result_bytes(&result);
            TrackedBytes::new(bytes, Arc::clone(tracker))
        });
        Self { result, accounting }
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let bytes = fetch_result_bytes(&self.result);
        match self.accounting.as_mut() {
            Some(accounting) => accounting.transfer_to(Arc::clone(&tracker)),
            None => {
                self.accounting = Some(TrackedBytes::new(bytes, tracker));
            }
        }
    }

    fn into_result(self, seq: i64) -> FetchResult {
        let TrackedFetchResult {
            mut result,
            accounting: _accounting,
        } = self;
        result.packet_seq = seq;
        result
    }
}

fn fetch_result_bytes(result: &FetchResult) -> usize {
    result.result_batch.heap_size_bytes()
}

#[derive(Debug, Clone)]
pub(crate) struct TypedFetchResult {
    pub(crate) packet_seq: i64,
    pub(crate) eos: bool,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug)]
struct TrackedTypedFetchResult {
    result: TypedFetchResult,
    accounting: Option<TrackedBytes>,
}

impl TrackedTypedFetchResult {
    fn new(result: TypedFetchResult, tracker: Option<&Arc<MemTracker>>) -> Self {
        let accounting = tracker.map(|tracker| {
            let bytes = typed_fetch_result_bytes(&result);
            TrackedBytes::new(bytes, Arc::clone(tracker))
        });
        Self { result, accounting }
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let bytes = typed_fetch_result_bytes(&self.result);
        match self.accounting.as_mut() {
            Some(accounting) => accounting.transfer_to(Arc::clone(&tracker)),
            None => {
                self.accounting = Some(TrackedBytes::new(bytes, tracker));
            }
        }
    }

    fn into_result(self, seq: i64) -> TypedFetchResult {
        let TrackedTypedFetchResult {
            mut result,
            accounting: _accounting,
        } = self;
        result.packet_seq = seq;
        result
    }
}

fn typed_fetch_result_bytes(result: &TypedFetchResult) -> usize {
    result.payload.capacity().max(result.payload.len())
}

struct ResultCtx {
    mu: Mutex<HashMap<UniqueId, BufferControlBlock>>,
    cvar: Condvar,
}

static CTX: OnceLock<ResultCtx> = OnceLock::new();

fn ctx() -> &'static ResultCtx {
    CTX.get_or_init(|| ResultCtx {
        mu: Mutex::new(HashMap::new()),
        cvar: Condvar::new(),
    })
}

fn notify_waiters() {
    ctx().cvar.notify_all();
}

pub(crate) fn insert(finst_id: UniqueId, result: FetchResult) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        if block.closed_ok || block.status_error.is_some() || block.cancelled {
            return ResultPublication::NoChange;
        }
        if block.set_mode(ResultBufferMode::Legacy).is_ok() {
            let tracked = TrackedFetchResult::new(result, block.mem_tracker.as_ref());
            block.queue.push_back(tracked);
            publication = ResultPublication::DataReady;
        } else {
            block.fail_mode_mismatch(ResultBufferMode::Legacy);
        }
    }
    notify_waiters();
    publication
}

pub(crate) fn insert_typed(
    finst_id: UniqueId,
    payload: Vec<u8>,
) -> Result<ResultPublication, String> {
    let c = ctx();
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        if block.closed_ok || block.status_error.is_some() || block.cancelled {
            return Ok(ResultPublication::NoChange);
        }
        block.set_mode(ResultBufferMode::Typed)?;
        let result = TypedFetchResult {
            packet_seq: 0,
            eos: false,
            payload,
        };
        let tracked = TrackedTypedFetchResult::new(result, block.mem_tracker.as_ref());
        block.typed_queue.push_back(tracked);
    }
    notify_waiters();
    Ok(ResultPublication::DataReady)
}

pub(crate) fn close_ok(finst_id: UniqueId) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        if !block.closed_ok && block.status_error.is_none() && !block.cancelled {
            block.closed_ok = true;
            publication = ResultPublication::TerminalReady;
        }
    }
    notify_waiters();
    publication
}

pub(crate) fn close_error(finst_id: UniqueId, message: String) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        if !block.closed_ok && block.status_error.is_none() && !block.cancelled {
            block.status_error = Some(message);
            block.queue.clear();
            block.typed_queue.clear();
            publication = ResultPublication::TerminalReady;
        }
    }
    notify_waiters();
    publication
}

pub(crate) fn cancel(finst_id: UniqueId) -> ResultPublication {
    cancel_with_message(finst_id, "Cancelled".to_string())
}

fn cancel_with_message(finst_id: UniqueId, message: String) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        if !block.closed_ok && block.status_error.is_none() && !block.cancelled {
            block.cancelled = true;
            block.cancel_message = Some(message);
            block.queue.clear();
            block.typed_queue.clear();
            publication = ResultPublication::TerminalReady;
        }
    }
    notify_waiters();
    publication
}

pub(crate) fn create_sender(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Legacy);
}

pub(crate) fn try_create_sender(finst_id: UniqueId) -> Result<(), String> {
    try_create_sender_with_mode(finst_id, ResultBufferMode::Legacy)
}

pub(crate) fn create_typed_sender(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Typed);
}

pub(crate) fn try_create_typed_sender(finst_id: UniqueId) -> Result<(), String> {
    try_create_sender_with_mode(finst_id, ResultBufferMode::Typed)
}

fn try_create_sender_with_mode(finst_id: UniqueId, mode: ResultBufferMode) -> Result<(), String> {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    if guard.contains_key(&finst_id) {
        return Err(format!(
            "result buffer already registered for fragment instance {finst_id}"
        ));
    }
    let mut block = BufferControlBlock::new();
    block.set_mode(mode)?;
    guard.insert(finst_id, block);
    Ok(())
}

pub(crate) fn discard(finst_id: UniqueId) -> ResultPublication {
    let c = ctx();
    c.mu.lock().expect("ctx lock").remove(&finst_id);
    c.cvar.notify_all();
    ResultPublication::Removed
}

pub(crate) fn is_registered(finst_id: UniqueId) -> bool {
    ctx().mu.lock().expect("ctx lock").contains_key(&finst_id)
}

pub(crate) fn set_mem_tracker(finst_id: UniqueId, tracker: Arc<MemTracker>) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.mem_tracker = Some(Arc::clone(&tracker));
    for result in block.queue.iter_mut() {
        result.set_mem_tracker(Arc::clone(&tracker));
    }
    for result in block.typed_queue.iter_mut() {
        result.set_mem_tracker(Arc::clone(&tracker));
    }
}

pub(crate) fn set_eos_template(finst_id: UniqueId, template: ResultBatch) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Legacy);
    block.eos_template = Some(template);
}

#[derive(Debug)]
pub enum TryFetchResult {
    Ready(FetchResult),
    NotReady,
    Error(FetchError),
}

#[derive(Debug)]
pub(crate) enum TryFetchTypedResult {
    Ready(TypedFetchResult),
    NotReady,
    Error(FetchError),
}

/// Inner fetch logic that works on an already-held HashMap guard.
///
/// Separating this from `try_fetch` allows `wait_fetch` to check state
/// while holding the lock, avoiding the missed-wakeup race that would
/// arise if the check and the condvar wait were not atomic with respect
/// to the mutex.
fn try_fetch_inner(
    guard: &mut HashMap<UniqueId, BufferControlBlock>,
    finst_id: UniqueId,
) -> TryFetchResult {
    let Some(block) = guard.get_mut(&finst_id) else {
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "no result for this query".to_string(),
        });
    };

    if block.cancelled {
        let msg = block
            .cancel_message
            .clone()
            .unwrap_or_else(|| "Cancelled".to_string());
        guard.remove(&finst_id);
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::Cancelled,
            message: msg,
        });
    }
    if let Some(msg) = block.status_error.as_ref() {
        let msg = msg.clone();
        guard.remove(&finst_id);
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::Failed,
            message: msg,
        });
    }
    if block.mode == Some(ResultBufferMode::Typed) {
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::Failed,
            message: "typed result buffer cannot be fetched as legacy result batch".to_string(),
        });
    }
    if let Some(result) = block.pop_next() {
        return TryFetchResult::Ready(result);
    }
    if block.closed_ok && !block.eos_sent {
        block.eos_sent = true;
        return TryFetchResult::Ready(block.make_eos_result());
    }
    if block.closed_ok && block.eos_sent {
        guard.remove(&finst_id);
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "result stream already reached eos".to_string(),
        });
    }
    TryFetchResult::NotReady
}

fn try_fetch_typed_inner(
    guard: &mut HashMap<UniqueId, BufferControlBlock>,
    finst_id: UniqueId,
) -> TryFetchTypedResult {
    let Some(block) = guard.get_mut(&finst_id) else {
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "no result for this query".to_string(),
        });
    };

    if block.cancelled {
        let msg = block
            .cancel_message
            .clone()
            .unwrap_or_else(|| "Cancelled".to_string());
        guard.remove(&finst_id);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Cancelled,
            message: msg,
        });
    }
    if let Some(msg) = block.status_error.as_ref() {
        let msg = msg.clone();
        guard.remove(&finst_id);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Failed,
            message: msg,
        });
    }
    if block.mode == Some(ResultBufferMode::Legacy) {
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Failed,
            message: "legacy result buffer cannot be fetched as typed Arrow IPC".to_string(),
        });
    }
    if let Some(result) = block.pop_next_typed() {
        return TryFetchTypedResult::Ready(result);
    }
    if block.closed_ok && !block.eos_sent {
        block.eos_sent = true;
        return TryFetchTypedResult::Ready(block.make_typed_eos_result());
    }
    if block.closed_ok && block.eos_sent {
        guard.remove(&finst_id);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "result stream already reached eos".to_string(),
        });
    }
    TryFetchTypedResult::NotReady
}

pub fn try_fetch(finst_id: UniqueId) -> TryFetchResult {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    try_fetch_inner(&mut guard, finst_id)
}

/// Long-poll variant of `try_fetch`.
///
/// - If `max_wait_ms <= 0` or the buffer already has a result, behaves like
///   `try_fetch` (returns immediately).
/// - Otherwise waits up to `max_wait_ms` milliseconds for a result to become
///   available, then returns whatever state the buffer is in at that point.
///
/// The implementation uses a `Condvar` that is notified by every mutating
/// operation (`insert`, `close_ok`, `close_error`, `cancel`).  The check and
/// the wait are performed while holding the mutex, so no wakeup is missed.
pub(crate) fn wait_fetch(finst_id: UniqueId, max_wait_ms: i64) -> TryFetchResult {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");

    // Check immediately under the lock before deciding whether to wait.
    let initial = try_fetch_inner(&mut guard, finst_id);
    if !matches!(initial, TryFetchResult::NotReady) || max_wait_ms <= 0 {
        return initial;
    }

    let timeout = Duration::from_millis(max_wait_ms as u64);
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return TryFetchResult::NotReady;
        }

        // Atomically release the lock and sleep until notified or timed out.
        // The lock is re-acquired before wait_timeout returns.
        let (mut guard2, _timeout_result) =
            c.cvar.wait_timeout(guard, remaining).expect("condvar wait");

        // Re-check state while still holding the newly re-acquired lock.
        let result = try_fetch_inner(&mut guard2, finst_id);
        if !matches!(result, TryFetchResult::NotReady) {
            return result;
        }

        // Prepare for the next iteration; release the lock by re-assigning.
        guard = guard2;

        if std::time::Instant::now() >= deadline {
            return TryFetchResult::NotReady;
        }
    }
}

pub(crate) fn wait_fetch_typed(finst_id: UniqueId, max_wait_ms: i64) -> TryFetchTypedResult {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");

    let initial = try_fetch_typed_inner(&mut guard, finst_id);
    if !matches!(initial, TryFetchTypedResult::NotReady) || max_wait_ms <= 0 {
        return initial;
    }

    let timeout = Duration::from_millis(max_wait_ms as u64);
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return TryFetchTypedResult::NotReady;
        }

        let (mut guard2, _timeout_result) =
            c.cvar.wait_timeout(guard, remaining).expect("condvar wait");

        let result = try_fetch_typed_inner(&mut guard2, finst_id);
        if !matches!(result, TryFetchTypedResult::NotReady) {
            return result;
        }

        guard = guard2;

        if std::time::Instant::now() >= deadline {
            return TryFetchTypedResult::NotReady;
        }
    }
}

fn fallback_fetch_wait_timeout() -> Duration {
    Duration::from_secs(300)
}

pub fn fetch_wait_timeout(finst_id: UniqueId) -> Duration {
    use crate::runtime::query_context::query_context_manager;

    query_context_manager()
        .get_query_timeout_by_finst(finst_id)
        .unwrap_or_else(fallback_fetch_wait_timeout)
}

pub fn fetch_wait_timeout_ms(finst_id: UniqueId) -> i64 {
    let millis = fetch_wait_timeout(finst_id).as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::fragment::io::ResultAbort;
    use crate::runtime::query_context::{QueryId, query_context_manager};

    #[test]
    fn fragment_result_session_writes_twenty_plus_seventeen_rows_and_finishes_once() {
        let finst_id = UniqueId::new(9901, 9902);
        let handle = ResultBufferWriteHandle::open(finst_id, false, None).expect("open result");
        let first_rows = (0..20)
            .map(|row| format!("row-{row}").into_bytes())
            .collect();
        let second_rows = (20..37)
            .map(|row| format!("row-{row}").into_bytes())
            .collect();

        handle
            .write_legacy(FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(first_rows, false, 0, None),
            })
            .expect("write first result batch");
        handle
            .write_legacy(FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(second_rows, false, 0, None),
            })
            .expect("write second result batch");
        handle.finish().expect("finish once");
        handle.finish().expect("repeated finish is idempotent");

        let TryFetchResult::Ready(first) = try_fetch(finst_id) else {
            panic!("expected first result batch");
        };
        let TryFetchResult::Ready(second) = try_fetch(finst_id) else {
            panic!("expected second result batch");
        };
        let TryFetchResult::Ready(eos) = try_fetch(finst_id) else {
            panic!("expected eos");
        };
        assert_eq!(first.result_batch.rows.len(), 20);
        assert_eq!(second.result_batch.rows.len(), 17);
        assert!(eos.eos);
    }

    #[test]
    fn fragment_result_session_abort_is_idempotent_and_rejects_late_write() {
        let finst_id = UniqueId::new(9903, 9904);
        let handle = ResultBufferWriteHandle::open(finst_id, false, None).expect("open result");

        assert_eq!(
            handle.abort(ResultAbort::Cancelled("Cancelled".to_string())),
            ResultPublication::TerminalReady
        );
        assert_eq!(
            handle.abort(ResultAbort::Failed("late failure".to_string())),
            ResultPublication::NoChange
        );
        assert!(
            handle
                .write_legacy(FetchResult {
                    packet_seq: 0,
                    eos: false,
                    result_batch: ResultBatch::empty(),
                })
                .is_err()
        );
        let TryFetchResult::Error(error) = try_fetch(finst_id) else {
            panic!("expected cancelled result");
        };
        assert!(matches!(error.kind, FetchErrorKind::Cancelled));
    }

    #[test]
    fn publication_outcomes_distinguish_data_terminal_and_removed_state() {
        let finst_id = UniqueId::new(9909, 9910);
        let handle = ResultBufferWriteHandle::open(finst_id, false, None).expect("open result");

        assert_eq!(
            handle
                .write_legacy(FetchResult {
                    packet_seq: 0,
                    eos: false,
                    result_batch: ResultBatch::empty(),
                })
                .expect("write result"),
            ResultPublication::DataReady
        );
        assert_eq!(
            handle.finish().expect("finish result"),
            ResultPublication::TerminalReady
        );
        assert_eq!(
            handle.finish().expect("repeat finish"),
            ResultPublication::NoChange
        );

        let rolled_back = UniqueId::new(9911, 9912);
        let handle = ResultBufferWriteHandle::open(rolled_back, false, None).expect("open result");
        assert_eq!(
            handle.abort(ResultAbort::PrepareRollback),
            ResultPublication::Removed
        );
        assert!(!is_registered(rolled_back));
    }

    #[test]
    fn fragment_result_session_failure_preserves_execution_error() {
        let finst_id = UniqueId::new(9907, 9908);
        let handle = ResultBufferWriteHandle::open(finst_id, false, None).expect("open result");

        assert_eq!(
            handle.abort(ResultAbort::Failed(
                "assert_num_rows failed: actual=2 row(s), expected = 1 row(s)".to_string(),
            )),
            ResultPublication::TerminalReady
        );

        let TryFetchResult::Error(error) = try_fetch(finst_id) else {
            panic!("expected failed result");
        };
        assert_eq!(
            error.message,
            "assert_num_rows failed: actual=2 row(s), expected = 1 row(s)"
        );
    }

    #[test]
    fn fragment_result_session_late_abort_preserves_finished_result() {
        let finst_id = UniqueId::new(9905, 9906);
        let handle = ResultBufferWriteHandle::open(finst_id, false, None).expect("open result");
        handle
            .write_legacy(FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(vec![b"done".to_vec()], false, 0, None),
            })
            .expect("write result");
        assert_eq!(
            handle.finish().expect("finish result"),
            ResultPublication::TerminalReady
        );
        assert_eq!(
            handle.abort(ResultAbort::Cancelled("Cancelled".to_string())),
            ResultPublication::NoChange
        );

        let TryFetchResult::Ready(result) = try_fetch(finst_id) else {
            panic!("late abort must not discard a finished result");
        };
        assert_eq!(result.result_batch.rows, vec![b"done".to_vec()]);
    }

    #[test]
    fn cancel_is_observable() {
        let finst_id = UniqueId::new(42, 7);
        create_sender(finst_id);
        assert_eq!(cancel(finst_id), ResultPublication::TerminalReady);

        let TryFetchResult::Error(err) = try_fetch(finst_id) else {
            panic!("expected cancel error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Cancelled));
    }

    #[test]
    fn close_error_is_observable() {
        let finst_id = UniqueId::new(1, 2);
        create_sender(finst_id);
        assert_eq!(
            close_error(finst_id, "boom".to_string()),
            ResultPublication::TerminalReady
        );

        let TryFetchResult::Error(err) = try_fetch(finst_id) else {
            panic!("expected close_error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert_eq!(err.message, "boom");
    }

    #[test]
    fn try_fetch_returns_batches_in_order_and_then_eos() {
        let finst_id = UniqueId::new(7, 9);
        create_sender(finst_id);
        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(vec![b"a".to_vec()], false, 0, None),
            },
        );
        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(vec![b"b".to_vec()], false, 0, None),
            },
        );
        close_ok(finst_id);

        let TryFetchResult::Ready(first) = try_fetch(finst_id) else {
            panic!("expected first batch");
        };
        assert_eq!(first.packet_seq, 0);
        assert!(!first.eos);
        assert_eq!(first.result_batch.rows, vec![b"a".to_vec()]);

        let TryFetchResult::Ready(second) = try_fetch(finst_id) else {
            panic!("expected second batch");
        };
        assert_eq!(second.packet_seq, 1);
        assert!(!second.eos);
        assert_eq!(second.result_batch.rows, vec![b"b".to_vec()]);

        let TryFetchResult::Ready(eos) = try_fetch(finst_id) else {
            panic!("expected eos");
        };
        assert_eq!(eos.packet_seq, 2);
        assert!(eos.eos);
        assert!(eos.result_batch.rows.is_empty());

        let TryFetchResult::Error(err) = try_fetch(finst_id) else {
            panic!("expected terminal not found after eos");
        };
        assert!(matches!(err.kind, FetchErrorKind::NotFound));
    }

    #[test]
    fn not_ready_transitions_to_ready_after_insert() {
        let finst_id = UniqueId::new(70, 90);
        create_sender(finst_id);
        assert!(matches!(try_fetch(finst_id), TryFetchResult::NotReady));

        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(vec![b"row".to_vec()], false, 0, None),
            },
        );

        let TryFetchResult::Ready(batch) = try_fetch(finst_id) else {
            panic!("expected ready batch");
        };
        assert_eq!(batch.packet_seq, 0);
        assert_eq!(batch.result_batch.rows.len(), 1);
    }

    #[test]
    fn fetch_wait_timeout_prefers_query_context() {
        let query_id = QueryId::new(101, 202);
        let finst_id = UniqueId::new(303, 404);
        let mgr = query_context_manager();
        mgr.ensure_native_context(
            query_id,
            false,
            Duration::from_secs(5),
            Duration::from_secs(12),
        )
        .expect("ensure query context");
        mgr.register_finst(finst_id, query_id);

        assert_eq!(fetch_wait_timeout_ms(finst_id), 12_000);

        mgr.unregister_finst(finst_id);
        mgr.finish_fragment(query_id);
    }

    #[test]
    fn wait_fetch_with_zero_max_wait_returns_not_ready_immediately() {
        let finst_id = UniqueId::new(601, 602);
        create_sender(finst_id);
        // Empty open buffer with max_wait_ms=0 must return NotReady instantly.
        assert!(matches!(wait_fetch(finst_id, 0), TryFetchResult::NotReady));
    }

    #[test]
    fn wait_fetch_returns_ready_after_delayed_insert() {
        let finst_id = UniqueId::new(603, 604);
        create_sender(finst_id);

        // Insert from a background thread after 20 ms.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            insert(
                finst_id,
                FetchResult {
                    packet_seq: 0,
                    eos: false,
                    result_batch: ResultBatch::new(vec![b"wait_data".to_vec()], false, 0, None),
                },
            );
        });

        let result = wait_fetch(finst_id, 1000);
        assert!(
            matches!(result, TryFetchResult::Ready(_)),
            "wait_fetch should return Ready after delayed insert; got: {result:?}"
        );
    }

    #[test]
    fn typed_fetch_returns_payloads_in_order_and_then_eof() {
        let finst_id = UniqueId::new(701, 702);
        create_typed_sender(finst_id);
        insert_typed(finst_id, vec![1, 2, 3]).expect("insert first typed payload");
        insert_typed(finst_id, vec![4, 5]).expect("insert second typed payload");
        close_ok(finst_id);

        let TryFetchTypedResult::Ready(first) = wait_fetch_typed(finst_id, 0) else {
            panic!("expected first typed payload");
        };
        assert_eq!(first.packet_seq, 0);
        assert!(!first.eos);
        assert_eq!(first.payload, vec![1, 2, 3]);

        let TryFetchTypedResult::Ready(second) = wait_fetch_typed(finst_id, 0) else {
            panic!("expected second typed payload");
        };
        assert_eq!(second.packet_seq, 1);
        assert!(!second.eos);
        assert_eq!(second.payload, vec![4, 5]);

        let TryFetchTypedResult::Ready(eos) = wait_fetch_typed(finst_id, 0) else {
            panic!("expected typed eos");
        };
        assert_eq!(eos.packet_seq, 2);
        assert!(eos.eos);
        assert!(eos.payload.is_empty());
    }

    #[test]
    fn typed_sender_rejects_legacy_fetch() {
        let finst_id = UniqueId::new(703, 704);
        create_typed_sender(finst_id);
        insert_typed(finst_id, vec![9]).expect("insert typed payload");

        let TryFetchResult::Error(err) = try_fetch(finst_id) else {
            panic!("expected mode mismatch error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert!(err.message.contains("typed"));
        assert!(err.message.contains("legacy"));
    }

    #[test]
    fn legacy_sender_rejects_typed_fetch() {
        let finst_id = UniqueId::new(705, 706);
        create_sender(finst_id);
        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: ResultBatch::new(vec![b"row".to_vec()], false, 0, None),
            },
        );

        let TryFetchTypedResult::Error(err) = wait_fetch_typed(finst_id, 0) else {
            panic!("expected mode mismatch error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert!(err.message.contains("legacy"));
        assert!(err.message.contains("typed"));
    }
}
