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
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use crate::runtime::result_batch::{FetchResult, ResultBatch};
use bytes::Bytes;
use novarocks_execution::runtime::exchange::BoundedExchangePayload;
use novarocks_execution::runtime::fragment::io::{
    ResultAbort, ResultWriteAdmission, ResultWriteCredit,
};
use novarocks_execution::runtime::mem_tracker::{MemTracker, TrackedBytes};
use novarocks_execution::runtime::observable::Observable;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_execution_contract::task_execution::operation::ResultByteLimit;
use novarocks_types::UniqueId;
use tokio::sync::Notify;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ResultBufferMode {
    Legacy,
    Typed,
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ResultBufferKey {
    LegacyFragment(UniqueId),
    Task(TaskIdentity),
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
/// The handle owns producer publication. A later task/context retirement may
/// still discard a finished but unacknowledged result and return its credits.
pub struct ResultBufferWriteHandle {
    key: ResultBufferKey,
    mode: ResultBufferMode,
    state: Mutex<ResultBufferWriteState>,
    retained_byte_cap: usize,
    retained_budget: Arc<ResultRetainedBudget>,
}

pub(crate) struct ResultRetainedBudget {
    process_byte_cap: usize,
    state: Mutex<ResultRetainedState>,
    writable: Arc<Observable>,
}

#[derive(Default)]
struct ResultRetainedState {
    process_retained_bytes: usize,
    process_high_water_bytes: usize,
    stream_retained_bytes: HashMap<ResultBufferKey, usize>,
}

impl ResultRetainedBudget {
    pub(crate) fn new(process_byte_cap: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            process_byte_cap: process_byte_cap.get(),
            state: Mutex::new(ResultRetainedState::default()),
            writable: Arc::new(Observable::new()),
        })
    }

    fn try_reserve(
        self: &Arc<Self>,
        key: ResultBufferKey,
        stream_byte_cap: usize,
        bytes: usize,
    ) -> Result<ResultWriteAdmission, String> {
        if bytes > stream_byte_cap {
            return Err(format!(
                "single result reservation of {bytes} bytes exceeds stream retained-byte cap {stream_byte_cap}"
            ));
        }
        if bytes > self.process_byte_cap {
            return Err(format!(
                "single result reservation of {bytes} bytes exceeds process retained-byte cap {}",
                self.process_byte_cap
            ));
        }
        {
            let mut state = self.state.lock().expect("result retained budget lock");
            let stream_retained = state.stream_retained_bytes.get(&key).copied().unwrap_or(0);
            let Some(next_stream) = stream_retained.checked_add(bytes) else {
                return Ok(ResultWriteAdmission::Blocked);
            };
            let Some(next_process) = state.process_retained_bytes.checked_add(bytes) else {
                return Ok(ResultWriteAdmission::Blocked);
            };
            if next_stream > stream_byte_cap || next_process > self.process_byte_cap {
                return Ok(ResultWriteAdmission::Blocked);
            }
            state.stream_retained_bytes.insert(key, next_stream);
            state.process_retained_bytes = next_process;
            state.process_high_water_bytes = state.process_high_water_bytes.max(next_process);
        }
        let budget = Arc::downgrade(self);
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            move |released| {
                let Some(budget) = budget.upgrade() else {
                    return;
                };
                {
                    let mut state = budget.state.lock().expect("result retained budget lock");
                    let stream = state
                        .stream_retained_bytes
                        .get_mut(&key)
                        .expect("result stream reservation exists");
                    *stream = stream
                        .checked_sub(released)
                        .expect("released result stream bytes were retained");
                    if *stream == 0 {
                        state.stream_retained_bytes.remove(&key);
                    }
                    state.process_retained_bytes = state
                        .process_retained_bytes
                        .checked_sub(released)
                        .expect("released process result bytes were retained");
                }
                budget.writable.notify_observers();
            },
        )))
    }
}

impl ResultBufferWriteHandle {
    pub fn open(
        key: ResultBufferKey,
        typed: bool,
        retained_byte_cap: NonZeroUsize,
        retained_budget: Arc<ResultRetainedBudget>,
        mem_tracker: Option<&Arc<MemTracker>>,
    ) -> Result<Self, String> {
        let mode = if typed {
            ResultBufferMode::Typed
        } else {
            ResultBufferMode::Legacy
        };
        try_create_sender_with_key_mode(key, mode)?;
        if let Some(root) = mem_tracker {
            let tracker = MemTracker::new_child(format!("ResultBuffer: key={key:?}"), root);
            set_mem_tracker_for_key(key, tracker);
        }
        Ok(Self {
            key,
            mode,
            state: Mutex::new(ResultBufferWriteState::Open),
            retained_byte_cap: retained_byte_cap.get(),
            retained_budget,
        })
    }

    pub fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, String> {
        self.require_open()?;
        self.retained_budget
            .try_reserve(self.key, self.retained_byte_cap, bytes)
    }

    pub fn writable_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.retained_budget.writable)
    }

    #[allow(
        dead_code,
        reason = "Legacy no-credit writes remain for the fragment-addressed compatibility path."
    )]
    pub fn write_legacy(&self, result: FetchResult) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Legacy {
            return Err("typed result buffer cannot accept legacy result batches".to_string());
        }
        Ok(insert_with_credit(self.key, result, None))
    }

    pub fn write_legacy_with_credit(
        &self,
        result: FetchResult,
        credit: ResultWriteCredit,
    ) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Legacy {
            return Err("typed result buffer cannot accept legacy result batches".to_string());
        }
        let retained = fetch_result_bytes(&result);
        if retained != credit.bytes() {
            return Err(format!(
                "legacy result retained {retained} bytes but owns {} bytes of credit",
                credit.bytes()
            ));
        }
        Ok(insert_with_credit(self.key, result, Some(credit)))
    }

    #[allow(
        dead_code,
        reason = "No-credit typed writes are test-only compatibility support; native Task results require credit."
    )]
    pub fn write_typed(&self, payload: Vec<u8>) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Typed {
            return Err("legacy result buffer cannot accept typed payloads".to_string());
        }
        insert_typed_with_credit(self.key, Bytes::from(payload), None)
    }

    pub fn write_typed_with_credit(
        &self,
        encoded: BoundedExchangePayload,
        mut credit: ResultWriteCredit,
    ) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Typed {
            return Err("legacy result buffer cannot accept typed payloads".to_string());
        }
        if encoded.len() > encoded.retained_bytes() || encoded.retained_bytes() > credit.bytes() {
            return Err(format!(
                "typed result payload has {} visible bytes and {} retained bytes but owns {} bytes of credit",
                encoded.len(),
                encoded.retained_bytes(),
                credit.bytes()
            ));
        }
        credit.shrink_to(encoded.retained_bytes())?;
        let (payload, retained_bytes) = encoded.into_parts();
        debug_assert_eq!(retained_bytes, credit.bytes());
        insert_typed_with_credit(self.key, payload, Some(credit))
    }

    #[cfg(test)]
    fn write_typed_bytes_with_credit_for_test(
        &self,
        payload: Bytes,
        credit: ResultWriteCredit,
    ) -> Result<ResultPublication, String> {
        self.require_open()?;
        if self.mode != ResultBufferMode::Typed {
            return Err("legacy result buffer cannot accept typed payloads".to_string());
        }
        if payload.len() != credit.bytes() {
            return Err(format!(
                "test typed result retained {} bytes but owns {} bytes of credit",
                payload.len(),
                credit.bytes()
            ));
        }
        insert_typed_with_credit(self.key, payload, Some(credit))
    }

    #[cfg(test)]
    fn retained_bytes(&self) -> usize {
        self.retained_budget
            .state
            .lock()
            .expect("result retained budget lock")
            .stream_retained_bytes
            .get(&self.key)
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn high_water_bytes(&self) -> usize {
        self.retained_budget
            .state
            .lock()
            .expect("result retained budget lock")
            .process_high_water_bytes
    }

    pub fn finish(&self) -> Result<ResultPublication, String> {
        let mut state = self.state.lock().expect("result buffer write handle lock");
        match *state {
            ResultBufferWriteState::Open => {
                let publication = close_ok_for_key(self.key);
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
        let publication = match *state {
            ResultBufferWriteState::Open => match reason {
                ResultAbort::PrepareRollback | ResultAbort::NeverStarted => discard_key(self.key),
                ResultAbort::Failed(error) => close_error_for_key(self.key, error),
                ResultAbort::Cancelled(reason) => cancel_with_message_for_key(self.key, reason),
            },
            // Producer completion does not transfer ownership to an unbounded
            // global cache. A later task failure/cancellation must be able to
            // revoke unacknowledged output and return every retained credit.
            ResultBufferWriteState::Finished => discard_key(self.key),
            ResultBufferWriteState::Aborted => return ResultPublication::NoChange,
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
    #[allow(
        dead_code,
        reason = "The structured legacy fetch error kind is read by protocol adapters outside this target."
    )]
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
    last_delivered_typed_packet: Option<i64>,
    last_acknowledged_typed_packet: Option<i64>,
    typed_eos_packet: Option<TypedFetchResult>,
    typed_change: Arc<Notify>,
    mem_tracker: Option<Arc<MemTracker>>,
    #[allow(
        dead_code,
        reason = "Legacy EOS templates are retained for protocol adapters outside the backend lib test configuration."
    )]
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
            last_delivered_typed_packet: None,
            last_acknowledged_typed_packet: None,
            typed_eos_packet: None,
            typed_change: Arc::new(Notify::new()),
            mem_tracker: None,
            eos_template: None,
        }
    }

    fn acknowledged_typed_eos(&self) -> Option<i64> {
        let sequence = self.typed_eos_packet.as_ref()?.packet_seq;
        (self.last_acknowledged_typed_packet == Some(sequence)).then_some(sequence)
    }

    #[allow(
        dead_code,
        reason = "Legacy EOS construction is retained for protocol adapters outside the backend lib test configuration."
    )]
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
            payload: Bytes::new(),
        }
    }

    #[allow(
        dead_code,
        reason = "Legacy result dequeue is retained for protocol adapters outside the backend lib test configuration."
    )]
    fn pop_next(&mut self) -> Option<FetchResult> {
        let out = self.queue.pop_front()?;
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
    _result_credit: Option<ResultWriteCredit>,
}

impl TrackedFetchResult {
    fn new(
        result: FetchResult,
        tracker: Option<&Arc<MemTracker>>,
        result_credit: Option<ResultWriteCredit>,
    ) -> Self {
        let accounting = tracker.map(|tracker| {
            let bytes = fetch_result_bytes(&result);
            TrackedBytes::new(bytes, Arc::clone(tracker))
        });
        Self {
            result,
            accounting,
            _result_credit: result_credit,
        }
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

    #[allow(
        dead_code,
        reason = "Legacy result conversion is retained for protocol adapters outside the backend lib test configuration."
    )]
    fn into_result(self, seq: i64) -> FetchResult {
        let TrackedFetchResult {
            mut result,
            accounting: _accounting,
            _result_credit,
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
    pub(crate) payload: Bytes,
}

#[derive(Debug)]
struct TrackedTypedFetchResult {
    result: TypedFetchResult,
    accounting: Option<TrackedBytes>,
    _result_credit: Option<ResultWriteCredit>,
}

impl TrackedTypedFetchResult {
    fn new(
        result: TypedFetchResult,
        tracker: Option<&Arc<MemTracker>>,
        result_credit: Option<ResultWriteCredit>,
    ) -> Self {
        let accounting = tracker.map(|tracker| {
            let bytes = typed_fetch_result_bytes(&result);
            TrackedBytes::new(bytes, Arc::clone(tracker))
        });
        Self {
            result,
            accounting,
            _result_credit: result_credit,
        }
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
}

fn typed_fetch_result_bytes(result: &TypedFetchResult) -> usize {
    result.payload.len()
}

struct ResultCtx {
    mu: Mutex<HashMap<ResultBufferKey, BufferControlBlock>>,
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

#[allow(
    dead_code,
    reason = "Legacy fragment-addressed insertion remains for compatibility adapters and tests."
)]
pub(crate) fn insert(finst_id: UniqueId, result: FetchResult) -> ResultPublication {
    insert_with_credit(ResultBufferKey::LegacyFragment(finst_id), result, None)
}

fn insert_with_credit(
    key: ResultBufferKey,
    result: FetchResult,
    result_credit: Option<ResultWriteCredit>,
) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard.entry(key).or_insert_with(BufferControlBlock::new);
        if block.closed_ok || block.status_error.is_some() || block.cancelled {
            return ResultPublication::NoChange;
        }
        if block.set_mode(ResultBufferMode::Legacy).is_ok() {
            let tracked =
                TrackedFetchResult::new(result, block.mem_tracker.as_ref(), result_credit);
            block.queue.push_back(tracked);
            publication = ResultPublication::DataReady;
        } else {
            block.fail_mode_mismatch(ResultBufferMode::Legacy);
        }
    }
    notify_waiters();
    publication
}

#[allow(
    dead_code,
    reason = "Legacy fragment-addressed insertion remains for compatibility adapters and tests."
)]
pub(crate) fn insert_typed(
    finst_id: UniqueId,
    payload: Vec<u8>,
) -> Result<ResultPublication, String> {
    insert_typed_with_credit(
        ResultBufferKey::LegacyFragment(finst_id),
        Bytes::from(payload),
        None,
    )
}

#[cfg(test)]
pub(crate) fn insert_task_typed(
    identity: TaskIdentity,
    payload: Vec<u8>,
) -> Result<ResultPublication, String> {
    insert_typed_with_credit(ResultBufferKey::Task(identity), Bytes::from(payload), None)
}

pub(crate) fn insert_typed_with_credit(
    key: ResultBufferKey,
    payload: Bytes,
    result_credit: Option<ResultWriteCredit>,
) -> Result<ResultPublication, String> {
    let c = ctx();
    let changed = {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard.entry(key).or_insert_with(BufferControlBlock::new);
        if block.closed_ok || block.status_error.is_some() || block.cancelled {
            return Ok(ResultPublication::NoChange);
        }
        block.set_mode(ResultBufferMode::Typed)?;
        let result = TypedFetchResult {
            packet_seq: block.next_packet_seq,
            eos: false,
            payload,
        };
        block.next_packet_seq += 1;
        let tracked =
            TrackedTypedFetchResult::new(result, block.mem_tracker.as_ref(), result_credit);
        block.typed_queue.push_back(tracked);
        Arc::clone(&block.typed_change)
    };
    changed.notify_one();
    notify_waiters();
    Ok(ResultPublication::DataReady)
}

#[allow(
    dead_code,
    reason = "Legacy fragment-addressed completion remains for compatibility adapters and tests."
)]
pub(crate) fn close_ok(finst_id: UniqueId) -> ResultPublication {
    close_ok_for_key(ResultBufferKey::LegacyFragment(finst_id))
}

#[cfg(test)]
pub(crate) fn close_task_ok(identity: TaskIdentity) -> ResultPublication {
    close_ok_for_key(ResultBufferKey::Task(identity))
}

fn close_ok_for_key(key: ResultBufferKey) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    let changed = {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard.entry(key).or_insert_with(BufferControlBlock::new);
        if !block.closed_ok && block.status_error.is_none() && !block.cancelled {
            block.closed_ok = true;
            publication = ResultPublication::TerminalReady;
        }
        Arc::clone(&block.typed_change)
    };
    changed.notify_one();
    notify_waiters();
    publication
}

#[allow(
    dead_code,
    reason = "Legacy fragment-addressed failure remains for compatibility adapters and tests."
)]
pub(crate) fn close_error(finst_id: UniqueId, message: String) -> ResultPublication {
    close_error_for_key(ResultBufferKey::LegacyFragment(finst_id), message)
}

fn close_error_for_key(key: ResultBufferKey, message: String) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    let changed = {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard.entry(key).or_insert_with(BufferControlBlock::new);
        if !block.closed_ok && block.status_error.is_none() && !block.cancelled {
            block.status_error = Some(message);
            block.queue.clear();
            block.typed_queue.clear();
            publication = ResultPublication::TerminalReady;
        }
        Arc::clone(&block.typed_change)
    };
    changed.notify_one();
    notify_waiters();
    publication
}

#[allow(
    dead_code,
    reason = "Legacy result cancellation remains available to protocol adapters outside this target."
)]
pub(crate) fn cancel(finst_id: UniqueId) -> ResultPublication {
    cancel_with_message(finst_id, "Cancelled".to_string())
}

fn cancel_with_message(finst_id: UniqueId, message: String) -> ResultPublication {
    cancel_with_message_for_key(ResultBufferKey::LegacyFragment(finst_id), message)
}

fn cancel_with_message_for_key(key: ResultBufferKey, message: String) -> ResultPublication {
    let c = ctx();
    let mut publication = ResultPublication::NoChange;
    let changed = {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard.entry(key).or_insert_with(BufferControlBlock::new);
        if !block.closed_ok && block.status_error.is_none() && !block.cancelled {
            block.cancelled = true;
            block.cancel_message = Some(message);
            block.queue.clear();
            block.typed_queue.clear();
            publication = ResultPublication::TerminalReady;
        }
        Arc::clone(&block.typed_change)
    };
    changed.notify_one();
    notify_waiters();
    publication
}

#[allow(
    dead_code,
    reason = "Legacy sender creation remains available to protocol adapters outside this target."
)]
pub(crate) fn create_sender(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(ResultBufferKey::LegacyFragment(finst_id))
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Legacy);
}

#[allow(
    dead_code,
    reason = "Legacy fallible sender creation remains available to protocol adapters outside this target."
)]
pub(crate) fn try_create_sender(finst_id: UniqueId) -> Result<(), String> {
    try_create_sender_with_mode(finst_id, ResultBufferMode::Legacy)
}

#[allow(
    dead_code,
    reason = "Typed sender creation remains available to protocol adapters outside this target."
)]
pub(crate) fn create_typed_sender(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(ResultBufferKey::LegacyFragment(finst_id))
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Typed);
}

#[cfg(test)]
pub(crate) fn create_task_typed_sender(identity: TaskIdentity) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(ResultBufferKey::Task(identity))
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Typed);
}

#[allow(
    dead_code,
    reason = "Typed fallible sender creation remains available to protocol adapters outside this target."
)]
pub(crate) fn try_create_typed_sender(finst_id: UniqueId) -> Result<(), String> {
    try_create_sender_with_mode(finst_id, ResultBufferMode::Typed)
}

fn try_create_sender_with_mode(finst_id: UniqueId, mode: ResultBufferMode) -> Result<(), String> {
    try_create_sender_with_key_mode(ResultBufferKey::LegacyFragment(finst_id), mode)
}

fn try_create_sender_with_key_mode(
    key: ResultBufferKey,
    mode: ResultBufferMode,
) -> Result<(), String> {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    if guard.contains_key(&key) {
        return Err(format!("result buffer already registered for {key:?}"));
    }
    let mut block = BufferControlBlock::new();
    block.set_mode(mode)?;
    guard.insert(key, block);
    Ok(())
}

#[allow(
    dead_code,
    reason = "Legacy fragment-addressed cleanup remains for compatibility adapters and tests."
)]
pub(crate) fn discard(finst_id: UniqueId) -> ResultPublication {
    discard_key(ResultBufferKey::LegacyFragment(finst_id))
}

pub(crate) fn discard_task(identity: TaskIdentity) -> ResultPublication {
    discard_key(ResultBufferKey::Task(identity))
}

/// Retires one task's result payload while preserving only an acknowledged EOS
/// replay record. The record is zero-byte and is reclaimed with the owning
/// context or the task retention fence.
pub(crate) fn retire_task_result(identity: TaskIdentity) -> ResultPublication {
    let key = ResultBufferKey::Task(identity);
    let c = ctx();
    let removed = {
        let mut guard = c.mu.lock().expect("ctx lock");
        match guard.get(&key) {
            Some(block) if block.acknowledged_typed_eos().is_some() => None,
            _ => guard.remove(&key),
        }
    };
    if let Some(block) = removed.as_ref() {
        block.typed_change.notify_one();
        c.cvar.notify_all();
        ResultPublication::Removed
    } else {
        ResultPublication::NoChange
    }
}

/// Returns whether a retained task result proves this exact terminal ACK.
///
/// This read is intentionally sequence-sensitive. It is used only after the
/// task registry has proved that the retired task owned the root result.
pub(crate) fn replays_task_terminal_ack(
    identity: TaskIdentity,
    acknowledged_packet_seq: Option<i64>,
) -> bool {
    let Some(acknowledged) = acknowledged_packet_seq else {
        return false;
    };
    ctx()
        .mu
        .lock()
        .expect("ctx lock")
        .get(&ResultBufferKey::Task(identity))
        .and_then(BufferControlBlock::acknowledged_typed_eos)
        == Some(acknowledged)
}

fn discard_key(key: ResultBufferKey) -> ResultPublication {
    let c = ctx();
    let removed = c.mu.lock().expect("ctx lock").remove(&key);
    if let Some(block) = removed.as_ref() {
        block.typed_change.notify_one();
    }
    c.cvar.notify_all();
    ResultPublication::Removed
}

#[allow(
    dead_code,
    reason = "Legacy registration inspection remains available to protocol adapters outside this target."
)]
pub(crate) fn is_registered(finst_id: UniqueId) -> bool {
    ctx()
        .mu
        .lock()
        .expect("ctx lock")
        .contains_key(&ResultBufferKey::LegacyFragment(finst_id))
}

#[allow(
    dead_code,
    reason = "Legacy fragment-addressed accounting remains for compatibility adapters."
)]
pub(crate) fn set_mem_tracker(finst_id: UniqueId, tracker: Arc<MemTracker>) {
    set_mem_tracker_for_key(ResultBufferKey::LegacyFragment(finst_id), tracker)
}

fn set_mem_tracker_for_key(key: ResultBufferKey, tracker: Arc<MemTracker>) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard.entry(key).or_insert_with(BufferControlBlock::new);
    block.mem_tracker = Some(Arc::clone(&tracker));
    for result in block.queue.iter_mut() {
        result.set_mem_tracker(Arc::clone(&tracker));
    }
    for result in block.typed_queue.iter_mut() {
        result.set_mem_tracker(Arc::clone(&tracker));
    }
}

#[allow(
    dead_code,
    reason = "Legacy EOS templates remain available to protocol adapters outside this target."
)]
pub(crate) fn set_eos_template(finst_id: UniqueId, template: ResultBatch) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(ResultBufferKey::LegacyFragment(finst_id))
        .or_insert_with(BufferControlBlock::new);
    block.fail_mode_mismatch(ResultBufferMode::Legacy);
    block.eos_template = Some(template);
}

#[allow(
    dead_code,
    reason = "Legacy fetch outcomes are retained for protocol adapters outside the backend lib test configuration."
)]
#[derive(Debug)]
pub enum TryFetchResult {
    Ready(FetchResult),
    NotReady,
    Error(FetchError),
}

#[derive(Debug)]
pub(crate) enum TryFetchTypedResult {
    Ready(TypedFetchResult),
    EndAcknowledged,
    NotReady,
    Error(FetchError),
}

/// Inner fetch logic that works on an already-held HashMap guard.
///
/// Separating this from `try_fetch` allows `wait_fetch` to check state
/// while holding the lock, avoiding the missed-wakeup race that would
/// arise if the check and the condvar wait were not atomic with respect
/// to the mutex.
#[allow(
    dead_code,
    reason = "Legacy fetch polling remains available to protocol adapters outside this target."
)]
fn try_fetch_inner(
    guard: &mut HashMap<ResultBufferKey, BufferControlBlock>,
    finst_id: UniqueId,
) -> TryFetchResult {
    let key = ResultBufferKey::LegacyFragment(finst_id);
    let Some(block) = guard.get_mut(&key) else {
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
        guard.remove(&key);
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::Cancelled,
            message: msg,
        });
    }
    if let Some(msg) = block.status_error.as_ref() {
        let msg = msg.clone();
        guard.remove(&key);
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
        guard.remove(&key);
        return TryFetchResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "result stream already reached eos".to_string(),
        });
    }
    TryFetchResult::NotReady
}

fn try_fetch_typed_inner(
    guard: &mut HashMap<ResultBufferKey, BufferControlBlock>,
    key: ResultBufferKey,
    acknowledged_packet_seq: Option<i64>,
    max_result_bytes: ResultByteLimit,
) -> TryFetchTypedResult {
    let Some(block) = guard.get_mut(&key) else {
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
        guard.remove(&key);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Cancelled,
            message: msg,
        });
    }
    if let Some(msg) = block.status_error.as_ref() {
        let msg = msg.clone();
        guard.remove(&key);
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

    if let Some(acknowledged) = acknowledged_packet_seq {
        if block.last_acknowledged_typed_packet == Some(acknowledged) {
            if block.acknowledged_typed_eos() == Some(acknowledged) {
                return TryFetchTypedResult::EndAcknowledged;
            }
            // An exact retry of a data acknowledgement is idempotent. The same
            // current packet is returned below.
        } else if block.last_delivered_typed_packet != Some(acknowledged) {
            return TryFetchTypedResult::Error(FetchError {
                kind: FetchErrorKind::Failed,
                message: format!(
                    "result acknowledgement {acknowledged} does not name the in-flight packet {:?}",
                    block.last_delivered_typed_packet
                ),
            });
        } else if block
            .typed_eos_packet
            .as_ref()
            .is_some_and(|packet| packet.packet_seq == acknowledged)
        {
            block.last_acknowledged_typed_packet = Some(acknowledged);
            return TryFetchTypedResult::EndAcknowledged;
        } else {
            let Some(front) = block.typed_queue.front() else {
                return TryFetchTypedResult::Error(FetchError {
                    kind: FetchErrorKind::Failed,
                    message: format!(
                        "result acknowledgement {acknowledged} has no retained packet"
                    ),
                });
            };
            if front.result.packet_seq != acknowledged {
                return TryFetchTypedResult::Error(FetchError {
                    kind: FetchErrorKind::Failed,
                    message: format!(
                        "result acknowledgement {acknowledged} skips retained packet {}",
                        front.result.packet_seq
                    ),
                });
            }
            block.typed_queue.pop_front();
            block.last_acknowledged_typed_packet = Some(acknowledged);
            block.last_delivered_typed_packet = None;
        }
    }

    if let Some(result) = block.typed_queue.front() {
        let payload_bytes = u64::try_from(result.result.payload.len()).unwrap_or(u64::MAX);
        if payload_bytes > max_result_bytes.get() {
            return TryFetchTypedResult::Error(FetchError {
                kind: FetchErrorKind::Failed,
                message: format!(
                    "retained result packet {} has {payload_bytes} payload bytes, exceeding the requested limit {}",
                    result.result.packet_seq,
                    max_result_bytes.get()
                ),
            });
        }
        let result = result.result.clone();
        block.last_delivered_typed_packet = Some(result.packet_seq);
        return TryFetchTypedResult::Ready(result);
    }
    if block.closed_ok {
        let result = match block.typed_eos_packet.as_ref() {
            Some(result) => result.clone(),
            None => {
                let result = block.make_typed_eos_result();
                block.typed_eos_packet = Some(result.clone());
                result
            }
        };
        block.last_delivered_typed_packet = Some(result.packet_seq);
        return TryFetchTypedResult::Ready(result);
    }
    TryFetchTypedResult::NotReady
}

fn try_fetch_typed_legacy_inner(
    guard: &mut HashMap<ResultBufferKey, BufferControlBlock>,
    finst_id: UniqueId,
) -> TryFetchTypedResult {
    let key = ResultBufferKey::LegacyFragment(finst_id);
    let Some(block) = guard.get_mut(&key) else {
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "no result for this query".to_string(),
        });
    };
    if block.cancelled {
        let message = block
            .cancel_message
            .clone()
            .unwrap_or_else(|| "Cancelled".to_string());
        guard.remove(&key);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Cancelled,
            message,
        });
    }
    if let Some(message) = block.status_error.clone() {
        guard.remove(&key);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Failed,
            message,
        });
    }
    if block.mode == Some(ResultBufferMode::Legacy) {
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::Failed,
            message: "legacy result buffer cannot be fetched as typed Arrow IPC".to_string(),
        });
    }
    if let Some(result) = block.typed_queue.pop_front() {
        return TryFetchTypedResult::Ready(result.result);
    }
    if block.closed_ok && !block.eos_sent {
        block.eos_sent = true;
        return TryFetchTypedResult::Ready(block.make_typed_eos_result());
    }
    if block.closed_ok {
        guard.remove(&key);
        return TryFetchTypedResult::Error(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "result stream already reached eos".to_string(),
        });
    }
    TryFetchTypedResult::NotReady
}

#[allow(
    dead_code,
    reason = "Legacy fetch polling remains available to protocol adapters outside this target."
)]
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
#[allow(
    dead_code,
    reason = "Legacy long-poll fetching remains available to protocol adapters outside this target."
)]
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

async fn wait_fetch_typed_for_key(
    key: ResultBufferKey,
    acknowledged_packet_seq: Option<i64>,
    max_wait: Duration,
    max_result_bytes: ResultByteLimit,
) -> TryFetchTypedResult {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let (result, change) = {
            let c = ctx();
            let mut guard = c.mu.lock().expect("ctx lock");
            let change = guard.get(&key).map(|block| Arc::clone(&block.typed_change));
            (
                try_fetch_typed_inner(&mut guard, key, acknowledged_packet_seq, max_result_bytes),
                change,
            )
        };
        if !matches!(result, TryFetchTypedResult::NotReady) || max_wait.is_zero() {
            return result;
        }
        let Some(change) = change else {
            return result;
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return TryFetchTypedResult::NotReady;
        }
        if tokio::time::timeout(remaining, change.notified())
            .await
            .is_err()
        {
            return TryFetchTypedResult::NotReady;
        }
    }
}

pub(crate) async fn wait_fetch_task_typed(
    identity: TaskIdentity,
    acknowledged_packet_seq: Option<i64>,
    max_wait: Duration,
    max_result_bytes: ResultByteLimit,
) -> TryFetchTypedResult {
    wait_fetch_typed_for_key(
        ResultBufferKey::Task(identity),
        acknowledged_packet_seq,
        max_wait,
        max_result_bytes,
    )
    .await
}

#[cfg(test)]
async fn wait_fetch_typed(
    finst_id: UniqueId,
    acknowledged_packet_seq: Option<i64>,
    max_wait: Duration,
) -> TryFetchTypedResult {
    wait_fetch_typed_for_key(
        ResultBufferKey::LegacyFragment(finst_id),
        acknowledged_packet_seq,
        max_wait,
        ResultByteLimit::new(u64::MAX).expect("the test fetch limit is nonzero"),
    )
    .await
}

pub(crate) fn wait_fetch_typed_legacy(finst_id: UniqueId, max_wait_ms: i64) -> TryFetchTypedResult {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let initial = try_fetch_typed_legacy_inner(&mut guard, finst_id);
    if !matches!(initial, TryFetchTypedResult::NotReady) || max_wait_ms <= 0 {
        return initial;
    }
    let deadline = std::time::Instant::now() + Duration::from_millis(max_wait_ms as u64);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return TryFetchTypedResult::NotReady;
        }
        let (mut next_guard, _) = c.cvar.wait_timeout(guard, remaining).expect("condvar wait");
        let result = try_fetch_typed_legacy_inner(&mut next_guard, finst_id);
        if !matches!(result, TryFetchTypedResult::NotReady) {
            return result;
        }
        guard = next_guard;
    }
}

#[allow(
    dead_code,
    reason = "Legacy fetch timeout fallback remains available to protocol adapters outside this target."
)]
fn fallback_fetch_wait_timeout() -> Duration {
    Duration::from_secs(300)
}

#[allow(
    dead_code,
    reason = "Legacy fetch timeout lookup remains available to protocol adapters outside this target."
)]
pub fn fetch_wait_timeout(finst_id: UniqueId) -> Duration {
    use crate::runtime::query_context::query_context_manager;

    query_context_manager()
        .get_query_timeout_by_finst(finst_id)
        .unwrap_or_else(fallback_fetch_wait_timeout)
}

#[allow(
    dead_code,
    reason = "Legacy millisecond timeout lookup remains available to protocol adapters outside this target."
)]
pub fn fetch_wait_timeout_ms(finst_id: UniqueId) -> i64 {
    let millis = fetch_wait_timeout(finst_id).as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use novarocks_execution::runtime::exchange::encode_chunks_bounded;
    use novarocks_execution::runtime::fragment::io::ResultAbort;
    use novarocks_types::SlotId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId as NativeQueryId, StageId, TaskId,
    };

    fn test_result_cap() -> NonZeroUsize {
        NonZeroUsize::new(1024 * 1024).expect("nonzero test result cap")
    }

    fn test_fetch_limit() -> ResultByteLimit {
        ResultByteLimit::new(test_result_cap().get() as u64).expect("nonzero test fetch limit")
    }

    fn test_result_budget() -> Arc<ResultRetainedBudget> {
        ResultRetainedBudget::new(test_result_cap())
    }

    fn test_typed_chunk() -> Chunk {
        let slot_id = SlotId::new(1);
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot_id,
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .expect("test chunk schema"),
        );
        Chunk::try_new_with_columns(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("test chunk")
    }

    fn test_task_identity(seed: u32) -> TaskIdentity {
        let query_execution = QueryExecutionId::new(
            NativeQueryId::new(i64::from(seed), i64::from(seed) + 1),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query identity");
        TaskIdentity::new(
            query_execution,
            StageId::new(seed).expect("nonzero stage"),
            TaskId::new(seed).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn fragment_result_session_writes_twenty_plus_seventeen_rows_and_finishes_once() {
        let finst_id = UniqueId::new(9901, 9902);
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            false,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open result");
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
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            false,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open result");

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
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            false,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open result");

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
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(rolled_back),
            false,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open result");
        assert_eq!(
            handle.abort(ResultAbort::PrepareRollback),
            ResultPublication::Removed
        );
        assert!(!is_registered(rolled_back));
    }

    #[test]
    fn fragment_result_session_failure_preserves_execution_error() {
        let finst_id = UniqueId::new(9907, 9908);
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            false,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open result");

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
    fn fragment_result_session_late_abort_discards_finished_result() {
        let finst_id = UniqueId::new(9905, 9906);
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            false,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open result");
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
            ResultPublication::Removed
        );

        let TryFetchResult::Error(error) = try_fetch(finst_id) else {
            panic!("late abort must revoke a finished result");
        };
        assert!(matches!(error.kind, FetchErrorKind::NotFound));
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

    #[tokio::test]
    async fn typed_fetch_replays_until_ack_and_releases_eof_on_final_ack() {
        let finst_id = UniqueId::new(701, 702);
        create_typed_sender(finst_id);
        insert_typed(finst_id, vec![1, 2, 3]).expect("insert first typed payload");
        insert_typed(finst_id, vec![4, 5]).expect("insert second typed payload");
        close_ok(finst_id);

        let TryFetchTypedResult::Ready(first) =
            wait_fetch_typed(finst_id, None, Duration::ZERO).await
        else {
            panic!("expected first typed payload");
        };
        assert_eq!(first.packet_seq, 0);
        assert!(!first.eos);
        assert_eq!(first.payload, Bytes::from_static(&[1, 2, 3]));

        let TryFetchTypedResult::Ready(replayed) =
            wait_fetch_typed(finst_id, None, Duration::ZERO).await
        else {
            panic!("expected exact replay of the first typed payload");
        };
        assert_eq!(replayed.packet_seq, first.packet_seq);
        assert_eq!(replayed.payload, first.payload);
        assert_eq!(
            replayed.payload.as_ptr(),
            first.payload.as_ptr(),
            "replay must share the retained Bytes allocation"
        );

        let TryFetchTypedResult::Ready(second) =
            wait_fetch_typed(finst_id, Some(first.packet_seq), Duration::ZERO).await
        else {
            panic!("expected second typed payload");
        };
        assert_eq!(second.packet_seq, 1);
        assert!(!second.eos);
        assert_eq!(second.payload, Bytes::from_static(&[4, 5]));

        let TryFetchTypedResult::Ready(eos) =
            wait_fetch_typed(finst_id, Some(second.packet_seq), Duration::ZERO).await
        else {
            panic!("expected typed eos");
        };
        assert_eq!(eos.packet_seq, 2);
        assert!(eos.eos);
        assert!(eos.payload.is_empty());

        assert!(matches!(
            wait_fetch_typed(finst_id, Some(eos.packet_seq), Duration::ZERO).await,
            TryFetchTypedResult::EndAcknowledged
        ));
        assert!(matches!(
            wait_fetch_typed(finst_id, Some(eos.packet_seq), Duration::ZERO).await,
            TryFetchTypedResult::EndAcknowledged
        ));
    }

    #[tokio::test]
    async fn acknowledged_task_eos_replays_until_controlled_cleanup() {
        let identity = test_task_identity(721);
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::Task(identity),
            true,
            test_result_cap(),
            test_result_budget(),
            None,
        )
        .expect("open task result");
        handle.finish().expect("finish task result");

        let TryFetchTypedResult::Ready(eos) =
            wait_fetch_task_typed(identity, None, Duration::ZERO, test_fetch_limit()).await
        else {
            panic!("finished task publishes eos");
        };
        assert!(eos.eos);
        assert!(matches!(
            wait_fetch_task_typed(
                identity,
                Some(eos.packet_seq),
                Duration::ZERO,
                test_fetch_limit(),
            )
            .await,
            TryFetchTypedResult::EndAcknowledged
        ));
        assert!(replays_task_terminal_ack(identity, Some(eos.packet_seq)));

        assert_eq!(retire_task_result(identity), ResultPublication::NoChange);
        assert!(matches!(
            wait_fetch_task_typed(
                identity,
                Some(eos.packet_seq),
                Duration::ZERO,
                test_fetch_limit(),
            )
            .await,
            TryFetchTypedResult::EndAcknowledged
        ));

        assert_eq!(discard_task(identity), ResultPublication::Removed);
        assert!(!replays_task_terminal_ack(identity, Some(eos.packet_seq)));
    }

    #[tokio::test]
    async fn task_retirement_discards_finished_unacknowledged_output_and_credit() {
        let identity = test_task_identity(722);
        let cap = NonZeroUsize::new(8).expect("nonzero cap");
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::Task(identity),
            true,
            cap,
            ResultRetainedBudget::new(cap),
            None,
        )
        .expect("open task result");
        let ResultWriteAdmission::Granted(credit) = handle.try_acquire(8).expect("reserve") else {
            panic!("task owns its result capacity");
        };
        handle
            .write_typed_bytes_with_credit_for_test(Bytes::from_static(b"12345678"), credit)
            .expect("retain task result");
        handle.finish().expect("producer finishes");
        assert_eq!(handle.retained_bytes(), 8);

        assert_eq!(retire_task_result(identity), ResultPublication::Removed);
        assert_eq!(handle.retained_bytes(), 0);
        assert!(matches!(
            wait_fetch_task_typed(identity, None, Duration::ZERO, test_fetch_limit()).await,
            TryFetchTypedResult::Error(FetchError {
                kind: FetchErrorKind::NotFound,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn task_fetch_limit_refuses_without_delivering_or_releasing_the_retained_packet() {
        let identity = test_task_identity(723);
        let retained_cap = NonZeroUsize::new(8).expect("nonzero cap");
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::Task(identity),
            true,
            retained_cap,
            ResultRetainedBudget::new(retained_cap),
            None,
        )
        .expect("open task result");
        let ResultWriteAdmission::Granted(credit) = handle.try_acquire(8).expect("reserve") else {
            panic!("task owns its result capacity");
        };
        handle
            .write_typed_bytes_with_credit_for_test(Bytes::from_static(b"12345678"), credit)
            .expect("retain task result");

        let small = ResultByteLimit::new(7).expect("small positive limit");
        assert!(matches!(
            wait_fetch_task_typed(identity, None, Duration::ZERO, small).await,
            TryFetchTypedResult::Error(FetchError {
                kind: FetchErrorKind::Failed,
                ..
            })
        ));
        assert_eq!(handle.retained_bytes(), 8);
        assert!(matches!(
            wait_fetch_task_typed(identity, Some(0), Duration::ZERO, test_fetch_limit()).await,
            TryFetchTypedResult::Error(FetchError {
                kind: FetchErrorKind::Failed,
                ..
            })
        ));

        let TryFetchTypedResult::Ready(packet) =
            wait_fetch_task_typed(identity, None, Duration::ZERO, test_fetch_limit()).await
        else {
            panic!("a sufficient limit receives the retained packet");
        };
        assert_eq!(packet.packet_seq, 0);
        assert_eq!(packet.payload.as_ref(), b"12345678");
        assert_eq!(handle.retained_bytes(), 8);
        discard_task(identity);
    }

    #[tokio::test]
    async fn typed_fetch_rejects_future_and_skipping_acknowledgements() {
        let finst_id = UniqueId::new(707, 708);
        create_typed_sender(finst_id);
        insert_typed(finst_id, vec![1]).expect("insert first typed payload");
        insert_typed(finst_id, vec![2]).expect("insert second typed payload");

        let TryFetchTypedResult::Ready(first) =
            wait_fetch_typed(finst_id, None, Duration::ZERO).await
        else {
            panic!("expected first packet");
        };
        assert_eq!(first.packet_seq, 0);
        let TryFetchTypedResult::Error(error) =
            wait_fetch_typed(finst_id, Some(1), Duration::ZERO).await
        else {
            panic!("future acknowledgement must fail closed");
        };
        assert!(error.message.contains("in-flight packet"));
    }

    #[tokio::test]
    async fn typed_ack_returns_shared_process_credit_and_wakes_writer() {
        let first_id = UniqueId::new(709, 710);
        let second_id = UniqueId::new(711, 712);
        let stream_cap = NonZeroUsize::new(8).expect("nonzero stream cap");
        let process_budget =
            ResultRetainedBudget::new(NonZeroUsize::new(12).expect("nonzero process cap"));
        let first = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(first_id),
            true,
            stream_cap,
            Arc::clone(&process_budget),
            None,
        )
        .expect("open first stream");
        let second = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(second_id),
            true,
            stream_cap,
            Arc::clone(&process_budget),
            None,
        )
        .expect("open second stream");

        let ResultWriteAdmission::Granted(credit) = first.try_acquire(8).expect("reserve first")
        else {
            panic!("first stream fits the process budget");
        };
        first
            .write_typed_bytes_with_credit_for_test(Bytes::from_static(b"12345678"), credit)
            .expect("retain first packet");
        assert!(matches!(
            second.try_acquire(8).expect("second admission is settled"),
            ResultWriteAdmission::Blocked
        ));
        let writable = second.writable_observable();
        let generation = writable.generation();

        let TryFetchTypedResult::Ready(packet) =
            wait_fetch_typed(first_id, None, Duration::ZERO).await
        else {
            panic!("first packet is ready");
        };
        assert!(matches!(
            wait_fetch_typed(first_id, Some(packet.packet_seq), Duration::ZERO).await,
            TryFetchTypedResult::NotReady
        ));
        assert!(writable.generation() > generation);
        assert_eq!(first.retained_bytes(), 0);

        assert!(matches!(
            second.try_acquire(8).expect("released process bytes"),
            ResultWriteAdmission::Granted(_)
        ));
        assert_eq!(second.high_water_bytes(), 8);
    }

    #[test]
    fn typed_write_retains_backing_capacity_credit_beyond_visible_length() {
        let finst_id = UniqueId::new(719, 720);
        let cap = NonZeroUsize::new(64 * 1024).expect("nonzero cap");
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            true,
            cap,
            ResultRetainedBudget::new(cap),
            None,
        )
        .expect("open stream");
        let encoded = encode_chunks_bounded(&[test_typed_chunk()], true, cap.get())
            .expect("bounded typed encoding");
        assert!(encoded.retained_bytes() > encoded.len());
        let retained_bytes = encoded.retained_bytes();
        let ResultWriteAdmission::Granted(credit) =
            handle.try_acquire(cap.get()).expect("reserve packet cap")
        else {
            panic!("packet cap fits the stream");
        };

        handle
            .write_typed_with_credit(encoded, credit)
            .expect("retain bounded typed payload");
        assert_eq!(handle.retained_bytes(), retained_bytes);

        handle.abort(ResultAbort::Cancelled("test cleanup".to_string()));
        assert_eq!(handle.retained_bytes(), 0);
    }

    #[test]
    fn single_result_reservation_above_stream_cap_fails_closed() {
        let finst_id = UniqueId::new(713, 714);
        let cap = NonZeroUsize::new(8).expect("nonzero cap");
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            true,
            cap,
            ResultRetainedBudget::new(cap),
            None,
        )
        .expect("open stream");
        let error = handle
            .try_acquire(9)
            .expect_err("one packet cannot exceed its stream cap");
        assert!(error.contains("exceeds stream retained-byte cap"));
        assert_eq!(handle.retained_bytes(), 0);
    }

    #[test]
    fn cancellation_releases_retained_credit_and_wakes_capacity_waiters() {
        let finst_id = UniqueId::new(715, 716);
        let cap = NonZeroUsize::new(8).expect("nonzero cap");
        let handle = ResultBufferWriteHandle::open(
            ResultBufferKey::LegacyFragment(finst_id),
            true,
            cap,
            ResultRetainedBudget::new(cap),
            None,
        )
        .expect("open stream");
        let writable = handle.writable_observable();
        let generation = writable.generation();
        let ResultWriteAdmission::Granted(credit) = handle.try_acquire(8).expect("reserve") else {
            panic!("stream has its full capacity");
        };
        handle
            .write_typed_bytes_with_credit_for_test(Bytes::from_static(b"12345678"), credit)
            .expect("retain packet");
        assert_eq!(handle.retained_bytes(), 8);
        handle
            .finish()
            .expect("producer finishes before cancellation");

        assert_eq!(
            handle.abort(ResultAbort::Cancelled("cancel test".to_string())),
            ResultPublication::Removed
        );
        assert_eq!(handle.retained_bytes(), 0);
        assert!(writable.generation() > generation);
    }

    #[tokio::test]
    async fn typed_async_wait_is_woken_by_cancellation() {
        let finst_id = UniqueId::new(717, 718);
        create_typed_sender(finst_id);
        let wait = wait_fetch_typed(finst_id, None, Duration::from_secs(5));
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), wait.as_mut())
                .await
                .is_err(),
            "empty result stream should park"
        );

        cancel_with_message(finst_id, "cancelled while parked".to_string());
        let result = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("cancellation wakes the async fetch");
        let TryFetchTypedResult::Error(error) = result else {
            panic!("cancelled fetch reports an error");
        };
        assert!(matches!(error.kind, FetchErrorKind::Cancelled));
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

    #[tokio::test]
    async fn legacy_sender_rejects_typed_fetch() {
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

        let TryFetchTypedResult::Error(err) =
            wait_fetch_typed(finst_id, None, Duration::ZERO).await
        else {
            panic!("expected mode mismatch error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert!(err.message.contains("legacy"));
        assert!(err.message.contains("typed"));
    }
}
