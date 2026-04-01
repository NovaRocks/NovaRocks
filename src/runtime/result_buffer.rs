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
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::common::types::{FetchResult, UniqueId};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};

#[derive(Debug, Clone)]
pub(crate) enum FetchErrorKind {
    NotFound,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchError {
    pub(crate) kind: FetchErrorKind,
    pub(crate) message: String,
}

#[derive(Debug)]
struct BufferControlBlock {
    queue: VecDeque<TrackedFetchResult>,
    closed_ok: bool,
    eos_sent: bool,
    status_error: Option<String>,
    cancelled: bool,
    cancel_message: Option<String>,
    next_packet_seq: i64,
    mem_tracker: Option<Arc<MemTracker>>,
    eos_template: Option<crate::data::TResultBatch>,
}

impl BufferControlBlock {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
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
            result_batch: self
                .eos_template
                .clone()
                .unwrap_or_else(|| crate::data::TResultBatch::new(vec![], false, 0, None)),
        }
    }

    fn pop_next(&mut self) -> Option<FetchResult> {
        let out = self.queue.pop_front()?;
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;
        Some(out.into_result(seq))
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
    let mut total = 0usize;
    let rows = &result.result_batch.rows;
    total = total.saturating_add(
        rows.capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>()),
    );
    for row in rows {
        total = total.saturating_add(row.capacity().max(row.len()));
    }
    total
}

struct ResultCtx {
    mu: Mutex<HashMap<UniqueId, BufferControlBlock>>,
}

static CTX: OnceLock<ResultCtx> = OnceLock::new();

fn ctx() -> &'static ResultCtx {
    CTX.get_or_init(|| ResultCtx {
        mu: Mutex::new(HashMap::new()),
    })
}

#[cfg(all(feature = "compat", not(test)))]
unsafe extern "C" {
    fn novarocks_compat_notify_fetch_ready(finst_id_hi: i64, finst_id_lo: i64);
}

fn notify_fetch_ready(finst_id: UniqueId) {
    #[cfg(all(feature = "compat", not(test)))]
    unsafe {
        novarocks_compat_notify_fetch_ready(finst_id.hi, finst_id.lo);
    }

    #[cfg(not(all(feature = "compat", not(test))))]
    let _ = finst_id;
}

pub(crate) fn insert(finst_id: UniqueId, result: FetchResult) {
    let c = ctx();
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        let tracked = TrackedFetchResult::new(result, block.mem_tracker.as_ref());
        block.queue.push_back(tracked);
    }
    notify_fetch_ready(finst_id);
}

pub(crate) fn close_ok(finst_id: UniqueId) {
    let c = ctx();
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        block.closed_ok = true;
    }
    notify_fetch_ready(finst_id);
}

pub(crate) fn close_error(finst_id: UniqueId, message: String) {
    let c = ctx();
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        block.status_error = Some(message);
        block.queue.clear();
    }
    notify_fetch_ready(finst_id);
}

pub(crate) fn cancel(finst_id: UniqueId) {
    let c = ctx();
    {
        let mut guard = c.mu.lock().expect("ctx lock");
        let block = guard
            .entry(finst_id)
            .or_insert_with(BufferControlBlock::new);
        block.cancelled = true;
        if block.cancel_message.is_none() {
            block.cancel_message = Some("Cancelled".to_string());
        }
        block.queue.clear();
    }
    notify_fetch_ready(finst_id);
}

pub(crate) fn create_sender(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
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
}

pub(crate) fn set_eos_template(finst_id: UniqueId, template: crate::data::TResultBatch) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.eos_template = Some(template);
}

#[derive(Debug)]
pub(crate) enum TryFetchResult {
    Ready(FetchResult),
    NotReady,
    Error(FetchError),
}

pub(crate) fn try_fetch(finst_id: UniqueId) -> TryFetchResult {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
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

fn fallback_fetch_wait_timeout() -> Duration {
    Duration::from_secs(300)
}

pub(crate) fn fetch_wait_timeout(finst_id: UniqueId) -> Duration {
    use crate::runtime::query_context::query_context_manager;

    query_context_manager()
        .get_query_timeout_by_finst(finst_id)
        .unwrap_or_else(fallback_fetch_wait_timeout)
}

pub(crate) fn fetch_wait_timeout_ms(finst_id: UniqueId) -> i64 {
    let millis = fetch_wait_timeout(finst_id).as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::query_context::{QueryId, query_context_manager};

    #[test]
    fn cancel_is_observable() {
        let finst_id = UniqueId { hi: 42, lo: 7 };
        create_sender(finst_id);
        cancel(finst_id);

        let TryFetchResult::Error(err) = try_fetch(finst_id) else {
            panic!("expected cancel error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Cancelled));
    }

    #[test]
    fn close_error_is_observable() {
        let finst_id = UniqueId { hi: 1, lo: 2 };
        create_sender(finst_id);
        close_error(finst_id, "boom".to_string());

        let TryFetchResult::Error(err) = try_fetch(finst_id) else {
            panic!("expected close_error");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert_eq!(err.message, "boom");
    }

    #[test]
    fn try_fetch_returns_batches_in_order_and_then_eos() {
        let finst_id = UniqueId { hi: 7, lo: 9 };
        create_sender(finst_id);
        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: crate::data::TResultBatch::new(vec![b"a".to_vec()], false, 0, None),
            },
        );
        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: crate::data::TResultBatch::new(vec![b"b".to_vec()], false, 0, None),
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
        let finst_id = UniqueId { hi: 70, lo: 90 };
        create_sender(finst_id);
        assert!(matches!(try_fetch(finst_id), TryFetchResult::NotReady));

        insert(
            finst_id,
            FetchResult {
                packet_seq: 0,
                eos: false,
                result_batch: crate::data::TResultBatch::new(vec![b"row".to_vec()], false, 0, None),
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
        let query_id = QueryId { hi: 101, lo: 202 };
        let finst_id = UniqueId { hi: 303, lo: 404 };
        let mgr = query_context_manager();
        mgr.ensure_context(
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
}
