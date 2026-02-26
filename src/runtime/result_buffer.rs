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

use crate::common::types::{FetchResult, UniqueId};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};

#[derive(Debug, Clone)]
pub(crate) enum FetchErrorKind {
    NotFound,
    Cancelled,
    Failed,
    Timeout,
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
    status_error: Option<String>,
    cancelled: bool,
    cancel_message: Option<String>,
    next_packet_seq: i64,
    mem_tracker: Option<Arc<MemTracker>>,
}

impl BufferControlBlock {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            closed_ok: false,
            status_error: None,
            cancelled: false,
            cancel_message: None,
            next_packet_seq: 0,
            mem_tracker: None,
        }
    }

    fn make_eos_result(&mut self) -> FetchResult {
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;
        FetchResult {
            packet_seq: seq,
            eos: true,
            result_batch: crate::data::TResultBatch::new(vec![], false, 0, None),
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
    cv: Condvar,
}

static CTX: OnceLock<ResultCtx> = OnceLock::new();

fn ctx() -> &'static ResultCtx {
    CTX.get_or_init(|| ResultCtx {
        mu: Mutex::new(HashMap::new()),
        cv: Condvar::new(),
    })
}

pub(crate) fn insert(finst_id: UniqueId, result: FetchResult) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    let tracked = TrackedFetchResult::new(result, block.mem_tracker.as_ref());
    block.queue.push_back(tracked);
    c.cv.notify_all();
}

pub(crate) fn close_ok(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.closed_ok = true;
    c.cv.notify_all();
}

pub(crate) fn close_error(finst_id: UniqueId, message: String) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.status_error = Some(message);
    block.queue.clear();
    c.cv.notify_all();
}

pub(crate) fn cancel(finst_id: UniqueId) {
    let c = ctx();
    let mut guard = c.mu.lock().expect("ctx lock");
    let block = guard
        .entry(finst_id)
        .or_insert_with(BufferControlBlock::new);
    block.cancelled = true;
    if block.cancel_message.is_none() {
        block.cancel_message = Some("Cancelled".to_string());
    }
    block.queue.clear();
    c.cv.notify_all();
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

pub(crate) fn fetch(finst_id: UniqueId) -> Result<FetchResult, FetchError> {
    use crate::novarocks_logging::{debug, warn};

    let c = ctx();
    use crate::runtime::query_context::query_context_manager;

    let timeout = {
        let mgr = query_context_manager();
        if let Some(query_timeout) = mgr.get_query_timeout_by_finst(finst_id) {
            debug!(
                "fetch: using query_timeout from FE for finst={}, timeout={:?}",
                finst_id, query_timeout
            );
            query_timeout
        } else {
            let fallback = Duration::from_secs(300);
            warn!(
                "fetch: query_context not found for finst={}, using fallback timeout={:?}",
                finst_id, fallback
            );
            fallback
        }
    };

    let mut guard = c.mu.lock().expect("ctx lock");
    if !guard.contains_key(&finst_id) {
        return Err(FetchError {
            kind: FetchErrorKind::NotFound,
            message: "no result for this query".to_string(),
        });
    };

    debug!("fetch: waiting up to {:?} for finst={}", timeout, finst_id);

    let start = std::time::Instant::now();
    loop {
        {
            let block = guard.get_mut(&finst_id).ok_or_else(|| FetchError {
                kind: FetchErrorKind::NotFound,
                message: "no result for this query".to_string(),
            })?;

            if block.cancelled {
                return Err(FetchError {
                    kind: FetchErrorKind::Cancelled,
                    message: block
                        .cancel_message
                        .clone()
                        .unwrap_or_else(|| "Cancelled".to_string()),
                });
            }
            if let Some(msg) = block.status_error.as_ref() {
                return Err(FetchError {
                    kind: FetchErrorKind::Failed,
                    message: msg.clone(),
                });
            }
            if let Some(result) = block.pop_next() {
                return Ok(result);
            }
            if block.closed_ok {
                return Ok(block.make_eos_result());
            }
        }

        let elapsed = start.elapsed();
        if elapsed >= timeout {
            warn!(
                "fetch: TIMEOUT waiting for result finst={} after {:?}",
                finst_id, elapsed
            );
            return Err(FetchError {
                kind: FetchErrorKind::Timeout,
                message: format!("timeout waiting for result after {:?}", elapsed),
            });
        }
        let remaining = timeout - elapsed;
        let (g, wait_res) = c.cv.wait_timeout(guard, remaining).expect("ctx wait");
        guard = g;
        if wait_res.timed_out() {
            // Loop will convert into a Timeout error on the next iteration.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn cancel_unblocks_waiter() {
        let finst_id = UniqueId { hi: 42, lo: 7 };
        create_sender(finst_id);
        let handle = thread::spawn(move || fetch(finst_id));
        thread::sleep(Duration::from_millis(50));
        cancel(finst_id);
        let out = handle.join().expect("join");
        let err = out.expect_err("expected cancel error");
        assert!(matches!(err.kind, FetchErrorKind::Cancelled));
    }

    #[test]
    fn close_error_is_observable() {
        let finst_id = UniqueId { hi: 1, lo: 2 };
        create_sender(finst_id);
        close_error(finst_id, "boom".to_string());
        let err = fetch(finst_id).expect_err("expected error");
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert_eq!(err.message, "boom");
    }
}
