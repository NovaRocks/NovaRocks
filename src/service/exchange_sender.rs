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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::common::config::exchange_io_max_inflight_bytes;
use crate::common::types::UniqueId;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::novarocks_logging::{debug, error};
use crate::runtime::io::io_executor;
use crate::runtime::mem_tracker::TrackedBytes;
use crate::runtime::profile::{OperatorProfiles, clamp_u128_to_i64};
use crate::runtime::runtime_state::RuntimeErrorState;

pub struct ExchangeSendTracker {
    inflight_tasks: AtomicUsize,
    inflight_bytes: AtomicUsize,
}

impl ExchangeSendTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inflight_tasks: AtomicUsize::new(0),
            inflight_bytes: AtomicUsize::new(0),
        })
    }

    pub fn on_enqueue(&self, bytes: usize) {
        self.inflight_tasks.fetch_add(1, Ordering::AcqRel);
        self.inflight_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    pub fn on_complete(&self, bytes: usize) {
        self.inflight_tasks.fetch_sub(1, Ordering::AcqRel);
        self.inflight_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    pub fn is_idle(&self) -> bool {
        self.inflight_tasks.load(Ordering::Acquire) == 0
    }

    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes.load(Ordering::Acquire)
    }
}

pub struct ExchangeSendTask {
    pub dest_host: String,
    pub dest_port: u16,
    pub finst_id: UniqueId,
    pub sender_finst_id: UniqueId,
    pub node_id: i32,
    pub sender_id: i32,
    pub be_number: i32,
    pub eos: bool,
    pub sequence: i64,
    pub payload: Vec<u8>,
    pub payload_accounting: Option<TrackedBytes>,
    pub encode_ns: u128,
    pub payload_bytes: usize,
    pub profiles: Option<OperatorProfiles>,
    pub notify: Arc<Observable>,
    pub error_state: Arc<RuntimeErrorState>,
    pub tracker: Arc<ExchangeSendTracker>,
}

#[cfg(feature = "compat")]
pub fn send_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: crate::service::internal_rpc_client::proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    crate::service::internal_rpc_client::transmit_runtime_filter(dest_host, dest_port, params)
}

#[cfg(not(feature = "compat"))]
pub fn send_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: crate::service::grpc_client::proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    crate::service::grpc_client::transmit_runtime_filter(dest_host, dest_port, params)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ExchangeSendKey {
    dest_host: String,
    dest_port: u16,
    finst_id: UniqueId,
    node_id: i32,
    sender_id: i32,
}

impl ExchangeSendKey {
    fn from_task(task: &ExchangeSendTask) -> Self {
        Self {
            dest_host: task.dest_host.clone(),
            dest_port: task.dest_port,
            finst_id: task.finst_id,
            node_id: task.node_id,
            sender_id: task.sender_id,
        }
    }
}

struct QueuedSendTask {
    task: ExchangeSendTask,
    reserve_bytes: usize,
}

#[derive(Debug)]
pub enum ExchangeSendEnqueue {
    Enqueued,
    NoCapacity,
}

/// Divisor applied to the global inflight budget to derive the per-destination cap. A single slow
/// destination's queued backlog is bounded to `max_inflight_bytes / DIVISOR`, so it cannot consume
/// the whole global budget and head-of-line-block sends to other (healthy) destinations. Tunable;
/// derived (not yet a config knob) to keep this change focused.
const INFLIGHT_BYTES_PER_DEST_DIVISOR: usize = 4;

pub struct ExchangeSendQueue {
    inflight_bytes: Arc<AtomicUsize>,
    max_inflight_bytes: usize,
    /// Per-destination reserved bytes (in-flight + queued), keyed by destination channel. Bounds
    /// each destination's backlog (see `INFLIGHT_BYTES_PER_DEST_DIVISOR`) so one slow receiver does
    /// not exhaust the shared `inflight_bytes` and stall senders to other destinations.
    per_dest_bytes: Arc<Mutex<HashMap<ExchangeSendKey, usize>>>,
    max_inflight_bytes_per_dest: usize,
    queues: Arc<Mutex<HashMap<ExchangeSendKey, VecDeque<QueuedSendTask>>>>,
    send_observers: Mutex<Vec<std::sync::Weak<Observable>>>,
}

impl ExchangeSendQueue {
    fn new() -> Self {
        let max_inflight_bytes = exchange_io_max_inflight_bytes().max(1);
        Self::with_limits(
            max_inflight_bytes,
            (max_inflight_bytes / INFLIGHT_BYTES_PER_DEST_DIVISOR).max(1),
        )
    }

    fn with_limits(max_inflight_bytes: usize, max_inflight_bytes_per_dest: usize) -> Self {
        Self {
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
            max_inflight_bytes: max_inflight_bytes.max(1),
            per_dest_bytes: Arc::new(Mutex::new(HashMap::new())),
            max_inflight_bytes_per_dest: max_inflight_bytes_per_dest.max(1),
            queues: Arc::new(Mutex::new(HashMap::new())),
            send_observers: Mutex::new(Vec::new()),
        }
    }

    pub fn register_send_observer(&self, observer: &Arc<Observable>) {
        let mut guard = self
            .send_observers
            .lock()
            .expect("exchange send observer lock");
        guard.push(Arc::downgrade(observer));
    }

    pub fn notify_send_observers(&self) {
        let observers = {
            let mut guard = self
                .send_observers
                .lock()
                .expect("exchange send observer lock");
            let mut alive = Vec::new();
            guard.retain(|weak| {
                if let Some(obs) = weak.upgrade() {
                    alive.push(obs);
                    true
                } else {
                    false
                }
            });
            alive
        };
        for observer in observers {
            let notify = observer.defer_notify();
            notify.arm();
        }
    }

    pub fn can_reserve(&self, bytes: usize) -> bool {
        let bytes = bytes.max(1);
        let cur = self.inflight_bytes.load(Ordering::Acquire);
        cur.saturating_add(bytes) <= self.max_inflight_bytes
    }

    pub fn max_inflight_bytes(&self) -> usize {
        self.max_inflight_bytes
    }

    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes.load(Ordering::Acquire)
    }

    pub fn submit_reserved(
        &self,
        task: ExchangeSendTask,
        reserve_bytes: usize,
    ) -> Result<ExchangeSendEnqueue, String> {
        let reserve_bytes = reserve_bytes.max(1);
        task.tracker.on_enqueue(reserve_bytes);
        self.enqueue_task(task, reserve_bytes);
        Ok(ExchangeSendEnqueue::Enqueued)
    }

    pub fn try_submit(
        &self,
        task: ExchangeSendTask,
        allow_overflow: bool,
    ) -> Result<ExchangeSendEnqueue, String> {
        let reserve_bytes = task.payload_bytes.max(1);
        let key = ExchangeSendKey::from_task(&task);
        if allow_overflow {
            self.force_add_per_dest(&key, reserve_bytes);
            self.inflight_bytes
                .fetch_add(reserve_bytes, Ordering::AcqRel);
        } else {
            if !self.reserve_per_dest(&key, reserve_bytes) {
                return Ok(ExchangeSendEnqueue::NoCapacity);
            }
            if !self.reserve_bytes(reserve_bytes) {
                self.release_per_dest(&key, reserve_bytes);
                return Ok(ExchangeSendEnqueue::NoCapacity);
            }
        }

        task.tracker.on_enqueue(reserve_bytes);
        self.enqueue_task(task, reserve_bytes);
        Ok(ExchangeSendEnqueue::Enqueued)
    }

    fn reserve_bytes(&self, bytes: usize) -> bool {
        loop {
            let cur = self.inflight_bytes.load(Ordering::Acquire);
            let next = cur.saturating_add(bytes);
            if next > self.max_inflight_bytes {
                return false;
            }
            if self
                .inflight_bytes
                .compare_exchange(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Reserve `bytes` against the destination's per-channel cap. The first task to a destination
    /// (cur == 0) is always admitted (subject to the global ceiling) so a single payload larger
    /// than the per-destination cap never deadlocks; once a destination has a backlog, further
    /// tasks are bounded by `max_inflight_bytes_per_dest`. Returns false without mutating on reject.
    fn reserve_per_dest(&self, key: &ExchangeSendKey, bytes: usize) -> bool {
        let mut guard = self
            .per_dest_bytes
            .lock()
            .expect("exchange per-dest bytes lock");
        let cur = guard.get(key).copied().unwrap_or(0);
        if cur > 0 && cur.saturating_add(bytes) > self.max_inflight_bytes_per_dest {
            return false;
        }
        guard.insert(key.clone(), cur.saturating_add(bytes));
        true
    }

    fn force_add_per_dest(&self, key: &ExchangeSendKey, bytes: usize) {
        let mut guard = self
            .per_dest_bytes
            .lock()
            .expect("exchange per-dest bytes lock");
        let cur = guard.get(key).copied().unwrap_or(0);
        guard.insert(key.clone(), cur.saturating_add(bytes));
    }

    fn release_per_dest(&self, key: &ExchangeSendKey, bytes: usize) {
        let mut guard = self
            .per_dest_bytes
            .lock()
            .expect("exchange per-dest bytes lock");
        if let Some(cur) = guard.get(key).copied() {
            let next = cur.saturating_sub(bytes);
            if next == 0 {
                guard.remove(key);
            } else {
                guard.insert(key.clone(), next);
            }
        }
    }

    /// Reserve `bytes` for a specific destination channel: passes only when both the destination's
    /// per-channel cap (see `reserve_per_dest`) and the global ceiling admit it. On global reject
    /// the per-destination reservation is rolled back. This is what isolates a slow receiver — its
    /// backlog cannot exhaust the shared budget and stall senders to other destinations.
    pub fn reserve_bytes_for(
        &self,
        dest_host: &str,
        dest_port: u16,
        finst_id: UniqueId,
        node_id: i32,
        sender_id: i32,
        bytes: usize,
    ) -> bool {
        let bytes = bytes.max(1);
        let key = ExchangeSendKey {
            dest_host: dest_host.to_string(),
            dest_port,
            finst_id,
            node_id,
            sender_id,
        };
        if !self.reserve_per_dest(&key, bytes) {
            return false;
        }
        if !self.reserve_bytes(bytes) {
            self.release_per_dest(&key, bytes);
            return false;
        }
        true
    }

    fn enqueue_task(&self, task: ExchangeSendTask, reserve_bytes: usize) {
        let key = ExchangeSendKey::from_task(&task);
        let queued = QueuedSendTask {
            task,
            reserve_bytes,
        };

        let mut start_now = None;
        {
            let mut guard = self.queues.lock().expect("exchange send queue lock");
            if let Some(queue) = guard.get_mut(&key) {
                queue.push_back(queued);
            } else {
                guard.insert(key.clone(), VecDeque::new());
                start_now = Some(queued);
            }
        }

        if let Some(queued) = start_now {
            spawn_send_task(
                Arc::clone(&self.inflight_bytes),
                Arc::clone(&self.queues),
                key,
                queued,
            );
        }
    }
}

fn run_send_task(task: ExchangeSendTask, inflight: Arc<AtomicUsize>, reserve_bytes: usize) {
    let send_start = Instant::now();
    // Built before the send moves `task.payload`, so we can release this destination's per-channel
    // reservation symmetrically with the global one on completion.
    let dest_key = ExchangeSendKey::from_task(&task);

    #[cfg(feature = "compat")]
    let result = crate::service::internal_rpc_client::send_chunks(
        &task.dest_host,
        task.dest_port,
        task.finst_id,
        task.node_id,
        task.sender_id,
        task.be_number,
        task.eos,
        task.sequence,
        task.payload,
    );

    #[cfg(not(feature = "compat"))]
    let result = crate::service::grpc_client::send_chunks(
        &task.dest_host,
        task.dest_port,
        task.finst_id,
        task.node_id,
        task.sender_id,
        task.be_number,
        task.eos,
        task.sequence,
        task.payload,
    );
    let send_ns = send_start.elapsed().as_nanos();

    if let Some(profile) = task.profiles.as_ref() {
        profile
            .common
            .counter_add("RequestSent", crate::thrift::metrics::TUnit::UNIT, 1);
        profile.common.counter_add(
            "BytesSent",
            crate::thrift::metrics::TUnit::BYTES,
            clamp_u128_to_i64(task.payload_bytes as u128),
        );
        profile.unique.counter_add(
            "NetworkTime",
            crate::thrift::metrics::TUnit::TIME_NS,
            clamp_u128_to_i64(send_ns),
        );
        profile.common.counter_add(
            "OverallTime",
            crate::thrift::metrics::TUnit::TIME_NS,
            clamp_u128_to_i64(task.encode_ns.saturating_add(send_ns)),
        );
    }

    if result.is_ok() {
        crate::service::metrics_http::observe_exchange_shuffle_bytes(task.payload_bytes);
    }

    if let Err(err) = result {
        task.error_state.set_error(err.clone());
        error!(
            "exchange send failed: dest={} dest_finst={} sender_finst={} node_id={} sender_id={} seq={} error={}",
            task.dest_host,
            task.finst_id,
            task.sender_finst_id,
            task.node_id,
            task.sender_id,
            task.sequence,
            err
        );
        crate::runtime::query_context::query_context_manager()
            .propagate_sender_error(task.sender_finst_id, err);
    } else {
        debug!(
            "exchange send completed: dest={} finst={} node_id={} sender_id={} eos={} seq={} bytes={}",
            task.dest_host,
            task.finst_id,
            task.node_id,
            task.sender_id,
            task.eos,
            task.sequence,
            task.payload_bytes
        );
    }

    inflight.fetch_sub(reserve_bytes, Ordering::AcqRel);
    exchange_send_queue().release_per_dest(&dest_key, reserve_bytes);
    task.tracker.on_complete(reserve_bytes);
    let notify = task.notify.defer_notify();
    notify.arm();
    exchange_send_queue().notify_send_observers();
}

fn spawn_send_task(
    inflight: Arc<AtomicUsize>,
    queues: Arc<Mutex<HashMap<ExchangeSendKey, VecDeque<QueuedSendTask>>>>,
    key: ExchangeSendKey,
    queued: QueuedSendTask,
) {
    let inflight_next = Arc::clone(&inflight);
    let queues_next = Arc::clone(&queues);
    io_executor().submit(move |_ctx| {
        run_send_task(queued.task, inflight, queued.reserve_bytes);
        on_task_complete(inflight_next, queues_next, key);
    });
}

fn on_task_complete(
    inflight: Arc<AtomicUsize>,
    queues: Arc<Mutex<HashMap<ExchangeSendKey, VecDeque<QueuedSendTask>>>>,
    key: ExchangeSendKey,
) {
    let next = {
        let mut guard = queues.lock().expect("exchange send queue lock");
        let Some(queue) = guard.get_mut(&key) else {
            return;
        };
        if let Some(next) = queue.pop_front() {
            Some(next)
        } else {
            guard.remove(&key);
            None
        }
    };

    if let Some(task) = next {
        spawn_send_task(inflight, queues, key, task);
    }
}

static SEND_QUEUE: OnceLock<ExchangeSendQueue> = OnceLock::new();

pub fn exchange_send_queue() -> &'static ExchangeSendQueue {
    SEND_QUEUE.get_or_init(ExchangeSendQueue::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finst() -> UniqueId {
        UniqueId { hi: 1, lo: 1 }
    }

    #[test]
    fn per_destination_cap_isolates_and_global_ceiling_binds() {
        // Global budget 100, per-destination cap 40.
        let q = ExchangeSendQueue::with_limits(100, 40);
        let key_a = ExchangeSendKey {
            dest_host: "A".to_string(),
            dest_port: 1,
            finst_id: finst(),
            node_id: 0,
            sender_id: 0,
        };

        // First task to destination A is admitted (empty destination).
        assert!(q.reserve_bytes_for("A", 1, finst(), 0, 0, 30));
        // A now has a 30-byte backlog; a second 30 would be 60 > 40 cap -> rejected.
        // A slow destination's backlog is bounded and cannot grow without limit.
        assert!(!q.reserve_bytes_for("A", 1, finst(), 0, 0, 30));
        // Destination B is NOT blocked by A's cap (B empty; global 30+30=60 <= 100).
        // This is the head-of-line-blocking fix: a slow A does not stall sends to a healthy B.
        assert!(q.reserve_bytes_for("B", 1, finst(), 0, 0, 30));
        assert_eq!(q.inflight_bytes(), 60);

        // The global ceiling still binds: C is empty but 60+50=110 > 100 -> rejected and rolled back.
        assert!(!q.reserve_bytes_for("C", 1, finst(), 0, 0, 50));
        assert_eq!(q.inflight_bytes(), 60);

        // Releasing A's per-destination reservation lets A admit again (global room: 60+30=90).
        q.release_per_dest(&key_a, 30);
        assert!(q.reserve_bytes_for("A", 1, finst(), 0, 0, 30));
        assert_eq!(q.inflight_bytes(), 90);
    }

    #[test]
    fn first_task_admitted_even_above_per_destination_cap() {
        // A single payload larger than the per-destination cap must not deadlock: the first task to
        // an empty destination is always admitted (subject to the global ceiling).
        let q = ExchangeSendQueue::with_limits(1000, 40);
        assert!(q.reserve_bytes_for("A", 1, finst(), 0, 0, 100));
        // Once it has a backlog, further tasks are capped.
        assert!(!q.reserve_bytes_for("A", 1, finst(), 0, 0, 1));
    }
}
