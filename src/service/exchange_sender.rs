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
use crate::service::grpc_client;

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

pub fn send_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: grpc_client::proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    grpc_client::transmit_runtime_filter(dest_host, dest_port, params)
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

pub struct ExchangeSendQueue {
    inflight_bytes: Arc<AtomicUsize>,
    max_inflight_bytes: usize,
    queues: Arc<Mutex<HashMap<ExchangeSendKey, VecDeque<QueuedSendTask>>>>,
    send_observers: Mutex<Vec<std::sync::Weak<Observable>>>,
}

impl ExchangeSendQueue {
    fn new() -> Self {
        Self {
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
            max_inflight_bytes: exchange_io_max_inflight_bytes().max(1),
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

    pub fn try_reserve_bytes(&self, bytes: usize) -> bool {
        let bytes = bytes.max(1);
        self.reserve_bytes(bytes)
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
        if !allow_overflow && !self.reserve_bytes(reserve_bytes) {
            return Ok(ExchangeSendEnqueue::NoCapacity);
        }
        if allow_overflow {
            self.inflight_bytes
                .fetch_add(reserve_bytes, Ordering::AcqRel);
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
    let result = grpc_client::send_chunks(
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
    let send_ns = send_start.elapsed().as_nanos() as u128;

    if let Some(profile) = task.profiles.as_ref() {
        profile
            .common
            .counter_add("RequestSent", crate::metrics::TUnit::UNIT, 1);
        profile.common.counter_add(
            "BytesSent",
            crate::metrics::TUnit::BYTES,
            clamp_u128_to_i64(task.payload_bytes as u128),
        );
        profile.unique.counter_add(
            "NetworkTime",
            crate::metrics::TUnit::TIME_NS,
            clamp_u128_to_i64(send_ns),
        );
        profile.common.counter_add(
            "OverallTime",
            crate::metrics::TUnit::TIME_NS,
            clamp_u128_to_i64(task.encode_ns.saturating_add(send_ns)),
        );
    }

    if let Err(err) = result {
        task.error_state.set_error(err.clone());
        error!(
            "exchange send failed: dest={} finst={}:{} node_id={} sender_id={} seq={} error={}",
            task.dest_host,
            task.finst_id.hi,
            task.finst_id.lo,
            task.node_id,
            task.sender_id,
            task.sequence,
            err
        );
    } else {
        debug!(
            "exchange send completed: dest={} finst={}:{} node_id={} sender_id={} eos={} seq={} bytes={}",
            task.dest_host,
            task.finst_id.hi,
            task.finst_id.lo,
            task.node_id,
            task.sender_id,
            task.eos,
            task.sequence,
            task.payload_bytes
        );
    }

    inflight.fetch_sub(reserve_bytes, Ordering::AcqRel);
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
