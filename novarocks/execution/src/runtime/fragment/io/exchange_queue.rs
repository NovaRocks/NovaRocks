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
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::io::IoExecutor;
use crate::runtime::mem_tracker::TrackedBytes;
use crate::runtime::profile::{OperatorProfiles, ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_types::UniqueId;
use tracing::{debug, error};

use super::exchange_edge::EdgeSendGate;
use super::exchange_metrics::observe_exchange_shuffle_bytes;
use super::{ExchangeFrame, ExchangeFrameTransmitter, ExchangeTransmitRejection};
use crate::task_execution::domain::ExchangeEdgeId;

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
    pub frame: ExchangeFrame,
    pub transmitter: Arc<dyn ExchangeFrameTransmitter>,
    pub payload_accounting: Option<TrackedBytes>,
    pub encode_ns: u128,
    pub payload_bytes: usize,
    pub profiles: Option<OperatorProfiles>,
    pub notify: Arc<Observable>,
    pub error_state: Arc<RuntimeErrorState>,
    pub tracker: Arc<ExchangeSendTracker>,
    /// The live send permission of the outbound edge this frame belongs to.
    /// It is what lets a destination's normal departure close exactly one
    /// edge and discard exactly that edge's frames. `None` for a producer
    /// whose outbound edges are not gated.
    pub edge_gate: Option<Arc<EdgeSendGate>>,
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
            dest_host: task.frame.destination.host().to_string(),
            dest_port: task.frame.destination.port() as u16,
            finst_id: task.frame.destination_fragment_instance_id,
            node_id: task.frame.destination_node_id,
            sender_id: task.frame.sender_id,
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
    io_executor: Arc<IoExecutor>,
}

impl ExchangeSendQueue {
    pub fn new(max_inflight_bytes: usize, io_executor: Arc<IoExecutor>) -> Self {
        Self::with_limits_with_executor(
            max_inflight_bytes,
            (max_inflight_bytes / INFLIGHT_BYTES_PER_DEST_DIVISOR).max(1),
            io_executor,
        )
    }

    fn with_limits_with_executor(
        max_inflight_bytes: usize,
        max_inflight_bytes_per_dest: usize,
        io_executor: Arc<IoExecutor>,
    ) -> Self {
        Self {
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
            max_inflight_bytes: max_inflight_bytes.max(1),
            per_dest_bytes: Arc::new(Mutex::new(HashMap::new())),
            max_inflight_bytes_per_dest: max_inflight_bytes_per_dest.max(1),
            queues: Arc::new(Mutex::new(HashMap::new())),
            send_observers: Mutex::new(Vec::new()),
            io_executor,
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
        self: &Arc<Self>,
        task: ExchangeSendTask,
        reserve_bytes: usize,
    ) -> Result<ExchangeSendEnqueue, String> {
        let reserve_bytes = reserve_bytes.max(1);
        task.tracker.on_enqueue(reserve_bytes);
        self.enqueue_task(task, reserve_bytes);
        Ok(ExchangeSendEnqueue::Enqueued)
    }

    pub fn try_submit(
        self: &Arc<Self>,
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

    fn enqueue_task(self: &Arc<Self>, task: ExchangeSendTask, reserve_bytes: usize) {
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
            self.spawn_send_task(key, queued);
        }
    }

    fn run_send_task(self: &Arc<Self>, task: ExchangeSendTask, reserve_bytes: usize) {
        let send_start = Instant::now();
        // Built before the send moves `task.payload`, so we can release this destination's per-channel
        // reservation symmetrically with the global one on completion.
        let dest_key = ExchangeSendKey::from_task(&task);

        let ExchangeSendTask {
            frame,
            transmitter,
            payload_accounting,
            encode_ns,
            payload_bytes,
            profiles,
            notify,
            error_state,
            tracker,
            edge_gate,
        } = task;
        let destination = frame.destination.host().to_string();
        let destination_fragment_instance_id = frame.destination_fragment_instance_id;
        let sender_fragment_instance_id = frame.sender_fragment_instance_id;
        let destination_node_id = frame.destination_node_id;
        let sender_id = frame.sender_id;
        let eos = frame.eos;
        let sequence = frame.sequence;
        let result = transmitter.transmit(frame);
        let send_ns = send_start.elapsed().as_nanos();

        if let Some(profile) = profiles.as_ref() {
            profile
                .common
                .counter_add("RequestSent", ProfileUnit::Unit, 1);
            profile.common.counter_add(
                "BytesSent",
                ProfileUnit::Bytes,
                clamp_u128_to_i64(payload_bytes as u128),
            );
            profile.unique.counter_add(
                "NetworkTime",
                ProfileUnit::TimeNs,
                clamp_u128_to_i64(send_ns),
            );
            profile.common.counter_add(
                "OverallTime",
                ProfileUnit::TimeNs,
                clamp_u128_to_i64(encode_ns.saturating_add(send_ns)),
            );
        }

        match result {
            Ok(()) => {
                observe_exchange_shuffle_bytes(payload_bytes);
                debug!(
                    "exchange send completed: dest={} finst={} node_id={} sender_id={} eos={} seq={} bytes={}",
                    destination,
                    destination_fragment_instance_id,
                    destination_node_id,
                    sender_id,
                    eos,
                    sequence,
                    payload_bytes
                );
            }
            // A destination's normal departure is not this producer's
            // failure, so the shared error state stays clean: only that
            // destination's edge closes and only that edge's frames are
            // dropped. Whether this attempt succeeded remains the
            // coordinator's decision, and the producer waits to be cancelled.
            Err(ExchangeTransmitRejection::DestinationCanceled(reason)) => match edge_gate.as_ref()
            {
                Some(gate) => {
                    let closed_here = gate.close_for_normal_cancellation();
                    let discarded = if closed_here {
                        self.discard_edge(gate.edge_id())
                    } else {
                        0
                    };
                    debug!(
                        "exchange destination cancelled normally: edge={} reason={} dest={} dest_finst={} node_id={} sender_id={} seq={} closed_here={} discarded_frames={}",
                        gate.edge_id(),
                        reason,
                        destination,
                        destination_fragment_instance_id,
                        destination_node_id,
                        sender_id,
                        sequence,
                        closed_here,
                        discarded
                    );
                }
                None => {
                    // With no gated edge there is nothing to attribute the
                    // departure to, so it cannot be told apart from a
                    // failure: fail closed rather than assume a cancellation.
                    let message = format!(
                        "exchange destination reported {reason} but this producer has no gated outbound edge"
                    );
                    error!(
                        "exchange send rejected: dest={} dest_finst={} sender_finst={} node_id={} sender_id={} seq={} error={}",
                        destination,
                        destination_fragment_instance_id,
                        sender_fragment_instance_id,
                        destination_node_id,
                        sender_id,
                        sequence,
                        message
                    );
                    error_state.set_error(message);
                }
            },
            Err(ExchangeTransmitRejection::Failed(err)) => {
                error_state.set_error(err.to_string());
                error!(
                    "exchange send failed: dest={} dest_finst={} sender_finst={} node_id={} sender_id={} seq={} error={}",
                    destination,
                    destination_fragment_instance_id,
                    sender_fragment_instance_id,
                    destination_node_id,
                    sender_id,
                    sequence,
                    err
                );
            }
        }

        self.inflight_bytes
            .fetch_sub(reserve_bytes, Ordering::AcqRel);
        self.release_per_dest(&dest_key, reserve_bytes);
        tracker.on_complete(reserve_bytes);
        let deferred_notify = notify.defer_notify();
        deferred_notify.arm();
        self.notify_send_observers();
        drop(payload_accounting);
    }

    /// Drops every queued frame of one abandoned edge and returns how many
    /// were dropped.
    ///
    /// Only that edge's frames go: a destination that left normally must not
    /// cost a healthy destination its backlog. The per-destination keys are
    /// kept even when they empty, because a present key is what tells
    /// [`Self::on_task_complete`] a send is still running for it.
    fn discard_edge(self: &Arc<Self>, edge: ExchangeEdgeId) -> usize {
        let discarded = {
            let mut guard = self.queues.lock().expect("exchange send queue lock");
            let mut discarded = Vec::new();
            for queue in guard.values_mut() {
                let mut retained = VecDeque::with_capacity(queue.len());
                while let Some(queued) = queue.pop_front() {
                    if queued
                        .task
                        .edge_gate
                        .as_ref()
                        .is_some_and(|gate| gate.edge_id() == edge)
                    {
                        discarded.push(queued);
                    } else {
                        retained.push_back(queued);
                    }
                }
                *queue = retained;
            }
            discarded
        };

        let count = discarded.len();
        for queued in discarded {
            self.release_discarded(queued);
        }
        count
    }

    /// Releases a discarded frame's reservations exactly as a completed send
    /// would, so an abandoned edge never leaks budget from the shared or the
    /// per-destination ceiling.
    fn release_discarded(self: &Arc<Self>, queued: QueuedSendTask) {
        let QueuedSendTask {
            task,
            reserve_bytes,
        } = queued;
        let dest_key = ExchangeSendKey::from_task(&task);
        self.inflight_bytes
            .fetch_sub(reserve_bytes, Ordering::AcqRel);
        self.release_per_dest(&dest_key, reserve_bytes);
        task.tracker.on_complete(reserve_bytes);
        let deferred_notify = task.notify.defer_notify();
        deferred_notify.arm();
        drop(task.payload_accounting);
        self.notify_send_observers();
    }

    fn spawn_send_task(self: &Arc<Self>, key: ExchangeSendKey, queued: QueuedSendTask) {
        let queue = Arc::clone(self);
        self.io_executor.submit(move |_ctx| {
            queue.run_send_task(queued.task, queued.reserve_bytes);
            queue.on_task_complete(key);
        });
    }

    fn on_task_complete(self: &Arc<Self>, key: ExchangeSendKey) {
        let next = {
            let mut guard = self.queues.lock().expect("exchange send queue lock");
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
            self.spawn_send_task(key, task);
        }
    }
}

#[cfg(test)]
impl ExchangeSendQueue {
    fn with_limits(max_inflight_bytes: usize, max_inflight_bytes_per_dest: usize) -> Self {
        Self::with_limits_with_executor(
            max_inflight_bytes,
            max_inflight_bytes_per_dest,
            Arc::new(IoExecutor::new(1)),
        )
    }

    /// Places a task in its destination's backlog with the same accounting a
    /// real submit performs, but without starting a send. It lets a test
    /// observe the backlog itself instead of a worker thread's timing.
    fn backlog_for_test(self: &Arc<Self>, task: ExchangeSendTask, reserve_bytes: usize) {
        let key = ExchangeSendKey::from_task(&task);
        self.force_add_per_dest(&key, reserve_bytes);
        self.inflight_bytes
            .fetch_add(reserve_bytes, Ordering::AcqRel);
        task.tracker.on_enqueue(reserve_bytes);
        let mut guard = self.queues.lock().expect("exchange send queue lock");
        guard.entry(key).or_default().push_back(QueuedSendTask {
            task,
            reserve_bytes,
        });
    }

    fn backlog_len_for_test(&self) -> usize {
        self.queues
            .lock()
            .expect("exchange send queue lock")
            .values()
            .map(VecDeque::len)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::fragment::io::exchange_edge::{
        EdgeSendGate, EdgeSendState, ExchangeDestinationKey, ExchangeEdgeGates,
    };
    use crate::runtime::fragment::io::{FragmentIoError, FragmentIoErrorKind, FragmentIoOperation};
    use crate::task_execution::domain::EdgeOpenVersion;
    use crate::task_execution::status::CancelReason;

    #[derive(Default)]
    struct RecordingTransmitter {
        frames: Mutex<Vec<ExchangeFrame>>,
        rejection: Option<ExchangeTransmitRejection>,
    }

    impl RecordingTransmitter {
        fn rejecting(rejection: ExchangeTransmitRejection) -> Self {
            Self {
                frames: Mutex::new(Vec::new()),
                rejection: Some(rejection),
            }
        }

        fn frame_count(&self) -> usize {
            self.frames.lock().expect("recorded frames lock").len()
        }
    }

    impl ExchangeFrameTransmitter for RecordingTransmitter {
        fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
            self.frames
                .lock()
                .expect("recorded frames lock")
                .push(frame);
            if let Some(rejection) = self.rejection.as_ref() {
                return Err(rejection.clone());
            }
            Ok(())
        }
    }

    fn finst() -> UniqueId {
        UniqueId::new(1, 1)
    }

    fn transmit_failure() -> FragmentIoError {
        FragmentIoError::new(
            FragmentIoOperation::ExchangeTransmit,
            FragmentIoErrorKind::Unavailable,
            "receiver unavailable",
        )
    }

    fn edge_id(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("nonzero edge")
    }

    /// One gate set with an open edge per destination, so a decision about
    /// one edge is observable as leaving the other alone.
    fn open_gates_for(destinations: &[(u32, UniqueId, i32)]) -> Arc<ExchangeEdgeGates> {
        let gates = ExchangeEdgeGates::try_new(destinations.iter().map(|(edge, finst, node)| {
            (
                edge_id(*edge),
                vec![ExchangeDestinationKey::new(*finst, *node)],
            )
        }))
        .expect("legal gate set");
        let edges: Vec<ExchangeEdgeId> = destinations
            .iter()
            .map(|(edge, _, _)| edge_id(*edge))
            .collect();
        gates
            .open(EdgeOpenVersion::FIRST, &edges)
            .expect("open every edge");
        gates
    }

    fn exchange_task(
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
        error_state: Arc<RuntimeErrorState>,
        tracker: Arc<ExchangeSendTracker>,
    ) -> ExchangeSendTask {
        ExchangeSendTask {
            frame: ExchangeFrame {
                destination: RuntimeEndpoint::new("be-2", 9060).expect("destination"),
                destination_fragment_instance_id: UniqueId::new(2, 3),
                sender_fragment_instance_id: UniqueId::new(4, 5),
                sender_ordinal: 0,
                sender_count: 1,
                destination_node_id: 6,
                sender_id: 7,
                backend_number: 8,
                sequence: 9,
                eos: true,
                payload: vec![10, 11],
            },
            transmitter,
            payload_accounting: None,
            encode_ns: 12,
            payload_bytes: 2,
            profiles: None,
            notify: Arc::new(Observable::default()),
            error_state,
            tracker,
            edge_gate: None,
        }
    }

    fn task_on_edge(
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
        error_state: Arc<RuntimeErrorState>,
        tracker: Arc<ExchangeSendTracker>,
        gate: Arc<EdgeSendGate>,
        destination_finst: UniqueId,
    ) -> ExchangeSendTask {
        let mut task = exchange_task(transmitter, error_state, tracker);
        task.frame.destination_fragment_instance_id = destination_finst;
        task.edge_gate = Some(gate);
        task
    }

    #[test]
    fn worker_forwards_the_complete_exchange_frame_to_the_injected_transmitter() {
        let transmitter = Arc::new(RecordingTransmitter::default());
        let error_state = Arc::new(RuntimeErrorState::default());
        let tracker = ExchangeSendTracker::new();
        tracker.on_enqueue(2);

        let queue = Arc::new(ExchangeSendQueue::with_limits(8, 8));
        queue.run_send_task(
            exchange_task(
                Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
            ),
            2,
        );

        let frames = transmitter.frames.lock().expect("recorded frames lock");
        assert_eq!(frames.len(), 1);
        let frame = &frames[0];
        assert_eq!(frame.destination.host(), "be-2");
        assert_eq!(frame.destination.port(), 9060);
        assert_eq!(frame.destination_fragment_instance_id, UniqueId::new(2, 3));
        assert_eq!(frame.sender_fragment_instance_id, UniqueId::new(4, 5));
        assert_eq!(frame.destination_node_id, 6);
        assert_eq!(frame.sender_id, 7);
        assert_eq!(frame.backend_number, 8);
        assert_eq!(frame.sequence, 9);
        assert!(frame.eos);
        assert_eq!(frame.payload, vec![10, 11]);
        assert!(error_state.error().is_none());
        assert!(tracker.is_idle());
    }

    #[test]
    fn worker_records_transmit_failure_only_in_its_fragment_error_state() {
        let transmitter = Arc::new(RecordingTransmitter::rejecting(
            ExchangeTransmitRejection::Failed(transmit_failure()),
        ));
        let error_state = Arc::new(RuntimeErrorState::default());
        let tracker = ExchangeSendTracker::new();
        tracker.on_enqueue(2);

        let queue = Arc::new(ExchangeSendQueue::with_limits(8, 8));
        queue.run_send_task(
            exchange_task(
                Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
            ),
            2,
        );

        assert!(
            error_state
                .error()
                .is_some_and(|error| error.contains("receiver unavailable"))
        );
        assert!(tracker.is_idle());
    }

    #[test]
    fn a_normal_destination_cancellation_closes_its_edge_without_poisoning_the_producer() {
        let dest = UniqueId::new(2, 3);
        let gates = open_gates_for(&[(1, dest, 6)]);
        let gate = Arc::clone(gates.gate(edge_id(1)).expect("edge one"));
        let transmitter = Arc::new(RecordingTransmitter::rejecting(
            ExchangeTransmitRejection::DestinationCanceled(CancelReason::UpstreamNoLongerNeeded),
        ));
        let error_state = Arc::new(RuntimeErrorState::default());
        let tracker = ExchangeSendTracker::new();
        tracker.on_enqueue(2);

        let queue = Arc::new(ExchangeSendQueue::with_limits(8, 8));
        assert!(queue.reserve_bytes_for("be-2", 9060, dest, 6, 7, 2));
        queue.run_send_task(
            task_on_edge(
                Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
                gate,
                dest,
            ),
            2,
        );

        assert_eq!(
            error_state.error(),
            None,
            "a destination's normal departure must never become this producer's error"
        );
        assert_eq!(
            gates.gate(edge_id(1)).expect("edge one").state(),
            EdgeSendState::NormallyCanceled
        );
        assert_eq!(gates.normally_canceled_edges(), vec![edge_id(1)]);
        assert_eq!(gates.normally_canceled_edge_count(), 1);
        assert!(tracker.is_idle());
        assert_eq!(queue.inflight_bytes(), 0);
    }

    #[test]
    fn a_normal_cancellation_on_an_ungated_producer_fails_closed() {
        let transmitter = Arc::new(RecordingTransmitter::rejecting(
            ExchangeTransmitRejection::DestinationCanceled(CancelReason::UpstreamNoLongerNeeded),
        ));
        let error_state = Arc::new(RuntimeErrorState::default());
        let tracker = ExchangeSendTracker::new();
        tracker.on_enqueue(2);

        let queue = Arc::new(ExchangeSendQueue::with_limits(8, 8));
        queue.run_send_task(
            exchange_task(
                Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
            ),
            2,
        );

        assert!(
            error_state
                .error()
                .is_some_and(|error| error.contains("no gated outbound edge")),
            "an unattributable cancellation cannot be told apart from a failure"
        );
    }

    #[test]
    fn a_normal_cancellation_discards_only_the_abandoned_edges_queued_frames() {
        let abandoned = UniqueId::new(2, 3);
        let healthy = UniqueId::new(2, 4);
        let gates = open_gates_for(&[(1, abandoned, 6), (2, healthy, 6)]);
        let abandoned_gate = Arc::clone(gates.gate(edge_id(1)).expect("edge one"));
        let healthy_gate = Arc::clone(gates.gate(edge_id(2)).expect("edge two"));
        let error_state = Arc::new(RuntimeErrorState::default());
        let tracker = ExchangeSendTracker::new();
        let queue = Arc::new(ExchangeSendQueue::with_limits(100, 100));

        for _ in 0..2 {
            queue.backlog_for_test(
                task_on_edge(
                    crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
                    Arc::clone(&error_state),
                    Arc::clone(&tracker),
                    Arc::clone(&abandoned_gate),
                    abandoned,
                ),
                2,
            );
        }
        queue.backlog_for_test(
            task_on_edge(
                crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
                Arc::clone(&error_state),
                Arc::clone(&tracker),
                Arc::clone(&healthy_gate),
                healthy,
            ),
            3,
        );
        assert_eq!(queue.backlog_len_for_test(), 3);
        assert_eq!(queue.inflight_bytes(), 7);

        let transmitter = Arc::new(RecordingTransmitter::rejecting(
            ExchangeTransmitRejection::DestinationCanceled(CancelReason::UpstreamNoLongerNeeded),
        ));
        // The frame that learns of the departure holds its own reservation,
        // exactly as a real in-flight send does.
        assert!(queue.reserve_bytes_for("be-2", 9060, abandoned, 6, 7, 2));
        tracker.on_enqueue(2);
        queue.run_send_task(
            task_on_edge(
                Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
                abandoned_gate,
                abandoned,
            ),
            2,
        );

        assert_eq!(
            queue.backlog_len_for_test(),
            1,
            "only the abandoned edge's queued frames are dropped"
        );
        assert_eq!(
            queue.inflight_bytes(),
            3,
            "the discarded frames release exactly their own reservation"
        );
        assert_eq!(error_state.error(), None);
        assert!(
            gates.gate(edge_id(2)).expect("edge two").may_send(),
            "the healthy edge keeps its send permission"
        );
    }

    #[test]
    fn a_cancelled_edge_does_not_stop_another_edge_from_sending() {
        let abandoned = UniqueId::new(2, 3);
        let healthy = UniqueId::new(2, 4);
        let gates = open_gates_for(&[(1, abandoned, 6), (2, healthy, 6)]);
        let error_state = Arc::new(RuntimeErrorState::default());
        let tracker = ExchangeSendTracker::new();
        let queue = Arc::new(ExchangeSendQueue::with_limits(100, 100));

        let cancelling = Arc::new(RecordingTransmitter::rejecting(
            ExchangeTransmitRejection::DestinationCanceled(CancelReason::UpstreamNoLongerNeeded),
        ));
        assert!(queue.reserve_bytes_for("be-2", 9060, abandoned, 6, 7, 2));
        tracker.on_enqueue(2);
        queue.run_send_task(
            task_on_edge(
                Arc::clone(&cancelling) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
                Arc::clone(gates.gate(edge_id(1)).expect("edge one")),
                abandoned,
            ),
            2,
        );

        let sending = Arc::new(RecordingTransmitter::default());
        assert!(queue.reserve_bytes_for("be-2", 9060, healthy, 6, 7, 2));
        tracker.on_enqueue(2);
        queue.run_send_task(
            task_on_edge(
                Arc::clone(&sending) as Arc<dyn ExchangeFrameTransmitter>,
                Arc::clone(&error_state),
                Arc::clone(&tracker),
                Arc::clone(gates.gate(edge_id(2)).expect("edge two")),
                healthy,
            ),
            2,
        );

        assert_eq!(sending.frame_count(), 1);
        assert_eq!(error_state.error(), None);
        assert_eq!(gates.normally_canceled_edges(), vec![edge_id(1)]);
        assert!(tracker.is_idle());
        assert_eq!(queue.inflight_bytes(), 0);
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
