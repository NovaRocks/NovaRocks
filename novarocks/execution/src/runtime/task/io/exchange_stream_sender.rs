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

//! Task-owned ordered retention and process-owned scheduling for Exchange units.
//!
//! Retention is local to an exact stream. A process permit covers only actual
//! transport activity and survives cancellation until that activity exits.
//! Neither state lock is held across a transport or observer callback.

// Design: ADR-0209 (docs/adr/ADR-0209-receiver-authorized-exchange-units-and-shared-wait-graph.md)

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureProjection};
use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeStreamFacts, ExchangeUnitCloseDisposition, ExchangeUnitDemand, ExchangeUnitItem,
    ExchangeUnitSequence,
};
use novarocks_execution_contract::ordered_retained_stream::{
    OfferOutcome, OrderedRetainedStream, PublishedStreamRange, RetainedItemKind,
    RetainedStreamSnapshot, RetainedStreamWindow, StreamError,
};
use novarocks_execution_contract::{ExchangeDeliveryRequirement, ExchangeStreamIdentity};
use novarocks_types::identity::{BackendProcessId, QueryExecutionId};
use tokio_util::sync::CancellationToken;

use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::observable::Observable;
use crate::runtime::task::ExecutionReason;

use super::ExchangeFrameTransmitter;
use super::exchange::ExchangeNormalCloseAccepted;
use super::exchange_unit::{
    ExchangeCallActivity, ExchangeCallOutcome, ExchangeDemandCallOutcome, ExchangeDemandEntry,
    ExchangeDemandOperation, ExchangeGrantNotice, ExchangeUnitFrame, ExchangeUnitReceiverFrame,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeUnitSenderError {
    InvalidLimits,
    InvalidStream,
    EmptyPayload,
    UnitTooLarge,
    Stopped,
    AfterEnd,
    Kernel(StreamError),
}

impl fmt::Display for ExchangeUnitSenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => {
                f.write_str("exchange sender limits must be positive and representable")
            }
            Self::InvalidStream => {
                f.write_str("exchange sender count does not cover its exact stream")
            }
            Self::EmptyPayload => f.write_str("exchange data unit must contain wire bytes"),
            Self::UnitTooLarge => f.write_str("exchange unit exceeds its grantable Native bound"),
            Self::Stopped => f.write_str("exchange stream sender has stopped"),
            Self::AfterEnd => f.write_str("exchange stream cannot publish after End"),
            Self::Kernel(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ExchangeUnitSenderError {}
impl FailureProjection for ExchangeUnitSenderError {
    fn failure_fact(&self) -> FailureFact {
        if let Self::Kernel(error) = self {
            return error.failure_fact();
        }
        let class = match self {
            Self::UnitTooLarge => FailureClass::Resource,
            Self::Stopped => FailureClass::Control,
            _ => FailureClass::Invariant,
        };
        FailureFact::new(class, ExecutionReason::ExchangeStreamContract.reason())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "backpressure leaves the payload with its driver for a later publication"]
pub enum ExchangeUnitPublishOutcome {
    Published(PublishedStreamRange),
    Backpressured,
}

fn contract_failure() -> FailureFact {
    FailureFact::new(
        FailureClass::Invariant,
        ExecutionReason::ExchangeStreamContract.reason(),
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExchangeSendActivitySnapshot {
    pub bytes: usize,
    pub calls: usize,
    pub waiting_streams: usize,
}

struct ScheduleState {
    bytes: usize,
    calls: usize,
    peers: HashMap<BackendProcessId, usize>,
    waiting:
        BTreeMap<QueryExecutionId, VecDeque<(ExchangeStreamIdentity, Weak<ExchangeStreamSender>)>>,
    queries: VecDeque<QueryExecutionId>,
    queued: BTreeSet<ExchangeStreamIdentity>,
    driving: bool,
    revision: u64,
}

impl ScheduleState {
    fn enqueue(&mut self, sender: &Arc<ExchangeStreamSender>, front: bool) {
        let stream = sender.config.stream;
        let query = stream.source().task().query_execution_id();
        if self.queued.insert(stream) {
            let queue = self.waiting.entry(query).or_insert_with(|| {
                self.queries.push_back(query);
                VecDeque::new()
            });
            if front {
                queue.push_front((stream, Arc::downgrade(sender)));
            } else {
                queue.push_back((stream, Arc::downgrade(sender)));
            }
        }
        if front {
            if let Some(queue) = self.waiting.get_mut(&query)
                && let Some(position) = queue.iter().position(|(queued, _)| *queued == stream)
                && position != 0
            {
                let entry = queue.remove(position).expect("queued exchange stream");
                queue.push_front(entry);
            }
            // Drain existing activity for the next byte-blocked request. Once
            // granted, this query goes to the back again; large units cannot
            // be starved by a continuous supply of small units.
            self.queries.retain(|queued| *queued != query);
            self.queries.push_front(query);
        }
    }

    fn pop(&mut self) -> Option<(ExchangeStreamIdentity, Weak<ExchangeStreamSender>)> {
        let query = self.queries.pop_front()?;
        let queue = self.waiting.get_mut(&query).expect("queued exchange query");
        let entry = queue.pop_front().expect("nonempty exchange query queue");
        if queue.is_empty() {
            self.waiting.remove(&query);
        } else {
            self.queries.push_back(query);
        }
        self.queued.remove(&entry.0);
        Some(entry)
    }
}

/// Instantiate once per execution process. Queued streams own no shared permit.
/// An oversized legal unit is admitted exclusively when activity is idle; M04a
/// replaces this intermediate rule with its byte-credit contract.
pub struct ExchangeUnitSendScheduler {
    max_bytes: usize,
    max_calls_per_peer: usize,
    max_unit_bytes: usize,
    state: Mutex<ScheduleState>,
}

impl ExchangeUnitSendScheduler {
    pub fn try_new(
        max_bytes: usize,
        max_calls_per_peer: usize,
        max_unit_bytes: usize,
    ) -> Result<Arc<Self>, ExchangeUnitSenderError> {
        if max_bytes == 0 || max_calls_per_peer == 0 || max_unit_bytes == 0 {
            return Err(ExchangeUnitSenderError::InvalidLimits);
        }
        Ok(Arc::new(Self {
            max_bytes,
            max_calls_per_peer,
            max_unit_bytes,
            state: Mutex::new(ScheduleState {
                bytes: 0,
                calls: 0,
                peers: HashMap::new(),
                waiting: BTreeMap::new(),
                queries: VecDeque::new(),
                queued: BTreeSet::new(),
                driving: false,
                revision: 0,
            }),
        }))
    }

    pub fn snapshot(&self) -> ExchangeSendActivitySnapshot {
        let state = self.state.lock().expect("exchange unit scheduler lock");
        ExchangeSendActivitySnapshot {
            bytes: state.bytes,
            calls: state.calls,
            waiting_streams: state.queued.len(),
        }
    }

    pub fn try_snapshot(&self) -> Option<ExchangeSendActivitySnapshot> {
        let state = self.state.try_lock().ok()?;
        Some(ExchangeSendActivitySnapshot {
            bytes: state.bytes,
            calls: state.calls,
            waiting_streams: state.queued.len(),
        })
    }

    fn request(self: &Arc<Self>, sender: &Arc<ExchangeStreamSender>) {
        {
            let mut state = self.state.lock().expect("exchange unit scheduler lock");
            state.enqueue(sender, false);
            state.revision = state.revision.wrapping_add(1);
        }
        self.drive();
    }

    fn drive(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().expect("exchange unit scheduler lock");
            if state.driving {
                return;
            }
            state.driving = true;
        }
        loop {
            let (count, revision) = {
                let state = self.state.lock().expect("exchange unit scheduler lock");
                (state.queued.len(), state.revision)
            };
            let mut sent = false;
            for _ in 0..count {
                let entry = {
                    let mut state = self.state.lock().expect("exchange unit scheduler lock");
                    state.pop()
                };
                let Some((stream, weak)) = entry else {
                    continue;
                };
                let Some(sender) = weak.upgrade() else {
                    continue;
                };
                let Some(candidate) = sender.candidate() else {
                    continue;
                };
                let peer = stream.destination().backend_process_id();
                let (reserved, wait_for_bytes) = {
                    let mut state = self.state.lock().expect("exchange unit scheduler lock");
                    let peer_calls = state.peers.get(&peer).copied().unwrap_or(0);
                    let fits = state
                        .bytes
                        .checked_add(candidate.bytes)
                        .is_some_and(|bytes| bytes <= self.max_bytes);
                    let exclusive = state.calls == 0
                        && candidate.bytes > self.max_bytes
                        && candidate.bytes <= self.max_unit_bytes;
                    if peer_calls < self.max_calls_per_peer && (fits || exclusive) {
                        state.bytes += candidate.bytes;
                        state.calls += 1;
                        *state.peers.entry(peer).or_default() += 1;
                        (true, false)
                    } else {
                        let wait_for_bytes =
                            peer_calls < self.max_calls_per_peer && !fits && state.calls > 0;
                        state.enqueue(&sender, wait_for_bytes);
                        (false, wait_for_bytes)
                    }
                };
                if wait_for_bytes {
                    break;
                }
                if reserved {
                    let permit = SendPermit {
                        scheduler: Arc::clone(self),
                        peer,
                        bytes: candidate.bytes,
                    };
                    sender.dispatch(candidate, permit);
                    sent = true;
                    break;
                }
            }
            if !sent {
                let mut state = self.state.lock().expect("exchange unit scheduler lock");
                if state.revision != revision && !state.queued.is_empty() {
                    continue;
                }
                state.driving = false;
                return;
            }
        }
    }
}

struct SendPermit {
    scheduler: Arc<ExchangeUnitSendScheduler>,
    peer: BackendProcessId,
    bytes: usize,
}
impl Drop for SendPermit {
    fn drop(&mut self) {
        {
            let mut state = self
                .scheduler
                .state
                .lock()
                .expect("exchange unit scheduler lock");
            state.bytes -= self.bytes;
            state.calls -= 1;
            state.revision = state.revision.wrapping_add(1);
            let count = state
                .peers
                .get_mut(&self.peer)
                .expect("active exchange peer");
            *count -= 1;
            if *count == 0 {
                state.peers.remove(&self.peer);
            }
        }
        self.scheduler.drive();
    }
}

#[derive(Clone, Debug)]
pub struct ExchangeStreamSenderConfig {
    pub stream: ExchangeStreamIdentity,
    pub destination: RuntimeEndpoint,
    pub sender_count: u32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub delivery: ExchangeDeliveryRequirement,
    pub retained_window: usize,
    pub max_unit_bytes: usize,
    pub retry_delay: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeStreamSenderStatus {
    Open,
    /// Every item is in receiver custody. This does not author Consumed.
    CustodyComplete,
    Withdrawn,
    Stopped,
    Failed(FailureFact),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeSenderStopState {
    NotRequired,
    Pending,
    Confirmed,
    Refused(FailureFact),
    RouteRetired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeStreamSenderSnapshot {
    pub retention: RetainedStreamSnapshot,
    pub status: ExchangeStreamSenderStatus,
    pub pending_grant: Option<ExchangeUnitSequence>,
    pub granted_through: u64,
    pub active_calls: usize,
    /// Original Stop/Demand request captures that have not actually exited.
    pub active_control_calls: usize,
    pub watching: bool,
    pub stop_obligation: ExchangeSenderStopState,
}

/// A borrowed diagnostic sample of existing state, never an exit or capacity proof.
#[derive(Clone, Copy)]
pub(crate) struct ExchangeStreamSenderDiagnosticSnapshot {
    pub(crate) sender: ExchangeStreamSenderSnapshot,
    pub(crate) peer_contact_started: bool,
    pub(crate) stop_active: bool,
    pub(crate) stop_retry_waiting: bool,
    pub(crate) demand_active: bool,
    pub(crate) demand_retry_waiting: bool,
}
impl fmt::Debug for ExchangeStreamSenderDiagnosticSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExchangeStreamSenderDiagnosticSnapshot")
            .field("sender", &self.sender)
            .field("peer_contact_started", &self.peer_contact_started)
            .field("stop_active", &self.stop_active)
            .field("stop_retry_waiting", &self.stop_retry_waiting)
            .field("demand_active", &self.demand_active)
            .field("demand_retry_waiting", &self.demand_retry_waiting)
            .finish()
    }
}

struct RetainedPayload {
    bytes: Bytes,
}
struct SenderState {
    kernel: OrderedRetainedStream,
    payloads: BTreeMap<u64, RetainedPayload>,
    pending: Option<u64>,
    granted: u64,
    status: ExchangeStreamSenderStatus,
    active: BTreeMap<u64, u64>, // sequence -> actual call identity
    active_control_calls: usize,
    next_call_id: u64,
    replay_due: BTreeSet<u64>,
    retry_pending: BTreeSet<u64>,
    demand_registered: Option<u64>,
    demand_active: bool,
    demand_retry_waiting: bool,
    watching: bool,
    refreshing: bool,
    refresh_again: bool,
    cleanup_started: bool,
    // Monotonic call-publication fact, independent of current activity. Set
    // under this lock before any demand, subscription or delivery can escape.
    peer_contact_started: bool,
    stop_obligation: ExchangeSenderStopState,
    stop_active: bool,
    stop_retry_waiting: bool,
}

#[derive(Clone, Copy)]
struct SendCandidate {
    sequence: u64,
    bytes: usize,
}

pub struct ExchangeStreamSender {
    config: ExchangeStreamSenderConfig,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    scheduler: Arc<ExchangeUnitSendScheduler>,
    observable: Arc<Observable>,
    cancellation: CancellationToken,
    stop_cancellation: CancellationToken,
    state: Mutex<SenderState>,
}

impl ExchangeStreamSender {
    pub fn try_new(
        config: ExchangeStreamSenderConfig,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
        scheduler: Arc<ExchangeUnitSendScheduler>,
        observable: Arc<Observable>,
    ) -> Result<Arc<Self>, ExchangeUnitSenderError> {
        if config.sender_count == 0
            || config.stream.source().sender_ordinal() >= config.sender_count
        {
            return Err(ExchangeUnitSenderError::InvalidStream);
        }
        if config.retained_window == 0
            || config.max_unit_bytes == 0
            || config.max_unit_bytes > scheduler.max_unit_bytes
            || config.retry_delay.is_zero()
        {
            return Err(ExchangeUnitSenderError::InvalidLimits);
        }
        let positions = config
            .retained_window
            .checked_add(1)
            .ok_or(ExchangeUnitSenderError::InvalidLimits)?;
        let bytes = config
            .max_unit_bytes
            .checked_mul(positions)
            .ok_or(ExchangeUnitSenderError::InvalidLimits)?;
        let window = RetainedStreamWindow::try_new(positions, bytes)
            .map_err(ExchangeUnitSenderError::Kernel)?;
        let kernel =
            OrderedRetainedStream::try_new(window).map_err(ExchangeUnitSenderError::Kernel)?;
        Ok(Arc::new(Self {
            config,
            transmitter,
            scheduler,
            observable,
            cancellation: CancellationToken::new(),
            stop_cancellation: CancellationToken::new(),
            state: Mutex::new(SenderState {
                kernel,
                payloads: BTreeMap::new(),
                pending: None,
                granted: 0,
                status: ExchangeStreamSenderStatus::Open,
                active: BTreeMap::new(),
                active_control_calls: 0,
                next_call_id: 0,
                replay_due: BTreeSet::new(),
                retry_pending: BTreeSet::new(),
                demand_registered: None,
                demand_active: false,
                demand_retry_waiting: false,
                watching: false,
                refreshing: false,
                refresh_again: false,
                cleanup_started: false,
                peer_contact_started: false,
                stop_obligation: ExchangeSenderStopState::NotRequired,
                stop_active: false,
                stop_retry_waiting: false,
            }),
        }))
    }

    pub fn observable(&self) -> &Arc<Observable> {
        &self.observable
    }

    pub fn snapshot(&self) -> ExchangeStreamSenderSnapshot {
        let state = self.state.lock().expect("exchange stream sender lock");
        ExchangeStreamSenderSnapshot {
            retention: state.kernel.snapshot(),
            status: state.status,
            pending_grant: state.pending.map(|sequence| {
                ExchangeUnitSequence::try_new(sequence).expect("published sequence")
            }),
            granted_through: state.granted,
            active_calls: state.active.len(),
            active_control_calls: state.active_control_calls,
            watching: state.watching,
            stop_obligation: state.stop_obligation,
        }
    }

    pub fn failure(&self) -> Option<FailureFact> {
        match self.snapshot().status {
            ExchangeStreamSenderStatus::Failed(failure) => Some(failure),
            _ => None,
        }
    }

    pub(crate) fn try_diagnostic_snapshot(&self) -> Option<ExchangeStreamSenderDiagnosticSnapshot> {
        let state = self.state.try_lock().ok()?;
        Some(ExchangeStreamSenderDiagnosticSnapshot {
            sender: ExchangeStreamSenderSnapshot {
                retention: state.kernel.snapshot(),
                status: state.status,
                pending_grant: state.pending.map(|sequence| {
                    ExchangeUnitSequence::try_new(sequence).expect("published sequence")
                }),
                granted_through: state.granted,
                active_calls: state.active.len(),
                active_control_calls: state.active_control_calls,
                watching: state.watching,
                stop_obligation: state.stop_obligation,
            },
            peer_contact_started: state.peer_contact_started,
            stop_active: state.stop_active,
            stop_retry_waiting: state.stop_retry_waiting,
            demand_active: state.demand_active,
            demand_retry_waiting: state.demand_retry_waiting,
        })
    }

    /// Borrows the driver-owned payload and retains a shared reference only on
    /// publication. Backpressured is normal flow control: it consumes no sequence
    /// and changes no failure state. The driver keeps the payload and waits for
    /// this sender's observable before retrying publication.
    pub fn publish(
        self: &Arc<Self>,
        payload: &Bytes,
        end_after_unit: bool,
    ) -> Result<ExchangeUnitPublishOutcome, ExchangeUnitSenderError> {
        if payload.is_empty() {
            return Err(ExchangeUnitSenderError::EmptyPayload);
        }
        if payload.len() > self.config.max_unit_bytes {
            return Err(ExchangeUnitSenderError::UnitTooLarge);
        }
        let range = {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if state.status != ExchangeStreamSenderStatus::Open {
                return Err(ExchangeUnitSenderError::Stopped);
            }
            if state.kernel.snapshot().end_sequence.is_some() {
                return Err(ExchangeUnitSenderError::AfterEnd);
            }
            if state.pending.is_some() {
                return Ok(ExchangeUnitPublishOutcome::Backpressured);
            }
            let range = state
                .kernel
                .publish_data(payload.len(), end_after_unit)
                .map_err(ExchangeUnitSenderError::Kernel)?;
            let sequence = range.data_sequence.expect("data publication");
            state.payloads.insert(
                sequence,
                RetainedPayload {
                    bytes: payload.clone(),
                },
            );
            state.pending = Some(sequence);
            range
        };
        self.notify();
        self.refresh();
        Ok(ExchangeUnitPublishOutcome::Published(range))
    }

    pub fn finish(self: &Arc<Self>) -> Result<PublishedStreamRange, ExchangeUnitSenderError> {
        let range = {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if state.status != ExchangeStreamSenderStatus::Open {
                return Err(ExchangeUnitSenderError::Stopped);
            }
            state
                .kernel
                .publish_end()
                .map_err(ExchangeUnitSenderError::Kernel)?
        };
        self.notify();
        self.refresh();
        Ok(range)
    }

    pub fn stop(self: &Arc<Self>) {
        self.terminate(ExchangeStreamSenderStatus::Stopped);
    }

    fn notify(&self) {
        self.observable.defer_notify().arm();
    }

    fn terminate(self: &Arc<Self>, status: ExchangeStreamSenderStatus) {
        let changed = {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if !matches!(state.status, ExchangeStreamSenderStatus::Failed(_))
                && (matches!(status, ExchangeStreamSenderStatus::Failed(_))
                    || !state.cleanup_started)
            {
                state.status = status;
            }
            if state.cleanup_started {
                false
            } else {
                state.cleanup_started = true;
                if state.stop_obligation == ExchangeSenderStopState::NotRequired
                    && state.peer_contact_started
                {
                    state.stop_obligation = ExchangeSenderStopState::Pending;
                }
                state.kernel.seal();
                state.payloads.clear();
                state.pending = None;
                state.replay_due.clear();
                state.retry_pending.clear();
                state.watching = false;
                true
            }
        };
        if !changed {
            self.notify();
            return;
        }
        self.cancellation.cancel();
        self.transmitter.forget_unit_facts(self.config.stream);
        self.drive_stop();
        self.notify();
    }

    /// The Task/context owner calls this only after authoritative retirement of
    /// this exact route. Cancellation or a local RPC timeout is not that fact.
    /// P06 must retain and observe a Pending stop obligation while its owner is
    /// live; local owner destruction is neither confirmation nor route retirement.
    /// Remote cleanup after owner exit remains with query/context lifecycle authority.
    pub fn retire_after_route_retirement(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            state.stop_obligation = ExchangeSenderStopState::RouteRetired;
        }
        self.stop_cancellation.cancel();
        self.notify();
    }

    fn drive_stop(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if state.stop_obligation != ExchangeSenderStopState::Pending
                || state.stop_active
                || state.stop_retry_waiting
            {
                return;
            }
            state.stop_active = true;
            state.active_control_calls = state
                .active_control_calls
                .checked_add(1)
                .expect("exchange control activity count exhausted");
        }
        let sender = Arc::clone(self);
        self.transmitter.register_unit_demands(
            self.config.destination.clone(),
            vec![ExchangeDemandEntry {
                stream: self.config.stream,
                operation: ExchangeDemandOperation::StopSender,
            }],
            self.stop_cancellation.clone(),
            self.control_activity(),
            Box::new(move |outcome| sender.stop_outcome(outcome)),
        );
    }

    // Dispatch reserves this lifetime fact under the same state lock as the
    // callback flag. It grants no DATA position, byte permit or remote fact.
    fn control_activity(self: &Arc<Self>) -> ExchangeCallActivity {
        let sender = Arc::clone(self);
        ExchangeCallActivity::new(move || {
            {
                let mut state = sender.state.lock().expect("exchange stream sender lock");
                state.active_control_calls = state
                    .active_control_calls
                    .checked_sub(1)
                    .expect("one original exchange control capture exit");
            }
            sender.notify();
        })
    }

    // Retry timers count as control activity, but must not retain the sender.
    // Their reservation is made under the same lock as the retry-waiting flag.
    fn retry_activity(self: &Arc<Self>) -> ExchangeCallActivity {
        let sender = Arc::downgrade(self);
        ExchangeCallActivity::new(move || {
            if let Some(sender) = sender.upgrade() {
                {
                    let mut state = sender.state.lock().expect("exchange stream sender lock");
                    state.active_control_calls = state
                        .active_control_calls
                        .checked_sub(1)
                        .expect("one original exchange retry capture exit");
                }
                sender.notify();
            }
        })
    }

    fn stop_outcome(self: &Arc<Self>, outcome: ExchangeDemandCallOutcome) {
        let result = {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            state.stop_active = false;
            if state.stop_obligation != ExchangeSenderStopState::Pending {
                return;
            }
            match outcome {
                ExchangeDemandCallOutcome::RouteRetired(stream) => {
                    if stream != self.config.stream {
                        Err(contract_failure())
                    } else {
                        drop(state);
                        self.retire_after_route_retirement();
                        return;
                    }
                }
                ExchangeDemandCallOutcome::Observed(results) => {
                    let result = if results.len() == 1 && results[0].0 == self.config.stream {
                        results[0].1
                    } else {
                        Err(contract_failure())
                    };
                    match result {
                        Ok(facts) if facts.sender_stopped || facts.receiver_closed.is_some() => {
                            self.merge_locked(&mut state, facts)
                        }
                        Ok(_) => Err(contract_failure()),
                        Err(failure) => Err(failure),
                    }
                }
                ExchangeDemandCallOutcome::Refused(failure) => Err(failure),
                ExchangeDemandCallOutcome::NotProcessed(_)
                | ExchangeDemandCallOutcome::Unknown(_) => {
                    state.stop_retry_waiting = true;
                    state.active_control_calls = state
                        .active_control_calls
                        .checked_add(1)
                        .expect("exchange control activity count exhausted");
                    drop(state);
                    let weak = Arc::downgrade(self);
                    if let Err(failure) = self.transmitter.schedule_unit_retry(
                        self.config.retry_delay,
                        self.retry_activity(),
                        Box::new(move || {
                            if let Some(sender) = weak.upgrade() {
                                sender
                                    .state
                                    .lock()
                                    .expect("exchange stream sender lock")
                                    .stop_retry_waiting = false;
                                sender.drive_stop();
                            }
                        }),
                    ) {
                        self.refuse_stop(failure);
                    }
                    self.notify();
                    return;
                }
            }
        };
        match result {
            Ok(Some(notice)) => self.transmitter.normal_close_accepted(notice),
            Ok(None) => {}
            Err(failure) => self.refuse_stop(failure),
        }
        self.notify();
    }

    fn refuse_stop(&self, failure: FailureFact) {
        let mut state = self.state.lock().expect("exchange stream sender lock");
        if state.stop_obligation == ExchangeSenderStopState::Pending {
            state.stop_obligation = ExchangeSenderStopState::Refused(failure);
            if !matches!(state.status, ExchangeStreamSenderStatus::Failed(_)) {
                state.status = ExchangeStreamSenderStatus::Failed(failure);
            }
        }
    }

    fn fail(self: &Arc<Self>, failure: FailureFact) {
        self.terminate(ExchangeStreamSenderStatus::Failed(failure));
    }

    // Validate against the immutable publication/offer history even after local
    // cleanup sealed the kernel. A late stop proof releases only its stop debt;
    // it never replays an ACK into a sealed stream or revives retained data.
    fn validate_facts_locked(
        &self,
        state: &SenderState,
        facts: ExchangeStreamFacts,
    ) -> Result<u64, FailureFact> {
        let snapshot = state.kernel.snapshot();
        let mut custody_through = facts.custody_through;
        if let Some(closed) = facts.receiver_closed {
            match closed {
                ExchangeUnitCloseDisposition::Withdrawn => {
                    if self.config.delivery == ExchangeDeliveryRequirement::MustDrain {
                        return Err(contract_failure());
                    }
                }
                ExchangeUnitCloseDisposition::Consumed { final_sequence } => {
                    if snapshot.end_sequence != Some(final_sequence.get()) {
                        return Err(contract_failure());
                    }
                    custody_through = custody_through.max(final_sequence.get());
                }
            }
        }
        if custody_through > snapshot.offered_through {
            return Err(contract_failure());
        }
        Ok(custody_through)
    }

    fn merge_locked(
        &self,
        state: &mut SenderState,
        facts: ExchangeStreamFacts,
    ) -> Result<Option<ExchangeNormalCloseAccepted>, FailureFact> {
        let custody_through = self.validate_facts_locked(state, facts)?;
        let confirms_stop = facts.sender_stopped || facts.receiver_closed.is_some();
        if matches!(
            state.status,
            ExchangeStreamSenderStatus::Open | ExchangeStreamSenderStatus::CustodyComplete
        ) {
            state
                .kernel
                .acknowledge(custody_through)
                .map_err(|_| contract_failure())?;
            let custody = state.kernel.snapshot().consumed_through;
            state.payloads.retain(|sequence, _| *sequence > custody);
            state.replay_due.retain(|sequence| *sequence > custody);
            state.retry_pending.retain(|sequence| *sequence > custody);
            state.granted = state.granted.max(facts.granted_through);
            if facts.sender_stopped {
                state.status = ExchangeStreamSenderStatus::Stopped;
            } else if facts.receiver_closed == Some(ExchangeUnitCloseDisposition::Withdrawn) {
                state.status = ExchangeStreamSenderStatus::Withdrawn;
            } else if state
                .kernel
                .snapshot()
                .end_sequence
                .is_some_and(|end| end <= custody)
                && state.status == ExchangeStreamSenderStatus::Open
            {
                state.status = ExchangeStreamSenderStatus::CustodyComplete;
            }
        }
        // Publish confirmation only after every fallible merge check. Keep
        // failure/route-retirement conclusions intact, including on late facts.
        if confirms_stop
            && matches!(
                state.stop_obligation,
                ExchangeSenderStopState::NotRequired | ExchangeSenderStopState::Pending
            )
        {
            state.stop_obligation = ExchangeSenderStopState::Confirmed;
        }
        Ok(facts.receiver_closed.map(|_| ExchangeNormalCloseAccepted {
            stream: self.config.stream,
            sender_count: self.config.sender_count,
            facts,
        }))
    }

    fn observe(self: &Arc<Self>, notice: ExchangeGrantNotice) {
        let result = {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            match notice {
                ExchangeGrantNotice::Facts(facts) => self.merge_locked(&mut state, facts),
                ExchangeGrantNotice::Refused(failure) => Err(failure),
                ExchangeGrantNotice::Resynchronize => {
                    state.demand_registered = None;
                    if let Some(sequence) = Self::oldest_offered(&state) {
                        state.replay_due.insert(sequence);
                    }
                    Ok(None)
                }
            }
        };
        if let Err(failure) = result {
            self.fail(failure);
        } else {
            if let Ok(Some(notice)) = result {
                self.transmitter.normal_close_accepted(notice);
            }
            let status = self.snapshot().status;
            if matches!(
                status,
                ExchangeStreamSenderStatus::Withdrawn | ExchangeStreamSenderStatus::Stopped
            ) {
                self.terminate(status);
            } else {
                self.notify();
                self.refresh();
            }
        }
    }

    fn oldest_offered(state: &SenderState) -> Option<u64> {
        let snapshot = state.kernel.snapshot();
        state
            .payloads
            .keys()
            .copied()
            .find(|sequence| *sequence <= snapshot.offered_through)
            .or(snapshot
                .end_sequence
                .filter(|end| *end > snapshot.consumed_through && *end <= snapshot.offered_through))
    }

    fn refresh(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if state.refreshing {
                state.refresh_again = true;
                return;
            }
            state.refreshing = true;
        }
        loop {
            let (demand, watch, terminal, forget) = {
                let mut state = self.state.lock().expect("exchange stream sender lock");
                let terminal = state.status != ExchangeStreamSenderStatus::Open;
                let demand = if !terminal && !state.demand_active && !state.demand_retry_waiting {
                    state
                        .pending
                        .filter(|pending| state.demand_registered != Some(*pending))
                        .map(|pending| {
                            let payload = &state.payloads[&pending];
                            let demand = ExchangeUnitDemand {
                                sequence: ExchangeUnitSequence::try_new(pending)
                                    .expect("published"),
                                wire_bytes: payload.bytes.len() as u64,
                            };
                            state.demand_registered = Some(pending);
                            state.demand_active = true;
                            state.active_control_calls = state
                                .active_control_calls
                                .checked_add(1)
                                .expect("exchange control activity count exhausted");
                            demand
                        })
                } else {
                    None
                };
                let snapshot = state.kernel.snapshot();
                let watch = !terminal
                    && !state.watching
                    && state.active.is_empty()
                    && snapshot.produced_through > snapshot.consumed_through;
                let forget = terminal && state.watching;
                if watch {
                    state.watching = true;
                }
                if forget {
                    state.watching = false;
                }
                if demand.is_some() || watch {
                    state.peer_contact_started = true;
                }
                (demand, watch, terminal, forget)
            };
            if forget {
                self.transmitter.forget_unit_facts(self.config.stream);
            }
            if let Some(demand) = demand {
                let sender = Arc::clone(self);
                self.transmitter.register_unit_demands(
                    self.config.destination.clone(),
                    vec![ExchangeDemandEntry {
                        stream: self.config.stream,
                        operation: ExchangeDemandOperation::Register(demand),
                    }],
                    self.cancellation.clone(),
                    self.control_activity(),
                    Box::new(move |outcome| sender.demand_outcome(outcome)),
                );
            }
            if watch {
                let weak = Arc::downgrade(self);
                if let Err(failure) = self.transmitter.watch_unit_facts(
                    self.config.destination.clone(),
                    self.config.stream,
                    Arc::new(move |notice| {
                        if let Some(sender) = weak.upgrade() {
                            sender.observe(notice);
                        }
                    }),
                ) {
                    self.fail(failure);
                }
                if self.snapshot().status != ExchangeStreamSenderStatus::Open {
                    self.transmitter.forget_unit_facts(self.config.stream);
                }
            }
            if !terminal && self.candidate().is_some() {
                self.scheduler.request(self);
            }
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if state.refresh_again {
                state.refresh_again = false;
            } else {
                state.refreshing = false;
                break;
            }
        }
    }

    fn demand_outcome(self: &Arc<Self>, outcome: ExchangeDemandCallOutcome) {
        {
            self.state
                .lock()
                .expect("exchange stream sender lock")
                .demand_active = false;
        }
        match outcome {
            ExchangeDemandCallOutcome::RouteRetired(_) => self.fail(contract_failure()),
            ExchangeDemandCallOutcome::Observed(results) => {
                if results.len() != 1 || results[0].0 != self.config.stream {
                    self.fail(contract_failure());
                    return;
                }
                match results[0].1 {
                    Ok(facts) => self.observe(ExchangeGrantNotice::Facts(facts)),
                    Err(failure) => self.fail(failure),
                }
            }
            ExchangeDemandCallOutcome::Refused(failure) => self.fail(failure),
            ExchangeDemandCallOutcome::NotProcessed(_) | ExchangeDemandCallOutcome::Unknown(_) => {
                {
                    let mut state = self.state.lock().expect("exchange stream sender lock");
                    if state.status != ExchangeStreamSenderStatus::Open {
                        return;
                    }
                    state.demand_registered = None;
                    state.demand_retry_waiting = true;
                    state.active_control_calls = state
                        .active_control_calls
                        .checked_add(1)
                        .expect("exchange control activity count exhausted");
                }
                let weak = Arc::downgrade(self);
                if let Err(failure) = self.transmitter.schedule_unit_retry(
                    self.config.retry_delay,
                    self.retry_activity(),
                    Box::new(move || {
                        if let Some(sender) = weak.upgrade() {
                            sender
                                .state
                                .lock()
                                .expect("exchange stream sender lock")
                                .demand_retry_waiting = false;
                            sender.refresh();
                        }
                    }),
                ) {
                    self.fail(failure);
                }
            }
        }
    }

    fn candidate(&self) -> Option<SendCandidate> {
        let state = self.state.lock().expect("exchange stream sender lock");
        if state.status != ExchangeStreamSenderStatus::Open {
            return None;
        }
        let snapshot = state.kernel.snapshot();
        let replay = state
            .replay_due
            .iter()
            .copied()
            .find(|sequence| !state.active.contains_key(sequence));
        let sequence = if let Some(replay) = replay {
            replay
        } else {
            let next = snapshot.offered_through.checked_add(1)?;
            if next > snapshot.produced_through {
                return None;
            }
            if state.payloads.contains_key(&next) {
                let retained = state
                    .payloads
                    .keys()
                    .filter(|sequence| **sequence <= snapshot.offered_through)
                    .count();
                if retained >= self.config.retained_window || next > state.granted {
                    return None;
                }
            }
            next
        };
        let bytes = state
            .payloads
            .get(&sequence)
            .map_or(1, |payload| payload.bytes.len());
        Some(SendCandidate { sequence, bytes })
    }

    fn dispatch(self: &Arc<Self>, candidate: SendCandidate, permit: SendPermit) {
        let prepared = {
            let mut state = self.state.lock().expect("exchange stream sender lock");
            if state.status != ExchangeStreamSenderStatus::Open
                || state.active.contains_key(&candidate.sequence)
                || candidate.sequence <= state.kernel.snapshot().consumed_through
            {
                None
            } else {
                match state.kernel.offer(candidate.sequence) {
                    Ok(OfferOutcome::Ready { item, .. }) => {
                        state.next_call_id = state
                            .next_call_id
                            .checked_add(1)
                            .expect("exchange call identity exhausted");
                        let call = state.next_call_id;
                        state.active.insert(candidate.sequence, call);
                        state.peer_contact_started = true;
                        state.replay_due.remove(&candidate.sequence);
                        if state.pending == Some(candidate.sequence) {
                            state.pending = None;
                        }
                        let (item, payload) = match item.kind {
                            RetainedItemKind::Data { end_after_data, .. } => {
                                let payload = &state.payloads[&candidate.sequence];
                                (
                                    ExchangeUnitItem::Unit {
                                        demand: ExchangeUnitDemand {
                                            sequence: ExchangeUnitSequence::try_new(
                                                candidate.sequence,
                                            )
                                            .expect("published"),
                                            wire_bytes: payload.bytes.len() as u64,
                                        },
                                        end_after_unit: end_after_data,
                                    },
                                    payload.bytes.clone(),
                                )
                            }
                            RetainedItemKind::End => (
                                ExchangeUnitItem::End(
                                    ExchangeUnitSequence::try_new(candidate.sequence)
                                        .expect("published"),
                                ),
                                Bytes::new(),
                            ),
                        };
                        Some((
                            call,
                            ExchangeUnitFrame {
                                destination: self.config.destination.clone(),
                                receiver: ExchangeUnitReceiverFrame {
                                    stream: self.config.stream,
                                    sender_count: self.config.sender_count,
                                    sender_id: self.config.sender_id,
                                    backend_number: self.config.backend_number,
                                    item,
                                    payload,
                                    next_demand: None,
                                },
                            },
                        ))
                    }
                    _ => None,
                }
            }
        };
        let Some((call, frame)) = prepared else {
            drop(permit);
            return;
        };
        let sender = Arc::clone(self);
        let activity = ExchangeCallActivity::new(move || {
            {
                let mut state = sender.state.lock().expect("exchange stream sender lock");
                if state.active.get(&candidate.sequence) == Some(&call) {
                    state.active.remove(&candidate.sequence);
                }
            }
            sender.notify();
            sender.refresh();
            drop(permit);
        });
        let sender = Arc::clone(self);
        self.transmitter.deliver_unit(
            frame,
            self.cancellation.clone(),
            activity,
            Box::new(move |outcome| sender.call_outcome(candidate.sequence, outcome)),
        );
        self.notify();
        self.refresh();
    }

    fn call_outcome(self: &Arc<Self>, sequence: u64, outcome: ExchangeCallOutcome) {
        match outcome {
            ExchangeCallOutcome::Observed(receipt) => {
                self.observe(ExchangeGrantNotice::Facts(receipt.facts))
            }
            ExchangeCallOutcome::Refused(failure) => self.fail(failure),
            ExchangeCallOutcome::NotProcessed(_) | ExchangeCallOutcome::Unknown(_) => {
                let schedule = {
                    let mut state = self.state.lock().expect("exchange stream sender lock");
                    let schedule = state.status == ExchangeStreamSenderStatus::Open
                        && sequence > state.kernel.snapshot().consumed_through
                        && state.retry_pending.insert(sequence);
                    if schedule {
                        state.active_control_calls = state
                            .active_control_calls
                            .checked_add(1)
                            .expect("exchange control activity count exhausted");
                    }
                    schedule
                };
                if !schedule {
                    return;
                }
                let weak = Arc::downgrade(self);
                if let Err(failure) = self.transmitter.schedule_unit_retry(
                    self.config.retry_delay,
                    self.retry_activity(),
                    Box::new(move || {
                        if let Some(sender) = weak.upgrade() {
                            let mut state =
                                sender.state.lock().expect("exchange stream sender lock");
                            state.retry_pending.remove(&sequence);
                            if state.status == ExchangeStreamSenderStatus::Open
                                && sequence > state.kernel.snapshot().consumed_through
                            {
                                state.replay_due.insert(sequence);
                            }
                            drop(state);
                            sender.refresh();
                        }
                    }),
                ) {
                    self.fail(failure);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "exchange_stream_sender_tests.rs"]
mod tests;

impl Drop for ExchangeStreamSender {
    fn drop(&mut self) {
        // Actual RPC callbacks retain the sender through their bounded work.
        // At the final owner exit only local capabilities are revoked: no new
        // stop request, receiver-close fact or reclamation proof is authored.
        self.cancellation.cancel();
        self.stop_cancellation.cancel();
        self.transmitter.forget_unit_facts(self.config.stream);
    }
}
