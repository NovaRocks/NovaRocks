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

//! Harness-only authenticated plaintext H2 response-message fault actor.
//! Its owned-buffer counters exclude H2 internals, kernel buffers and DTO metadata.

//! Authenticated BE message faults on the original bounded H2 actor.

use super::*;
use novarocks_execution_contract::exchange_flow::ExchangeStreamIdentity;
use novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;
use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeAdmissionVerdict, ExchangeUnitItem, ExchangeUnitSequence,
};
use novarocks_task_codec::exchange_unit_codec::{
    self as codec, DecodedExchangeDeliveryOutcome, DecodedExchangeGrantSubscription,
    ExchangeProcessPair,
};
use novarocks_types::BackendProcessId;
use novarocks_types::identity::QueryExecutionId;
use std::num::NonZeroU64;

const DELIVER: &str = "/novarocks.NovaRocksGrpc/DeliverExchangeUnit";
const DEMAND: &str = "/novarocks.NovaRocksGrpc/RegisterExchangeDemand";
const GRANTS: &str = "/novarocks.NovaRocksGrpc/SubscribeExchangeGrants";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ExchangeFaultAction {
    HoldDataBeforeForward,
    DropAcceptedReply,
    HoldReplyAfterAdmission,
    CutGrantSubscription,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExchangeFaultSelector {
    pub destination_node: Option<i32>,
    pub backend_index: Option<usize>,
    pub query_execution_id: Option<QueryExecutionId>,
}
impl ExchangeFaultSelector {
    fn matches(self, stream: ExchangeStreamIdentity, backend: usize) -> bool {
        self.destination_node
            .is_none_or(|node| node == stream.destination_node().get())
            && self.backend_index.is_none_or(|index| index == backend)
            && self
                .query_execution_id
                .is_none_or(|query| query == stream.destination().query_execution_id())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeFaultTarget {
    Unit {
        stream: ExchangeStreamIdentity,
        sequence: ExchangeUnitSequence,
    },
    Subscription {
        stream: ExchangeStreamIdentity,
        pair: ExchangeProcessPair,
        generation: NonZeroU64,
    },
}
impl ExchangeFaultTarget {
    pub const fn stream(self) -> ExchangeStreamIdentity {
        match self {
            Self::Unit { stream, .. } | Self::Subscription { stream, .. } => stream,
        }
    }
    pub const fn sequence(self) -> Option<ExchangeUnitSequence> {
        match self {
            Self::Unit { sequence, .. } => Some(sequence),
            _ => None,
        }
    }
    pub const fn generation(self) -> Option<NonZeroU64> {
        match self {
            Self::Subscription { generation, .. } => Some(generation),
            _ => None,
        }
    }
}
impl Serialize for ExchangeFaultTarget {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serde_json::json!({"stream":format!("{:?}",self.stream()),
            "source_task":format!("{:?}",self.stream().source().task()),
            "destination_task":format!("{:?}",self.stream().destination()),
            "destination_node":self.stream().destination_node().get(),
            "sequence":self.sequence().map(ExchangeUnitSequence::get),
            "generation":self.generation().map(NonZeroU64::get)})
        .serialize(serializer)
    }
}

/// The original signed request is observed, never re-signed by this actor.
/// The runner must independently match the selected statement and live roster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeFaultCandidate {
    pub target: ExchangeFaultTarget,
    pub backend_index: usize,
    pub caller: BackendProcessId,
    pub downstream_stream: u32,
    pub request_sha256: String,
}
impl Serialize for ExchangeFaultCandidate {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serde_json::json!({"target":self.target,"backend_index":self.backend_index,
            "caller":format!("{:?}",self.caller),"downstream_stream":self.downstream_stream,
            "request_sha256":self.request_sha256})
        .serialize(serializer)
    }
}

/// One bounded evidence position per actual target RPC, updated in place.
#[derive(Clone, Debug, Serialize)]
pub struct ExchangeFaultAttempt {
    pub candidate: ExchangeFaultCandidate,
    pub upstream_stream: Option<u32>,
    pub replay: bool,
    pub started_millis: u64,
    pub data_forward_held_at_millis: Option<u64>,
    pub request_forwarded_at_millis: Option<u64>,
    pub admission_observed_at_millis: Option<u64>,
    pub custody_through: Option<u64>,
    pub receiver_closed: Option<bool>,
    pub sender_stopped: Option<bool>,
    pub verdict: Option<&'static str>,
    pub reply_dropped: bool,
    pub reply_held_at_millis: Option<u64>,
    pub subscription_cut: bool,
    pub peer_cancelled: bool,
    pub stream_future_exited_at_millis: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExchangeFaultObservation {
    pub action: Option<ExchangeFaultAction>,
    pub candidate: Option<ExchangeFaultCandidate>,
    pub armed: bool,
    pub released: bool,
    pub elapsed_millis: u64,
    pub active_target_streams: usize,
    pub attempts: Vec<ExchangeFaultAttempt>,
    pub data_forward_held: u64,
    pub requests_forwarded: u64,
    pub admissions_observed: u64,
    pub replies_dropped: u64,
    pub replies_held: u64,
    pub subscriptions_cut: u64,
    pub replay_requests: u64,
    pub replay_idempotent: u64,
    pub rebuilt_generation: Option<u64>,
    pub rebuilt_stream: Option<ExchangeFaultTarget>,
    pub rebuilt_snapshot_complete: bool,
    pub rebuilt_stream_granted_through: Option<u64>,
    pub peer_cancels: u64,
    pub custody_batches_held: u64,
    pub active_custody_batches_held: usize,
    pub custody_released_after_replay: bool,
}

#[derive(Default)]
pub(super) struct ExchangeState {
    peers: Vec<(usize, NativeCallerSubject, BackendProcessId)>,
    session_deadline: Option<Instant>,
    capture: Option<CaptureState>,
}
enum CaptureState {
    Live(Capture),
    RetiredReceipt(CaptureReceipt),
}

struct Capture(CaptureReceipt);

// Only bounded scalar identities, hashes and attempt observations. No future,
// payload, ByteCredit, thread, connection, secret or resource permit is retained.
struct CaptureReceipt {
    retired_elapsed_millis: Option<u64>,
    action: ExchangeFaultAction,
    selector: ExchangeFaultSelector,
    started: Instant,
    deadline: Instant,
    candidate: Option<ExchangeFaultCandidate>,
    cut_unit: Option<ExchangeFaultCandidate>,
    armed: bool,
    released: bool,
    fired: bool,
    active: usize,
    attempts: Vec<ExchangeFaultAttempt>,
    rebuilt_generation: Option<u64>,
    rebuilt_stream: Option<ExchangeFaultTarget>,
    rebuilt_snapshot_complete: bool,
    rebuilt_stream_granted_through: Option<u64>,
    peer_cancels: u64,
    custody_batches_held: u64,
    active_custody_batches_held: usize,
    custody_released_after_replay: bool,
}
impl CaptureReceipt {
    fn millis(&self) -> u64 {
        if let Some(elapsed) = self.retired_elapsed_millis {
            return elapsed;
        }
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

impl ExchangeState {
    fn live_capture(&self) -> Option<&CaptureReceipt> {
        match self.capture.as_ref() {
            Some(CaptureState::Live(capture)) => Some(&capture.0),
            _ => None,
        }
    }
    fn live_capture_mut(&mut self) -> Option<&mut CaptureReceipt> {
        match self.capture.as_mut() {
            Some(CaptureState::Live(capture)) => Some(&mut capture.0),
            _ => None,
        }
    }
    fn receipt(&self) -> Option<&CaptureReceipt> {
        match self.capture.as_ref() {
            Some(CaptureState::Live(capture)) => Some(&capture.0),
            Some(CaptureState::RetiredReceipt(receipt)) => Some(receipt),
            None => None,
        }
    }
    pub(super) fn retire_after_group_join(&mut self, _joined: &JoinedRootReplyGroup) -> Result<()> {
        if let Some(capture) = self.live_capture() {
            ensure!(
                capture.active == 0 && capture.active_custody_batches_held == 0,
                "Exchange target or custody work has not actually exited"
            );
        }
        self.capture = match self.capture.take() {
            Some(CaptureState::Live(Capture(mut receipt))) => {
                receipt.retired_elapsed_millis = Some(receipt.millis());
                Some(CaptureState::RetiredReceipt(receipt))
            }
            other => other,
        };
        Ok(())
    }
    pub(super) fn occupied(&self) -> bool {
        self.live_capture().is_some()
    }
    pub(super) fn pending_deadline(&self) -> Option<Instant> {
        self.live_capture()
            .filter(|capture| !capture.released && !capture.fired)
            .map(|capture| capture.deadline)
    }
    pub(super) fn expire(&mut self, deadline: Instant) -> bool {
        if let Some(capture) = self
            .live_capture_mut()
            .filter(|capture| capture.deadline == deadline && !capture.released && !capture.fired)
        {
            capture.released = true;
            true
        } else {
            false
        }
    }
}

impl RootReplyFaultControl {
    pub(crate) fn bind_exchange_peers(
        &self,
        peers: Vec<(usize, NativeCallerSubject, BackendProcessId)>,
        session_deadline: Instant,
    ) -> Result<()> {
        ensure!(
            !peers.is_empty() && peers.len() <= 3,
            "Exchange actor requires the original live backend roster"
        );
        ensure!(
            session_deadline > Instant::now(),
            "Exchange fault test session has expired"
        );
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            state
                .exchange
                .session_deadline
                .is_none_or(|deadline| deadline == session_deadline),
            "Exchange fault session deadline cannot be extended or replaced"
        );
        state.exchange.session_deadline = Some(session_deadline);
        ensure!(
            !state.lifecycle_closed,
            "Exchange actor lifecycle is closed"
        );
        for (position, (index, subject, process)) in peers.iter().enumerate() {
            ensure!(
                peers[..position]
                    .iter()
                    .all(|(old_index, old_subject, old_process)| old_index != index
                        && old_subject != subject
                        && old_process != process),
                "Exchange actor roster repeats a backend identity"
            );
        }
        if !state.exchange.peers.is_empty() {
            ensure!(
                state.exchange.peers == peers,
                "Exchange actor live process roster changed"
            );
        } else {
            state.exchange.peers = peers;
        }
        Ok(())
    }
    pub fn begin_exchange_capture(
        &self,
        action: ExchangeFaultAction,
        deadline: Instant,
    ) -> Result<()> {
        self.begin_exchange_capture_on(action, ExchangeFaultSelector::default(), deadline)
    }
    pub fn begin_exchange_capture_on(
        &self,
        action: ExchangeFaultAction,
        selector: ExchangeFaultSelector,
        deadline: Instant,
    ) -> Result<()> {
        let now = Instant::now();
        ensure!(
            deadline > now
                && deadline.duration_since(now)
                    <= Duration::from_millis(self.core.bounds.maximum_capture_millis),
            "Exchange capture exceeds the original absolute capture deadline"
        );
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            !state.lifecycle_closed && state.failures.is_empty() && !state.failure_overflow,
            "Exchange actor is not healthy and open"
        );
        ensure!(
            !state.exchange.peers.is_empty()
                && state
                    .exchange
                    .session_deadline
                    .is_some_and(|end| end >= deadline),
            "Exchange capture requires the original unexpired test session and live backend roster"
        );
        ensure!(
            (!state.used_capture || state.exchange.occupied())
                && state.slot.is_none()
                && !state.pending_capture,
            "Root and Exchange faults cannot own the target slot together"
        );
        ensure!(
            state
                .exchange
                .live_capture()
                .is_none_or(|capture| capture.active == 0
                    && capture.active_custody_batches_held == 0
                    && (capture.released || capture.fired)),
            "Previous Exchange target owners have not exited"
        );
        state.used_capture = true;
        state.exchange.capture = Some(CaptureState::Live(Capture(CaptureReceipt {
            retired_elapsed_millis: None,
            action,
            selector,
            started: now,
            deadline,
            candidate: None,
            cut_unit: None,
            armed: false,
            released: false,
            fired: false,
            active: 0,
            attempts: Vec::new(),
            rebuilt_generation: None,
            rebuilt_stream: None,
            rebuilt_snapshot_complete: false,
            rebuilt_stream_granted_through: None,
            peer_cancels: 0,
            custody_batches_held: 0,
            active_custody_batches_held: 0,
            custody_released_after_replay: false,
        })));

        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }
    pub fn exchange_candidate(&self) -> Option<ExchangeFaultCandidate> {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .exchange
            .receipt()
            .and_then(|capture| capture.candidate.clone())
    }
    pub fn arm_exchange_exact(&self, candidate: &ExchangeFaultCandidate) -> Result<()> {
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            !state.lifecycle_closed && state.failures.is_empty() && !state.failure_overflow,
            "Exchange actor cannot arm after failure or close"
        );
        let capture = state
            .exchange
            .live_capture_mut()
            .context("Exchange capture was not started")?;
        ensure!(
            !capture.armed && !capture.released && capture.deadline > Instant::now(),
            "Exchange candidate is already armed, released or expired"
        );
        ensure!(
            capture.candidate.as_ref() == Some(candidate),
            "Exchange candidate differs from the exact captured signed request"
        );
        capture.armed = true;
        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }
    pub fn release_exchange(&self) {
        if let Some(capture) = self
            .core
            .state
            .lock()
            .expect("root fault state lock")
            .exchange
            .live_capture_mut()
        {
            capture.released = true;
        }
        self.core.changed.notify_waiters();
    }
    pub fn exchange_observation(&self) -> ExchangeFaultObservation {
        let state = self.core.state.lock().expect("root fault state lock");
        let capture = state.exchange.receipt();
        let attempts = capture
            .map(|capture| capture.attempts.clone())
            .unwrap_or_default();
        ExchangeFaultObservation {
            action: capture.map(|capture| capture.action),
            candidate: capture.and_then(|capture| capture.candidate.clone()),
            armed: capture.is_some_and(|capture| capture.armed),
            released: capture.is_some_and(|capture| capture.released),
            elapsed_millis: capture.map_or(0, CaptureReceipt::millis),
            active_target_streams: capture.map_or(0, |capture| capture.active),
            data_forward_held: attempts
                .iter()
                .filter(|attempt| attempt.data_forward_held_at_millis.is_some())
                .count() as u64,
            requests_forwarded: attempts
                .iter()
                .filter(|attempt| attempt.request_forwarded_at_millis.is_some())
                .count() as u64,
            admissions_observed: attempts
                .iter()
                .filter(|attempt| attempt.admission_observed_at_millis.is_some())
                .count() as u64,
            replies_dropped: attempts
                .iter()
                .filter(|attempt| attempt.reply_dropped)
                .count() as u64,
            replies_held: attempts
                .iter()
                .filter(|attempt| attempt.reply_held_at_millis.is_some())
                .count() as u64,
            subscriptions_cut: attempts
                .iter()
                .filter(|attempt| attempt.subscription_cut)
                .count() as u64,
            replay_requests: attempts.iter().filter(|attempt| attempt.replay).count() as u64,
            replay_idempotent: attempts
                .iter()
                .filter(|attempt| {
                    attempt.replay
                        && attempt.verdict == Some("Idempotent")
                        && capture
                            .and_then(|capture| capture.candidate.as_ref())
                            .is_some_and(|selected| selected.target == attempt.candidate.target)
                })
                .count() as u64,
            rebuilt_generation: capture.and_then(|capture| capture.rebuilt_generation),
            rebuilt_stream: capture.and_then(|capture| capture.rebuilt_stream),
            rebuilt_snapshot_complete: capture
                .is_some_and(|capture| capture.rebuilt_snapshot_complete),
            rebuilt_stream_granted_through: capture
                .and_then(|capture| capture.rebuilt_stream_granted_through),
            peer_cancels: capture.map_or(0, |capture| capture.peer_cancels),
            custody_batches_held: capture.map_or(0, |capture| capture.custody_batches_held),
            active_custody_batches_held: capture
                .map_or(0, |capture| capture.active_custody_batches_held),
            custody_released_after_replay: capture
                .is_some_and(|capture| capture.custody_released_after_replay),
            attempts,
        }
    }
}

pub(super) fn enabled_method(core: &Core, request: &Request<RecvStream>) -> bool {
    matches!(request.uri().path(), DELIVER | DEMAND | GRANTS)
        && !core
            .state
            .lock()
            .expect("root fault state lock")
            .exchange
            .peers
            .is_empty()
}
fn authenticate<T>(
    core: &Core,
    request: &Request<T>,
    backend_index: usize,
) -> Result<BackendProcessId> {
    ensure!(
        request.method() == http::Method::POST,
        "Exchange actor requires POST"
    );
    let caller = core
        .verifier
        .admit_headers(request.headers())
        .map_err(anyhow::Error::msg)?;
    let Some(NativeProcessIdentity::Backend(process)) = caller.process_identity() else {
        bail!("Exchange actor requires a signed backend process");
    };
    let state = core.state.lock().expect("root fault state lock");
    ensure!(
        state
            .exchange
            .peers
            .iter()
            .any(|(_, subject, id)| subject == caller.subject() && *id == process),
        "Exchange caller subject and process differ from the live backend roster"
    );
    ensure!(
        state
            .exchange
            .peers
            .iter()
            .any(|(index, _, _)| *index == backend_index),
        "Exchange destination listener is outside the original roster"
    );
    Ok(process)
}
fn require_pair(
    core: &Core,
    pair: ExchangeProcessPair,
    caller: BackendProcessId,
    index: usize,
) -> Result<()> {
    ensure!(
        pair.sender == caller,
        "Exchange sender differs from the signed backend process"
    );
    let state = core.state.lock().expect("root fault state lock");
    ensure!(
        state
            .exchange
            .peers
            .iter()
            .any(|(backend, _, process)| *backend == index && *process == pair.receiver),
        "Exchange receiver differs from the exact live destination process"
    );
    Ok(())
}

/// Native decoded-message size remains the protocol bound. Actual actor Vec
/// capacity is funded incrementally by the existing shared owned-buffer ledger.
fn append_growing(buffer: &mut OwnedBuffer, value: &[u8]) -> Result<()> {
    let next = buffer
        .bytes
        .len()
        .checked_add(value.len())
        .context("Exchange message length overflow")?;
    ensure!(
        next <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES + 5,
        "Exchange actor message exceeds the native decoded-message bound"
    );
    if next > buffer.bytes.capacity() {
        let mut credit = ByteCredit::new(buffer._credit.context.clone(), next)?;
        buffer
            .bytes
            .try_reserve_exact(next - buffer.bytes.len())
            .context("grow credited Exchange actor buffer")?;
        if buffer.bytes.capacity() > next {
            let mut extra = ByteCredit::new(
                buffer._credit.context.clone(),
                buffer.bytes.capacity() - next,
            )?;
            credit.bytes += extra.bytes;
            extra.bytes = 0;
        }
        std::mem::swap(&mut buffer._credit, &mut credit);
    }
    buffer.bytes.extend_from_slice(value);
    Ok(())
}
async fn unary_body(
    body: &mut RecvStream,
    buffers: BufferContext,
) -> Result<(Bytes, Option<HeaderMap>)> {
    let mut buffer = OwnedBuffer::new(buffers, 0)?;
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        ensure!(
            chunk.len() <= FRAME_BYTES,
            "Exchange actor received oversized H2 DATA"
        );
        append_growing(&mut buffer, &chunk)?;
        body.flow_control().release_capacity(chunk.len())?;
    }
    let trailers = body.trailers().await?;
    Ok((buffer.into_bytes(), trailers))
}

struct AttemptOwner {
    core: Arc<Core>,
    index: usize,
    mutate: bool,
}
impl Drop for AttemptOwner {
    fn drop(&mut self) {
        let mut state = self.core.state.lock().expect("root fault state lock");
        if let Some(capture) = state.exchange.live_capture_mut() {
            let now = capture.millis();
            capture.attempts[self.index].stream_future_exited_at_millis = Some(now);
            capture.active -= 1;
        }
        drop(state);
        self.core.changed.notify_waiters();
    }
}
impl AttemptOwner {
    fn update(&self, apply: impl FnOnce(&mut ExchangeFaultAttempt, u64)) {
        let mut state = self.core.state.lock().expect("root fault state lock");
        let capture = state
            .exchange
            .live_capture_mut()
            .expect("original Exchange capture owner");
        let now = capture.millis();
        apply(&mut capture.attempts[self.index], now);
        drop(state);
        self.core.changed.notify_waiters();
    }
}
fn select_unit(
    core: &Arc<Core>,
    candidate: ExchangeFaultCandidate,
) -> Result<Option<AttemptOwner>> {
    let mut state = core.state.lock().expect("root fault state lock");
    let Some(capture) = state.exchange.live_capture_mut() else {
        return Ok(None);
    };
    if !capture
        .selector
        .matches(candidate.target.stream(), candidate.backend_index)
    {
        return Ok(None);
    }
    if capture.action == ExchangeFaultAction::CutGrantSubscription {
        if capture.released || Instant::now() >= capture.deadline {
            return Ok(None);
        }
        if capture
            .candidate
            .as_ref()
            .is_some_and(|selected| selected.target.stream() != candidate.target.stream())
        {
            return Ok(None);
        }
        if capture.cut_unit.is_none() {
            capture.cut_unit = Some(candidate.clone());
        }
        let selected = capture
            .cut_unit
            .as_ref()
            .expect("original cut-subscription Unit");
        if selected.target != candidate.target || selected.backend_index != candidate.backend_index
        {
            return Ok(None);
        }
        ensure!(
            selected.caller == candidate.caller
                && selected.request_sha256 == candidate.request_sha256,
            "Cut-subscription Unit retry changed its exact signed bytes"
        );
        let owner = add_attempt(capture, candidate, core.clone())?;
        drop(state);
        core.changed.notify_waiters();
        return Ok(Some(owner));
    }
    if capture.candidate.is_none() {
        if capture.released || Instant::now() >= capture.deadline {
            return Ok(None);
        }
        capture.candidate = Some(candidate.clone());
    }
    let frozen = capture
        .candidate
        .as_ref()
        .expect("selected Exchange candidate");
    if frozen.target != candidate.target || frozen.backend_index != candidate.backend_index {
        let successor = frozen.target.stream() == candidate.target.stream()
            && frozen.backend_index == candidate.backend_index
            && frozen
                .target
                .sequence()
                .and_then(|sequence| sequence.checked_next().ok())
                == candidate.target.sequence();
        if successor {
            let mut owner = add_attempt(capture, candidate, core.clone())?;
            owner.mutate = false;
            drop(state);
            core.changed.notify_waiters();
            return Ok(Some(owner));
        }
        return Ok(None);
    }
    ensure!(
        frozen.caller == candidate.caller && frozen.request_sha256 == candidate.request_sha256,
        "Exact Exchange Unit replay changed its signed identity or request bytes"
    );
    let owner = add_attempt(capture, candidate, core.clone())?;
    drop(state);
    core.changed.notify_waiters();
    Ok(Some(owner))
}
fn add_attempt(
    capture: &mut CaptureReceipt,
    candidate: ExchangeFaultCandidate,
    core: Arc<Core>,
) -> Result<AttemptOwner> {
    ensure!(
        u64::try_from(capture.attempts.len())? < MAX_TARGET_ATTEMPTS,
        "Exchange target exhausted the existing actor attempt evidence positions"
    );
    let index = capture.attempts.len();
    let now = capture.millis();
    let replay = capture
        .attempts
        .iter()
        .any(|attempt| attempt.candidate.target == candidate.target);
    capture.attempts.push(ExchangeFaultAttempt {
        candidate,
        upstream_stream: None,
        replay,
        started_millis: now,
        data_forward_held_at_millis: None,
        request_forwarded_at_millis: None,
        admission_observed_at_millis: None,
        custody_through: None,
        receiver_closed: None,
        sender_stopped: None,
        verdict: None,
        reply_dropped: false,
        reply_held_at_millis: None,
        subscription_cut: false,
        peer_cancelled: false,
        stream_future_exited_at_millis: None,
    });
    capture.active += 1;
    Ok(AttemptOwner {
        core,
        index,
        mutate: true,
    })
}

async fn wait_gate(
    core: &Arc<Core>,
    response: &mut server::SendResponse<Bytes>,
    owner: &AttemptOwner,
    release: bool,
) -> Result<bool> {
    loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let (armed, released, rebuilt, deadline) = {
            let state = core.state.lock().expect("root fault state lock");
            let capture = state
                .exchange
                .live_capture()
                .expect("original Exchange gate");
            let rebuilt = capture.action == ExchangeFaultAction::CutGrantSubscription
                && capture.rebuilt_snapshot_complete
                && capture.rebuilt_stream.is_some()
                && capture
                    .rebuilt_stream_granted_through
                    .is_some_and(|grant| grant > 0);
            (capture.armed, capture.released, rebuilt, capture.deadline)
        };
        if released || rebuilt || (armed && !release) {
            return Ok(true);
        }
        tokio::select! {
            _=&mut changed => {},
            reset=poll_fn(|cx|response.poll_reset(cx)) => {
                let reason=reset.context("observe held Exchange downstream reset")?;
                ensure!(reason==h2::Reason::CANCEL,"Held Exchange request reset with an unexpected reason");
                owner.update(|attempt,_|attempt.peer_cancelled=true);return Ok(false)
            },
            _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => bail!("Exchange capture absolute deadline expired while holding its original target"),
        }
    }
}

fn peer_cancel(error: &anyhow::Error) -> bool {
    // A scoped send has already checked its original output handle and
    // current scope. The older generic catch must not override that refusal.
    if error.downcast_ref::<DeliveryOutputFailure>().is_some()
        || error.downcast_ref::<GrantOutputFailure>().is_some()
    {
        return false;
    }
    error.downcast_ref::<h2::Error>().is_some_and(|error|error.is_reset() && error.is_remote() && error.reason()==Some(h2::Reason::CANCEL))
        || error.downcast_ref::<CapacityClosed>().is_some_and(|closed|
            matches!(closed.reset,CapacityResetObservation::Reason(reason) if reason==u32::from(h2::Reason::CANCEL)))
}

/// Constructed only after the original BE authentication, full unary decode,
/// request EOF and exact live process-pair validation have succeeded.
#[derive(Clone, Copy, Debug)]
struct VerifiedGrantHeaderScope {
    expected: DecodedExchangeGrantSubscription,
    caller: BackendProcessId,
    backend_index: usize,
    downstream: u32,
}

#[derive(Debug)]
enum GrantCancelRejection {
    ExpiredSession,
    RetiredScope,
    InvalidLivePair,
    UnknownCandidate,
    InvalidCandidateKind,
    ProtectedCustodyPair,
    ProtectedSubscriptionPair,
}

fn same_signed_target(a: &ExchangeFaultCandidate, b: &ExchangeFaultCandidate) -> bool {
    a.target == b.target
        && a.backend_index == b.backend_index
        && a.caller == b.caller
        && a.request_sha256 == b.request_sha256
}

fn observed_open_unit_exited(attempt: &ExchangeFaultAttempt) -> bool {
    attempt.request_forwarded_at_millis.is_some()
        && attempt.admission_observed_at_millis.is_some()
        && matches!(attempt.verdict, Some("Applied" | "Idempotent"))
        && attempt.receiver_closed == Some(false)
        && attempt.sender_stopped == Some(false)
        && !attempt.peer_cancelled
        && attempt.stream_future_exited_at_millis.is_some()
}

fn fault_attempts_exited(capture: &CaptureReceipt) -> bool {
    !capture.attempts.is_empty()
        && capture.active == 0
        && capture
            .attempts
            .iter()
            .all(|attempt| attempt.stream_future_exited_at_millis.is_some())
}

// These facts end only the actor's fault obligation. They do not retire the
// capture, prove Consumed, refund bytes or replace the original group join.
fn dropped_reply_obligation_completed(capture: &CaptureReceipt) -> bool {
    let Some(candidate) = capture.candidate.as_ref() else {
        return false;
    };
    matches!(candidate.target, ExchangeFaultTarget::Unit { .. })
        && capture.armed && capture.fired && fault_attempts_exited(capture)
        && capture.custody_batches_held > 0 && capture.custody_released_after_replay
        // Only the original CustodyBatchOwner destructor decrements this field.
        && capture.active_custody_batches_held == 0
        && capture.attempts.iter().any(|attempt|
            same_signed_target(&attempt.candidate, candidate) && !attempt.replay
                && attempt.reply_dropped && attempt.verdict == Some("Applied")
                && observed_open_unit_exited(attempt))
        && capture.attempts.iter().any(|attempt|
            same_signed_target(&attempt.candidate, candidate) && attempt.replay
                && !attempt.reply_dropped && attempt.verdict == Some("Idempotent")
                && observed_open_unit_exited(attempt))
}

fn cut_subscription_obligation_completed(
    capture: &CaptureReceipt,
    scope: VerifiedGrantHeaderScope,
) -> bool {
    let Some(candidate) = capture.candidate.as_ref() else {
        return false;
    };
    let ExchangeFaultTarget::Subscription {
        stream,
        pair,
        generation,
    } = candidate.target
    else {
        return false;
    };
    let Some(ExchangeFaultTarget::Subscription {
        stream: rebuilt,
        pair: rebuilt_pair,
        generation: rebuilt_generation,
    }) = capture.rebuilt_stream
    else {
        return false;
    };
    let Some(unit) = capture.cut_unit.as_ref() else {
        return false;
    };
    capture.armed && capture.fired && fault_attempts_exited(capture)
        && capture.active_custody_batches_held == 0
        && scope.caller == candidate.caller && scope.backend_index == candidate.backend_index
        && pair == scope.expected.pair && rebuilt_pair == pair && rebuilt == stream
        && rebuilt_generation > generation
        // Bind the actual decoded snapshot to this exact verified logical watch,
        // not every later generation on the same pair or an H2 numeric ordering.
        && Some(rebuilt_generation.get()) == capture.rebuilt_generation
        && scope.expected.generation == rebuilt_generation
        && capture.rebuilt_snapshot_complete
        && capture.rebuilt_stream_granted_through.is_some_and(|grant| grant > 0)
        && matches!(unit.target, ExchangeFaultTarget::Unit { stream: held, .. } if held == stream)
        && capture.attempts.iter().any(|attempt|
            same_signed_target(&attempt.candidate, candidate) && attempt.subscription_cut
                && !attempt.peer_cancelled && attempt.request_forwarded_at_millis.is_some()
                && attempt.stream_future_exited_at_millis.is_some())
        && capture.attempts.iter().any(|attempt|
            same_signed_target(&attempt.candidate, unit)
                && attempt.data_forward_held_at_millis.is_some()
                && observed_open_unit_exited(attempt))
}

fn non_target_grant_headers(
    state: &ExchangeState,
    scope: VerifiedGrantHeaderScope,
) -> std::result::Result<(), GrantCancelRejection> {
    if matches!(state.capture, Some(CaptureState::RetiredReceipt(_))) {
        return Err(GrantCancelRejection::RetiredScope);
    }
    if !state
        .session_deadline
        .is_some_and(|deadline| deadline > Instant::now())
    {
        return Err(GrantCancelRejection::ExpiredSession);
    }
    let pair = scope.expected.pair;
    if scope.caller != pair.sender
        || !state
            .peers
            .iter()
            .any(|(_, _, process)| *process == scope.caller)
        || !state
            .peers
            .iter()
            .any(|(index, _, process)| *index == scope.backend_index && *process == pair.receiver)
    {
        return Err(GrantCancelRejection::InvalidLivePair);
    }
    let Some(capture) = state.live_capture() else {
        return Ok(());
    };
    match capture.action {
        // These closed actions select only DeliverExchangeUnit. A grant RPC
        // cannot become their target while the Unit candidate is being chosen.
        ExchangeFaultAction::HoldDataBeforeForward
        | ExchangeFaultAction::HoldReplyAfterAdmission => {
            if capture.candidate.as_ref().is_none_or(|candidate| {
                matches!(candidate.target, ExchangeFaultTarget::Unit { .. })
            }) {
                Ok(())
            } else {
                Err(GrantCancelRejection::InvalidCandidateKind)
            }
        }
        // A watch can carry the selected Unit's custody in any generation.
        // Explicit release does not prove that the original obligation settled.
        ExchangeFaultAction::DropAcceptedReply => {
            match capture.candidate.as_ref().map(|candidate| candidate.target) {
                Some(ExchangeFaultTarget::Unit { stream, .. })
                    if ExchangeProcessPair::for_stream(stream) != pair =>
                {
                    Ok(())
                }
                Some(ExchangeFaultTarget::Unit { .. }) => {
                    if dropped_reply_obligation_completed(capture) {
                        Ok(())
                    } else {
                        Err(GrantCancelRejection::ProtectedCustodyPair)
                    }
                }
                Some(_) => Err(GrantCancelRejection::InvalidCandidateKind),
                None => Err(GrantCancelRejection::UnknownCandidate),
            }
        }
        // Protect both the original subscription and every same-pair generation,
        // including the rebuild which must furnish the real complete snapshot.
        ExchangeFaultAction::CutGrantSubscription => {
            match capture.candidate.as_ref().map(|candidate| candidate.target) {
                Some(ExchangeFaultTarget::Subscription { pair: selected, .. })
                    if selected != pair =>
                {
                    Ok(())
                }
                Some(ExchangeFaultTarget::Subscription { .. }) => {
                    if cut_subscription_obligation_completed(capture, scope) {
                        Ok(())
                    } else {
                        Err(GrantCancelRejection::ProtectedSubscriptionPair)
                    }
                }
                Some(_) => Err(GrantCancelRejection::InvalidCandidateKind),
                None => Err(GrantCancelRejection::UnknownCandidate),
            }
        }
    }
}

async fn recover_grant_cancel_before_headers(
    core: &Core,
    scope: VerifiedGrantHeaderScope,
    response: &mut server::SendResponse<Bytes>,
    inbound: &mut RecvStream,
    sending: &mut SendStream<Bytes>,
) -> bool {
    if response.stream_id().as_u32() != scope.downstream
        || inbound.stream_id() != response.stream_id()
    {
        return false;
    }
    let reset = poll_fn(|cx| std::task::Poll::Ready(response.poll_reset(cx))).await;
    if !matches!(reset, std::task::Poll::Ready(Ok(h2::Reason::CANCEL))) {
        return false;
    }
    let input = poll_fn(|cx| std::task::Poll::Ready(inbound.poll_data(cx))).await;
    match input {
        std::task::Poll::Ready(None) => {}
        std::task::Poll::Ready(Some(Err(error)))
            if error.is_reset()
                && error.is_remote()
                && error.reason() == Some(h2::Reason::CANCEL) => {}
        _ => return false,
    }
    settle_grant_peer_cancel(core, ObservedGrantPeerCancel { scope }, sending).is_ok()
}

// This observation is constructed only after a nonblocking original H2 handle
// returns Ready(CANCEL). A boolean or a failed send is not a reset receipt.
struct ObservedGrantPeerCancel {
    scope: VerifiedGrantHeaderScope,
}

fn settle_grant_peer_cancel(
    core: &Core,
    observed: ObservedGrantPeerCancel,
    sending: &mut SendStream<Bytes>,
) -> std::result::Result<(), GrantCancelRejection> {
    let mut state = core.state.lock().expect("root fault state lock");
    // The capture may have changed while the original upstream headers were
    // awaited. Reclassify under its current lock; no old classification grants
    // cancellation authority over a new target or its custody/rebuild watch.
    non_target_grant_headers(&state.exchange, observed.scope)?;
    // No local reset has been sent to this non-target downstream stream. The
    // original handle's independently observed CANCEL is the peer's.
    // Retire only the corresponding original upstream RPC, never its connection.
    sending.send_reset(h2::Reason::CANCEL);
    if let Some(capture) = state.exchange.live_capture_mut() {
        capture.peer_cancels = capture.peer_cancels.saturating_add(1);
    }
    drop(state);
    core.changed.notify_waiters();
    Ok(())
}

#[derive(Debug)]
enum GrantCancelProbe {
    Settled,
    StreamMismatch { actual: u32, expected: u32 },
    Pending,
    ReadyOther(u32),
    ObservationError(FailureDigest),
    ReadyCancelRejected(GrantCancelRejection),
}

impl std::fmt::Display for GrantCancelProbe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Settled => formatter.write_str("Ready(CANCEL) settled"),
            Self::StreamMismatch { actual, expected } => write!(
                formatter,
                "stream-mismatch(actual={actual},expected={expected})"
            ),
            Self::Pending => formatter.write_str("Pending"),
            Self::ReadyOther(reason) => write!(formatter, "Ready(reason={reason})"),
            Self::ObservationError(error) => write!(formatter, "observation-error({error:?})"),
            Self::ReadyCancelRejected(reason) => {
                write!(formatter, "Ready(CANCEL) rejected={reason:?}")
            }
        }
    }
}

async fn recover_grant_cancel_after_headers(
    core: &Core,
    scope: VerifiedGrantHeaderScope,
    output: &mut SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
) -> GrantCancelProbe {
    if output.stream_id().as_u32() != scope.downstream {
        return GrantCancelProbe::StreamMismatch {
            actual: output.stream_id().as_u32(),
            expected: scope.downstream,
        };
    }
    // Request EOF is an immutable fact of the same original verified scope.
    // The Streaming handle now owns reset observation; SendResponse cannot be
    // polled in this phase. This probe never waits or adds an idle deadline.
    match poll_fn(|cx| std::task::Poll::Ready(output.poll_reset(cx))).await {
        std::task::Poll::Pending => GrantCancelProbe::Pending,
        std::task::Poll::Ready(Err(error)) => GrantCancelProbe::ObservationError(digest_display(
            "grant-output-reset-observation",
            &error,
        )),
        std::task::Poll::Ready(Ok(reason)) if reason != h2::Reason::CANCEL => {
            GrantCancelProbe::ReadyOther(u32::from(reason))
        }
        std::task::Poll::Ready(Ok(_)) => {
            match settle_grant_peer_cancel(core, ObservedGrantPeerCancel { scope }, sending) {
                Ok(()) => GrantCancelProbe::Settled,
                Err(rejection) => GrantCancelProbe::ReadyCancelRejected(rejection),
            }
        }
    }
}

enum GrantOutput {
    Message(Bytes),
    Trailers(HeaderMap),
    End,
}

#[derive(Debug)]
struct GrantOutputFailure {
    stage: &'static str,
    scope: VerifiedGrantHeaderScope,
    reset: GrantCancelProbe,
}

impl std::fmt::Display for GrantOutputFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Exchange grant stage={} scope={:?} reset={}",
            self.stage, self.scope, self.reset
        )
    }
}

async fn forward_grant_output(
    core: &Core,
    scope: VerifiedGrantHeaderScope,
    output: &mut SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
    part: GrantOutput,
) -> Result<bool> {
    let (stage, outcome) = match part {
        GrantOutput::Message(message) => ("send-message", send_bytes(output, message).await),
        GrantOutput::Trailers(trailers) => (
            "send-trailers",
            output.send_trailers(trailers).map_err(anyhow::Error::from),
        ),
        GrantOutput::End => (
            "send-eof",
            output
                .send_data(Bytes::new(), true)
                .map_err(anyhow::Error::from),
        ),
    };
    match outcome {
        Ok(()) => Ok(true),
        Err(error) => {
            let reset = recover_grant_cancel_after_headers(core, scope, output, sending).await;
            if matches!(reset, GrantCancelProbe::Settled) {
                return Ok(false);
            }
            Err(error).context(GrantOutputFailure {
                stage,
                scope,
                reset,
            })
        }
    }
}

/// Only the original authenticated, decoded delivery with request EOF creates
/// this metadata. It retains neither the request payload nor an attempt owner.
#[derive(Clone, Copy, Debug)]
struct VerifiedDeliveryScope {
    stream: ExchangeStreamIdentity,
    item: ExchangeUnitItem,
    caller: BackendProcessId,
    backend_index: usize,
    downstream: u32,
    session_deadline: Instant,
    capture: Option<(Instant, Instant)>,
    tracked_attempt: bool,
}

fn verified_delivery_scope(
    core: &Core,
    frame: &codec::DecodedExchangeUnitFrame,
    caller: BackendProcessId,
    backend_index: usize,
    downstream: u32,
    tracked_attempt: bool,
) -> Result<VerifiedDeliveryScope> {
    require_pair(
        core,
        ExchangeProcessPair::for_stream(frame.stream),
        caller,
        backend_index,
    )?;
    let state = core.state.lock().expect("root fault state lock");
    Ok(VerifiedDeliveryScope {
        stream: frame.stream,
        item: frame.item,
        caller,
        backend_index,
        downstream,
        session_deadline: state
            .exchange
            .session_deadline
            .context("delivery scope has no original session")?,
        capture: state
            .exchange
            .live_capture()
            .map(|capture| (capture.started, capture.deadline)),
        tracked_attempt,
    })
}

#[derive(Debug)]
enum DeliveryCancelRejection {
    ExpiredSession,
    RetiredScope,
    ChangedCapture,
    ExpiredCapture,
    InvalidLivePair,
    UnknownCandidate,
    InvalidCandidateKind,
    ProtectedOriginalAttempt,
    ProtectedSelectedStream,
    ProtectedCustodyPair,
    ProtectedSubscriptionPair,
}

fn non_target_delivery_output(
    state: &ExchangeState,
    scope: VerifiedDeliveryScope,
) -> std::result::Result<(), DeliveryCancelRejection> {
    if state.session_deadline != Some(scope.session_deadline)
        || scope.session_deadline <= Instant::now()
    {
        return Err(DeliveryCancelRejection::ExpiredSession);
    }
    if matches!(state.capture, Some(CaptureState::RetiredReceipt(_))) {
        return Err(DeliveryCancelRejection::RetiredScope);
    }
    let pair = ExchangeProcessPair::for_stream(scope.stream);
    if scope.caller != pair.sender
        || !state
            .peers
            .iter()
            .any(|(_, _, process)| *process == scope.caller)
        || !state
            .peers
            .iter()
            .any(|(index, _, process)| *index == scope.backend_index && *process == pair.receiver)
    {
        return Err(DeliveryCancelRejection::InvalidLivePair);
    }
    let current = state.live_capture();
    if current.map(|capture| (capture.started, capture.deadline)) != scope.capture {
        return Err(DeliveryCancelRejection::ChangedCapture);
    }
    if scope.tracked_attempt {
        return Err(DeliveryCancelRejection::ProtectedOriginalAttempt);
    }
    let Some(capture) = current else {
        return Ok(());
    };
    if capture.deadline <= Instant::now() {
        return Err(DeliveryCancelRejection::ExpiredCapture);
    }
    let Some(candidate) = capture.candidate.as_ref() else {
        return Err(DeliveryCancelRejection::UnknownCandidate);
    };
    match capture.action {
        ExchangeFaultAction::HoldDataBeforeForward
        | ExchangeFaultAction::HoldReplyAfterAdmission => match candidate.target {
            ExchangeFaultTarget::Unit { stream, .. } if stream != scope.stream => Ok(()),
            ExchangeFaultTarget::Unit { .. } => {
                Err(DeliveryCancelRejection::ProtectedSelectedStream)
            }
            _ => Err(DeliveryCancelRejection::InvalidCandidateKind),
        },
        ExchangeFaultAction::DropAcceptedReply => match candidate.target {
            ExchangeFaultTarget::Unit { stream, .. }
                if ExchangeProcessPair::for_stream(stream) != pair =>
            {
                Ok(())
            }
            ExchangeFaultTarget::Unit { .. } => Err(DeliveryCancelRejection::ProtectedCustodyPair),
            _ => Err(DeliveryCancelRejection::InvalidCandidateKind),
        },
        ExchangeFaultAction::CutGrantSubscription => match candidate.target {
            ExchangeFaultTarget::Subscription { pair: selected, .. }
                if selected != pair
                    && capture.cut_unit.as_ref().is_none_or(|unit| {
                        ExchangeProcessPair::for_stream(unit.target.stream()) != pair
                    }) =>
            {
                Ok(())
            }
            ExchangeFaultTarget::Subscription { .. } => {
                Err(DeliveryCancelRejection::ProtectedSubscriptionPair)
            }
            _ => Err(DeliveryCancelRejection::InvalidCandidateKind),
        },
    }
}

// Only a real original H2 handle's Ready(CANCEL) constructs this observation.
struct ObservedDeliveryPeerCancel(VerifiedDeliveryScope);

fn settle_delivery_peer_cancel(
    core: &Core,
    observed: ObservedDeliveryPeerCancel,
    sending: &mut SendStream<Bytes>,
) -> std::result::Result<(), DeliveryCancelRejection> {
    let mut state = core.state.lock().expect("root fault state lock");
    non_target_delivery_output(&state.exchange, observed.0)?;
    sending.send_reset(h2::Reason::CANCEL);
    if let Some(capture) = state.exchange.live_capture_mut() {
        capture.peer_cancels = capture.peer_cancels.saturating_add(1);
    }
    drop(state);
    core.changed.notify_waiters();
    Ok(())
}

#[derive(Debug)]
enum DeliveryCancelProbe {
    Settled,
    StreamMismatch,
    RequestNotEnded,
    Pending,
    ReadyOther(u32),
    ObservationError(FailureDigest),
    ReadyCancelRejected(DeliveryCancelRejection),
}
impl std::fmt::Display for DeliveryCancelProbe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Settled => formatter.write_str("Ready(CANCEL) settled"),
            Self::StreamMismatch => formatter.write_str("stream-mismatch"),
            Self::RequestNotEnded => formatter.write_str("request-not-ended"),
            Self::Pending => formatter.write_str("Pending"),
            Self::ReadyOther(reason) => write!(formatter, "Ready(reason={reason})"),
            Self::ObservationError(error) => write!(formatter, "observation-error({error:?})"),
            Self::ReadyCancelRejected(reason) => {
                write!(formatter, "Ready(CANCEL) rejected={reason:?}")
            }
        }
    }
}

enum DeliveryResetHandle<'a> {
    Headers(&'a mut server::SendResponse<Bytes>, &'a mut RecvStream),
    Body(&'a mut SendStream<Bytes>),
}

async fn recover_delivery_output_cancel(
    core: &Core,
    scope: VerifiedDeliveryScope,
    handle: DeliveryResetHandle<'_>,
    sending: &mut SendStream<Bytes>,
) -> DeliveryCancelProbe {
    let reset = match handle {
        DeliveryResetHandle::Headers(response, inbound) => {
            if response.stream_id().as_u32() != scope.downstream
                || inbound.stream_id() != response.stream_id()
            {
                return DeliveryCancelProbe::StreamMismatch;
            }
            let reset = poll_fn(|cx| std::task::Poll::Ready(response.poll_reset(cx))).await;
            if matches!(reset, std::task::Poll::Ready(Ok(h2::Reason::CANCEL))) {
                match poll_fn(|cx| std::task::Poll::Ready(inbound.poll_data(cx))).await {
                    std::task::Poll::Ready(None) => {}
                    std::task::Poll::Ready(Some(Err(error)))
                        if error.is_reset()
                            && error.is_remote()
                            && error.reason() == Some(h2::Reason::CANCEL) => {}
                    _ => return DeliveryCancelProbe::RequestNotEnded,
                }
            }
            reset
        }
        DeliveryResetHandle::Body(output) => {
            if output.stream_id().as_u32() != scope.downstream {
                return DeliveryCancelProbe::StreamMismatch;
            }
            poll_fn(|cx| std::task::Poll::Ready(output.poll_reset(cx))).await
        }
    };
    match reset {
        std::task::Poll::Pending => DeliveryCancelProbe::Pending,
        std::task::Poll::Ready(Err(error)) => {
            DeliveryCancelProbe::ObservationError(digest_display("delivery-output-reset", &error))
        }
        std::task::Poll::Ready(Ok(reason)) if reason != h2::Reason::CANCEL => {
            DeliveryCancelProbe::ReadyOther(u32::from(reason))
        }
        std::task::Poll::Ready(Ok(_)) => {
            match settle_delivery_peer_cancel(core, ObservedDeliveryPeerCancel(scope), sending) {
                Ok(()) => DeliveryCancelProbe::Settled,
                Err(reason) => DeliveryCancelProbe::ReadyCancelRejected(reason),
            }
        }
    }
}

#[derive(Debug)]
struct DeliveryOutputFailure {
    stage: &'static str,
    scope: VerifiedDeliveryScope,
    reset: DeliveryCancelProbe,
}
impl std::fmt::Display for DeliveryOutputFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Exchange delivery stage={} scope={:?} item={:?} reset={}",
            self.stage, self.scope, self.scope.item, self.reset
        )
    }
}

async fn delivery_response_headers(
    core: &Core,
    scope: VerifiedDeliveryScope,
    response: &mut server::SendResponse<Bytes>,
    inbound: &mut RecvStream,
    sending: &mut SendStream<Bytes>,
    parts: http::response::Parts,
) -> Result<Option<SendStream<Bytes>>> {
    if matches!(
        recover_delivery_output_cancel(
            core,
            scope,
            DeliveryResetHandle::Headers(response, inbound),
            sending
        )
        .await,
        DeliveryCancelProbe::Settled
    ) {
        return Ok(None);
    }
    match response.send_response(Response::from_parts(parts, ()), false) {
        Ok(output) => Ok(Some(output)),
        Err(error) => {
            let reset = recover_delivery_output_cancel(
                core,
                scope,
                DeliveryResetHandle::Headers(response, inbound),
                sending,
            )
            .await;
            if matches!(reset, DeliveryCancelProbe::Settled) {
                return Ok(None);
            }
            Err(error).context(DeliveryOutputFailure {
                stage: "response-headers",
                scope,
                reset,
            })
        }
    }
}

async fn forward_delivery_output(
    core: &Core,
    scope: VerifiedDeliveryScope,
    output: &mut SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
    part: GrantOutput,
) -> Result<bool> {
    let (stage, result) = match part {
        GrantOutput::Message(bytes) => ("send-message", send_bytes(output, bytes).await),
        GrantOutput::Trailers(trailers) => (
            "send-trailers",
            output.send_trailers(trailers).map_err(anyhow::Error::from),
        ),
        GrantOutput::End => (
            "send-eof",
            output
                .send_data(Bytes::new(), true)
                .map_err(anyhow::Error::from),
        ),
    };
    match result {
        Ok(()) => Ok(true),
        Err(error) => {
            let reset = recover_delivery_output_cancel(
                core,
                scope,
                DeliveryResetHandle::Body(output),
                sending,
            )
            .await;
            if matches!(reset, DeliveryCancelProbe::Settled) {
                return Ok(false);
            }
            Err(error).context(DeliveryOutputFailure {
                stage,
                scope,
                reset,
            })
        }
    }
}

async fn copy_delivery_body(
    core: &Core,
    scope: VerifiedDeliveryScope,
    mut input: RecvStream,
    mut output: SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
    buffers: BufferContext,
) -> Result<()> {
    while let Some(bytes) = input.data().await {
        let bytes = bytes
            .with_context(|| format!("Exchange delivery stage=read-message scope={scope:?}"))?;
        ensure!(
            bytes.len() <= FRAME_BYTES,
            "delivery forwarding received oversized DATA frame"
        );
        let mut copy = OwnedBuffer::new(buffers.clone(), FRAME_BYTES)?;
        copy.append(&bytes)?;
        input.flow_control().release_capacity(bytes.len())?;
        drop(bytes);
        if !forward_delivery_output(
            core,
            scope,
            &mut output,
            sending,
            GrantOutput::Message(copy.into_bytes()),
        )
        .await?
        {
            return Ok(());
        }
    }
    let trailers = input
        .trailers()
        .await
        .with_context(|| format!("Exchange delivery stage=read-trailers scope={scope:?}"))?;
    forward_delivery_output(
        core,
        scope,
        &mut output,
        sending,
        trailers.map_or(GrantOutput::End, GrantOutput::Trailers),
    )
    .await?;
    Ok(())
}

fn delivery_stage<T, E: Into<anyhow::Error>>(
    result: std::result::Result<T, E>,
    scope: Option<VerifiedDeliveryScope>,
    stage: &'static str,
) -> Result<T> {
    let result: Result<T> = result.map_err(Into::into);
    match scope {
        Some(scope) => {
            result.with_context(|| format!("Exchange delivery stage={stage} scope={scope:?}"))
        }
        None => result,
    }
}

pub(super) async fn forward(
    request: Request<RecvStream>,
    mut response: server::SendResponse<Bytes>,
    sender: client::SendRequest<Bytes>,
    backend_index: usize,
    buffers: BufferContext,
) -> Result<()> {
    let core = buffers.core.clone();
    let caller = match authenticate(&core, &request, backend_index) {
        Err(error) if request.uri().path() == DELIVER => {
            return Err(error).context("Exchange delivery stage=authenticate scope=unverified");
        }
        result => result?,
    };
    let path = request.uri().path().to_owned();
    let downstream = response.stream_id().as_u32();
    require_uncompressed(request.headers())?;
    let (parts, mut inbound) = request.into_parts();
    let mut verified = false;
    let mut owner: Option<AttemptOwner> = None;
    let work = async {
        let (body, trailers) = match unary_body(&mut inbound, buffers.clone()).await {
            Err(error) if path == DELIVER => {
                return Err(error).context("Exchange delivery stage=read-request scope=unverified");
            }
            result => result?,
        };
        ensure!(
            trailers.as_ref().is_none_or(HeaderMap::is_empty),
            "Exchange request contains unexpected trailers"
        );
        let mut unit = None;
        let mut subscription = None;
        let mut delivery = None;
        match path.as_str() {
            DELIVER => {
                let wire =
                    wire::DeliverExchangeUnitRequest::decode(single_message(&body).context(
                        "Exchange delivery stage=decode-request-envelope scope=unverified",
                    )?)
                    .context("Exchange delivery stage=decode-request-protobuf scope=unverified")?;
                let frame = codec::decode_delivery_request(
                    wire,
                    FieldPath::root("exchange_fault_delivery_request"),
                )
                .context("Exchange delivery stage=decode-request-frame scope=unverified")?;
                require_pair(
                    &core,
                    ExchangeProcessPair::for_stream(frame.stream),
                    caller,
                    backend_index,
                )
                .context("Exchange delivery stage=verify-request-pair scope=unverified")?;
                if matches!(frame.item, ExchangeUnitItem::Unit { .. }) {
                    owner = select_unit(
                        &core,
                        ExchangeFaultCandidate {
                            target: ExchangeFaultTarget::Unit {
                                stream: frame.stream,
                                sequence: frame.item.sequence(),
                            },
                            backend_index,
                            caller,
                            downstream_stream: downstream,
                            request_sha256: hash(&body),
                        },
                    )?;
                }
                delivery = Some(verified_delivery_scope(
                    &core,
                    &frame,
                    caller,
                    backend_index,
                    downstream,
                    owner.is_some(),
                )?);
                unit = Some(frame);
            }
            DEMAND => {
                let wire = wire::RegisterExchangeDemandRequest::decode(single_message(&body)?)?;
                let batch = codec::decode_demand_request(
                    &wire,
                    FieldPath::root("exchange_fault_demand_request"),
                )?;
                require_pair(&core, batch.pair, caller, backend_index)?;
            }
            GRANTS => {
                let wire = wire::SubscribeExchangeGrantsRequest::decode(single_message(&body)?)?;
                let decoded = codec::decode_grant_subscription(
                    &wire,
                    FieldPath::root("exchange_fault_subscription_request"),
                )?;
                require_pair(&core, decoded.pair, caller, backend_index)?;
                subscription = Some(VerifiedGrantHeaderScope {
                    expected: decoded,
                    caller,
                    backend_index,
                    downstream,
                });
            }
            _ => bail!("Exchange actor received an unowned method"),
        }
        verified = true;
        if let Some(owner) = owner.as_ref().filter(|owner| owner.mutate) {
            let hold = {
                let state = core.state.lock().expect("root fault state lock");
                let action = state
                    .exchange
                    .live_capture()
                    .expect("original Exchange target")
                    .action;
                matches!(
                    action,
                    ExchangeFaultAction::HoldDataBeforeForward
                        | ExchangeFaultAction::CutGrantSubscription
                )
            };
            if hold {
                owner.update(|attempt, now| attempt.data_forward_held_at_millis = Some(now));
            }
            // All retries of this exact Unit share the original absolute gate.
            // No upstream request exists before this gate returns.
            if !wait_gate(&core, &mut response, owner, hold).await? {
                return Ok(());
            }
        }
        let mut sender = delivery_stage(sender.ready().await, delivery, "upstream-ready")?;
        let (reply, mut sending) = delivery_stage(
            sender.send_request(Request::from_parts(parts, ()), false),
            delivery,
            "send-request-headers",
        )?;
        let upstream = sending.stream_id().as_u32();
        let dispatched = Instant::now();
        if let Some(owner) = owner.as_ref() {
            owner.update(|attempt, now| {
                attempt.upstream_stream = Some(upstream);
                attempt.request_forwarded_at_millis = Some(now);
            });
        }
        delivery_stage(
            send_bytes(&mut sending, body.clone()).await,
            delivery,
            "send-request-message",
        )?;
        delivery_stage(
            sending.send_data(Bytes::new(), true),
            delivery,
            "send-request-eof",
        )?;
        // Only connection/response establishment has the original forward bound.
        // A live grant subscription has no invented actor idle timeout.
        let upstream_response = delivery_stage(
            tokio::time::timeout(
                Duration::from_millis(core.bounds.forward_timeout_millis),
                reply,
            )
            .await
            .context("Exchange upstream response establishment deadline")
            .and_then(|reply| reply.map_err(anyhow::Error::from)),
            delivery,
            "await-upstream-headers",
        )?;
        let (reply_parts, mut reply_body) = upstream_response.into_parts();
        if let Some(scope) = subscription {
            if recover_grant_cancel_before_headers(
                &core,
                scope,
                &mut response,
                &mut inbound,
                &mut sending,
            )
            .await
            {
                return Ok(());
            }
            let mut output =
                match response.send_response(Response::from_parts(reply_parts, ()), false) {
                    Ok(output) => output,
                    Err(error) => {
                        // The original connection can process RST after the first
                        // probe but before this transition. Observe its typed reset
                        // again; the failed transition is not cancellation proof.
                        if recover_grant_cancel_before_headers(
                            &core,
                            scope,
                            &mut response,
                            &mut inbound,
                            &mut sending,
                        )
                        .await
                        {
                            return Ok(());
                        }
                        return Err(error).with_context(|| {
                            format!("Exchange grant stage=response-headers scope={scope:?}")
                        });
                    }
                };
            return forward_grants(
                &core,
                buffers.clone(),
                &mut reply_body,
                &mut output,
                &mut sending,
                scope,
                upstream,
                hash(&body),
                dispatched,
            )
            .await
            .with_context(|| format!("Exchange grant stage=response-body scope={scope:?}"));
        }
        if let Some(owner) = owner.as_ref() {
            let scope = delivery.context("Exchange delivery output has no verified scope")?;
            let (reply_bytes, reply_trailers) = unary_body(&mut reply_body, buffers.clone())
                .await
                .with_context(|| {
                    format!("Exchange delivery stage=read-unary-response scope={scope:?}")
                })?;
            let frame = unit.as_ref().context("Exchange target has no Unit frame")?;
            // Header-only RPC failures remain the real upstream RPC outcome.
            // They never authorize an accepted-receipt mutation.
            if !reply_parts.headers.contains_key("grpc-status") {
                delivery_stage(
                    success_status(&reply_parts.headers, reply_trailers.as_ref()),
                    Some(scope),
                    "validate-upstream-status",
                )?;
                let decoded = delivery_stage(
                    wire::DeliverExchangeUnitResponse::decode(delivery_stage(
                        single_message(&reply_bytes),
                        Some(scope),
                        "decode-response-envelope",
                    )?),
                    Some(scope),
                    "decode-response-protobuf",
                )?;
                if let DecodedExchangeDeliveryOutcome::Observed { verdict, facts } = delivery_stage(
                    codec::decode_delivery_response(
                        &decoded,
                        frame.stream,
                        frame.item.sequence(),
                        FieldPath::root("exchange_fault_delivery_response"),
                    ),
                    Some(scope),
                    "decode-response-receipt",
                )? {
                    owner.update(|attempt, now| {
                        attempt.admission_observed_at_millis = Some(now);
                        attempt.custody_through = Some(facts.custody_through);
                        attempt.receiver_closed = Some(facts.receiver_closed.is_some());
                        attempt.sender_stopped = Some(facts.sender_stopped);
                        attempt.verdict = Some(match verdict {
                            ExchangeAdmissionVerdict::Applied => "Applied",
                            ExchangeAdmissionVerdict::Idempotent => "Idempotent",
                        });
                    });
                    let action = {
                        let mut state = core.state.lock().expect("root fault state lock");
                        let capture = state
                            .exchange
                            .live_capture_mut()
                            .expect("original Exchange target");
                        if owner.mutate
                            && capture.armed
                            && !capture.released
                            && !facts.sender_stopped
                            && facts.receiver_closed.is_none()
                        {
                            if capture.action == ExchangeFaultAction::DropAcceptedReply
                                && !capture.fired
                            {
                                capture.fired = true;
                                Some(capture.action)
                            } else if capture.action == ExchangeFaultAction::HoldReplyAfterAdmission
                            {
                                Some(capture.action)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    match action {
                        Some(ExchangeFaultAction::DropAcceptedReply) => {
                            owner.update(|attempt, _| attempt.reply_dropped = true);
                            response.send_reset(h2::Reason::CANCEL);
                            return Ok(());
                        }
                        Some(ExchangeFaultAction::HoldReplyAfterAdmission) => {
                            owner.update(|attempt, now| attempt.reply_held_at_millis = Some(now));
                            if !wait_gate(&core, &mut response, owner, true).await? {
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                }
            }
            let Some(mut output) = delivery_response_headers(
                &core,
                scope,
                &mut response,
                &mut inbound,
                &mut sending,
                reply_parts,
            )
            .await?
            else {
                return Ok(());
            };
            if !forward_delivery_output(
                &core,
                scope,
                &mut output,
                &mut sending,
                GrantOutput::Message(reply_bytes),
            )
            .await?
            {
                return Ok(());
            }
            forward_delivery_output(
                &core,
                scope,
                &mut output,
                &mut sending,
                reply_trailers.map_or(GrantOutput::End, GrantOutput::Trailers),
            )
            .await?;
        } else if let Some(scope) = delivery {
            let Some(output) = delivery_response_headers(
                &core,
                scope,
                &mut response,
                &mut inbound,
                &mut sending,
                reply_parts,
            )
            .await?
            else {
                return Ok(());
            };
            copy_delivery_body(
                &core,
                scope,
                reply_body,
                output,
                &mut sending,
                buffers.clone(),
            )
            .await?;
        } else {
            let output = response.send_response(Response::from_parts(reply_parts, ()), false)?;
            copy_body(reply_body, output, buffers.clone()).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let outcome = if path == GRANTS {
        work.await
    } else {
        tokio::time::timeout(
            Duration::from_millis(core.bounds.forward_timeout_millis),
            work,
        )
        .await
        .context("Exchange unary forwarding deadline")?
    };
    match outcome {
        Err(error) if verified && peer_cancel(&error) => {
            if let Some(owner)=owner.as_ref() {owner.update(|attempt,_|attempt.peer_cancelled=true);}
            if let Some(capture)=core.state.lock().expect("root fault state lock").exchange.live_capture_mut() {capture.peer_cancels+=1;}
            Ok(())
        },
        result=>result.with_context(||format!("Exchange actor method={path} backend_index={backend_index} downstream_stream={downstream}")),
    }
}

struct CustodyBatchOwner {
    core: Arc<Core>,
}
impl Drop for CustodyBatchOwner {
    fn drop(&mut self) {
        if let Some(capture) = self
            .core
            .state
            .lock()
            .expect("root fault state lock")
            .exchange
            .live_capture_mut()
        {
            capture.active_custody_batches_held -= 1;
        }
        self.core.changed.notify_waiters();
    }
}
async fn hold_selected_custody(
    core: &Arc<Core>,
    batch: &codec::DecodedExchangeGrantBatch,
    output: &mut SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
) -> Result<bool> {
    let hold = {
        let mut state = core.state.lock().expect("root fault state lock");
        if let Some(capture) = state.exchange.live_capture_mut() {
            let selected = capture.action == ExchangeFaultAction::DropAcceptedReply
                && capture.armed
                && !capture.released
                && !capture.attempts.iter().any(|attempt| {
                    attempt.replay
                        && attempt.verdict == Some("Idempotent")
                        && capture
                            .candidate
                            .as_ref()
                            .is_some_and(|selected| selected.target == attempt.candidate.target)
                })
                && capture
                    .candidate
                    .as_ref()
                    .is_some_and(|candidate| match candidate.target {
                        ExchangeFaultTarget::Unit { stream, sequence } => {
                            batch.states.iter().any(|state| {
                                state.stream == stream
                                    && state.facts.custody_through >= sequence.get()
                            })
                        }
                        _ => false,
                    });
            if selected {
                capture.custody_batches_held += 1;
                capture.active_custody_batches_held += 1;
            }
            selected
        } else {
            false
        }
    };
    if !hold {
        return Ok(true);
    }
    let _owner = CustodyBatchOwner { core: core.clone() };
    core.changed.notify_waiters();
    loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let (ready, deadline) = {
            let mut state = core.state.lock().expect("root fault state lock");
            let capture = state
                .exchange
                .live_capture_mut()
                .expect("original custody batch owner");
            let replay = capture.attempts.iter().any(|attempt| {
                attempt.replay
                    && attempt.verdict == Some("Idempotent")
                    && capture
                        .candidate
                        .as_ref()
                        .is_some_and(|selected| selected.target == attempt.candidate.target)
            });
            if replay {
                capture.custody_released_after_replay = true;
            }
            (capture.released || replay, capture.deadline)
        };
        if ready {
            return Ok(true);
        }
        tokio::select! {
            _=&mut changed=>{},
            reset=poll_fn(|cx|output.poll_reset(cx))=>{
                ensure!(reset?==h2::Reason::CANCEL,"Held custody subscription received an unexpected reset");sending.send_reset(h2::Reason::CANCEL);return Ok(false)
            },
            _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))=>bail!("Exchange custody hold expired before a real idempotent replay response"),
        }
    }
}

/// Keep at most one original H2 DATA slice and one credited message. No
/// intermediate queue or extra byte authority is introduced for subscriptions.
struct MessageReader {
    pending: Bytes,
}
impl MessageReader {
    async fn next(
        &mut self,
        body: &mut RecvStream,
        buffers: BufferContext,
    ) -> Result<Option<Bytes>> {
        let mut buffer = OwnedBuffer::new(buffers, 0)?;
        let mut wanted = 5;
        loop {
            if buffer.bytes.len() == wanted {
                if wanted == 5 {
                    ensure!(buffer.bytes[0] == 0, "Exchange grant message is compressed");
                    let length = u32::from_be_bytes(buffer.bytes[1..5].try_into()?) as usize;
                    ensure!(
                        length <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
                        "Exchange grant message exceeds the native decoded-message bound"
                    );
                    wanted = length + 5;
                    if wanted == 5 {
                        return Ok(Some(buffer.into_bytes()));
                    }
                } else {
                    return Ok(Some(buffer.into_bytes()));
                }
            }
            if self.pending.is_empty() {
                let Some(chunk) = body.data().await else {
                    ensure!(
                        buffer.bytes.is_empty(),
                        "Exchange grant stream ended inside a message"
                    );
                    return Ok(None);
                };
                self.pending = chunk?;
                ensure!(
                    self.pending.len() <= FRAME_BYTES,
                    "Exchange grant stream received oversized H2 DATA"
                );
                if self.pending.is_empty() {
                    continue;
                }
            }
            let count = (wanted - buffer.bytes.len()).min(self.pending.len());
            append_growing(&mut buffer, &self.pending[..count])?;
            self.pending = self.pending.slice(count..);
            body.flow_control().release_capacity(count)?;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn forward_grants(
    core: &Arc<Core>,
    buffers: BufferContext,
    body: &mut RecvStream,
    output: &mut SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
    scope: VerifiedGrantHeaderScope,
    upstream: u32,
    request_sha256: String,
    dispatched: Instant,
) -> Result<()> {
    let VerifiedGrantHeaderScope {
        expected,
        backend_index,
        caller,
        downstream,
    } = scope;
    let mut reader = MessageReader {
        pending: Bytes::new(),
    };
    while let Some(message) = reader
        .next(body, buffers.clone())
        .await
        .context("Exchange grant stage=read-message")?
    {
        let wire = wire::ExchangeGrantBatch::decode(
            single_message(&message).context("Exchange grant stage=decode-envelope")?,
        )
        .context("Exchange grant stage=decode-protobuf")?;
        let batch = codec::decode_grant_batch(
            &wire,
            expected,
            FieldPath::root("exchange_fault_grant_batch"),
        )
        .context("Exchange grant stage=decode-batch")?;
        if !hold_selected_custody(core, &batch, output, sending).await? {
            return Ok(());
        }
        let selected = {
            let mut state = core.state.lock().expect("root fault state lock");
            if let Some(capture) = state.exchange.live_capture_mut() {
                if let Some(ExchangeFaultCandidate {
                    target:
                        ExchangeFaultTarget::Subscription {
                            stream,
                            pair,
                            generation,
                        },
                    ..
                }) = capture.candidate.as_ref()
                    && capture.fired
                    && expected.pair == *pair
                    && expected.generation > *generation
                {
                    if capture.rebuilt_generation.is_none() {
                        capture.rebuilt_generation = Some(expected.generation.get());
                    }

                    if capture.rebuilt_generation == Some(expected.generation.get()) {
                        capture.rebuilt_snapshot_complete |= batch.snapshot_complete;
                        if let Some(actual) =
                            batch.states.iter().find(|state| state.stream == *stream)
                        {
                            capture.rebuilt_stream = Some(ExchangeFaultTarget::Subscription {
                                stream: *stream,
                                pair: *pair,
                                generation: expected.generation,
                            });
                            capture.rebuilt_stream_granted_through =
                                Some(actual.facts.granted_through);
                        }
                    }
                }
                if capture.action == ExchangeFaultAction::CutGrantSubscription
                    && !capture.fired
                    && !capture.released
                    && Instant::now() < capture.deadline
                {
                    let actual = batch.states.iter().find(|state| {
                        capture.selector.matches(state.stream, backend_index)
                            && capture
                                .cut_unit
                                .as_ref()
                                .is_none_or(|unit| unit.target.stream() == state.stream)
                    });
                    if let Some(actual) = actual {
                        let candidate = ExchangeFaultCandidate {
                            target: ExchangeFaultTarget::Subscription {
                                stream: actual.stream,
                                pair: expected.pair,
                                generation: expected.generation,
                            },
                            backend_index,
                            caller,
                            downstream_stream: downstream,
                            request_sha256: request_sha256.clone(),
                        };
                        if capture.candidate.is_none() {
                            capture.candidate = Some(candidate.clone());
                        }
                        if capture.candidate.as_ref() == Some(&candidate) {
                            let owner = add_attempt(capture, candidate, core.clone())?;
                            let millis = dispatched
                                .checked_duration_since(capture.started)
                                .unwrap_or_default()
                                .as_millis()
                                .try_into()
                                .unwrap_or(u64::MAX);
                            capture.attempts[owner.index].upstream_stream = Some(upstream);
                            capture.attempts[owner.index].request_forwarded_at_millis =
                                Some(millis);
                            Some(owner)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        core.changed.notify_waiters();
        if let Some(owner) = selected {
            loop {
                let changed = core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let (armed, released, deadline) = {
                    let state = core.state.lock().expect("root fault state lock");
                    let capture = state
                        .exchange
                        .live_capture()
                        .expect("original grant capture");
                    (capture.armed, capture.released, capture.deadline)
                };
                if released {
                    break;
                }
                if armed {
                    owner.update(|attempt, _| attempt.subscription_cut = true);
                    core.state
                        .lock()
                        .expect("root fault state lock")
                        .exchange
                        .live_capture_mut()
                        .expect("original grant capture")
                        .fired = true;
                    // Reset only the selected H2 stream on both original peers.
                    // The connection and every sibling RPC remain owned/live.
                    sending.send_reset(h2::Reason::CANCEL);
                    output.send_reset(h2::Reason::CANCEL);
                    return Ok(());
                }
                tokio::select! {
                    _=&mut changed=>{},
                    reset=poll_fn(|cx|output.poll_reset(cx))=>{
                        ensure!(reset?==h2::Reason::CANCEL,"Provisional Exchange subscription received an unexpected reset");
                        owner.update(|attempt,_|attempt.peer_cancelled=true);sending.send_reset(h2::Reason::CANCEL);return Ok(())
                    },
                    _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))=>bail!("Exchange subscription capture absolute deadline expired"),
                }
            }
        }
        if !forward_grant_output(core, scope, output, sending, GrantOutput::Message(message))
            .await?
        {
            return Ok(());
        }
    }
    let part = if let Some(trailers) = body
        .trailers()
        .await
        .context("Exchange grant stage=read-trailers")?
    {
        GrantOutput::Trailers(trailers)
    } else {
        GrantOutput::End
    };
    forward_grant_output(core, scope, output, sending, part).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::{ExchangeSource, FragmentNodeId};
    use novarocks_native_trust::{
        DeploymentId, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_types::{AttemptId, QueryId, StageId, TaskId, UniqueId};
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicBool;

    fn stream() -> ExchangeStreamIdentity {
        let query =
            QueryExecutionId::new(QueryId::new(11, 12), AttemptId::new(1).unwrap()).unwrap();
        let source = TaskIdentity::new(
            query,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let destination = TaskIdentity::new(
            query,
            StageId::new(2).unwrap(),
            TaskId::new(2).unwrap(),
            BackendProcessId::new_v7(),
        );
        ExchangeStreamIdentity::try_new(
            ExchangeSource::new(source, UniqueId::new(3, 4), 0),
            destination,
            UniqueId::new(5, 6),
            FragmentNodeId::new(7),
        )
        .unwrap()
    }
    fn peers(
        stream: ExchangeStreamIdentity,
    ) -> Vec<(usize, NativeCallerSubject, BackendProcessId)> {
        vec![
            (
                0,
                NativeCallerSubject::parse("be@127.0.0.1:1111").unwrap(),
                stream.source().task().backend_process_id(),
            ),
            (
                1,
                NativeCallerSubject::parse("be@127.0.0.1:2222").unwrap(),
                stream.destination().backend_process_id(),
            ),
        ]
    }
    fn signed_request(
        path: &str,
        subject: NativeCallerSubject,
        process: NativeProcessIdentity,
    ) -> Result<Request<()>> {
        let trust = NativeTrust::new(
            DeploymentId::parse("root-actor-component")?,
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))?,
            subject,
            NativeTransportMode::Disabled,
        );
        trust.bind_process_identity(process)?;
        let mut metadata = Default::default();
        trust.apply_client_authorization(&mut metadata)?;
        let mut request = Request::builder()
            .method("POST")
            .uri(format!("http://localhost{path}"))
            .header("content-type", "application/grpc")
            .body(())?;
        request.headers_mut().extend(metadata.into_headers());
        Ok(request)
    }
    fn encoded(message: impl Message) -> Bytes {
        let mut body = Vec::with_capacity(message.encoded_len() + 5);
        body.push(0);
        body.extend_from_slice(&(message.encoded_len() as u32).to_be_bytes());
        message.encode(&mut body).unwrap();
        Bytes::from(body)
    }
    fn data(stream: ExchangeStreamIdentity) -> Bytes {
        encoded(codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {stream,sender_count:1,sender_id:0,backend_number:0,
            item:ExchangeUnitItem::Unit{demand:novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitDemand {sequence:ExchangeUnitSequence::FIRST,wire_bytes:4},end_after_unit:false},
            payload:Bytes::from_static(b"data"),next_demand:None}))
    }
    async fn candidate(control: &RootReplyFaultControl) -> Result<ExchangeFaultCandidate> {
        loop {
            let changed = control.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(candidate) = control.exchange_candidate() {
                return Ok(candidate);
            }
            changed.await;
        }
    }
    async fn exchange_rpc(
        sender: client::SendRequest<Bytes>,
        stream: ExchangeStreamIdentity,
        body: Bytes,
    ) -> Result<Bytes> {
        let request = signed_request(
            DELIVER,
            peers(stream)[0].1.clone(),
            NativeProcessIdentity::Backend(stream.source().task().backend_process_id()),
        )?;
        let mut sender = sender.ready().await?;
        let (reply, mut send) = sender.send_request(request, false)?;
        send_bytes(&mut send, body).await?;
        send.send_data(Bytes::new(), true)?;
        let reply = reply.await?;
        let (parts, mut body) = reply.into_parts();
        let core = super::super::tests::core();
        let (bytes, trailers) = unary_body(&mut body, BufferContext::standalone(core)).await?;
        success_status(&parts.headers, trailers.as_ref())?;
        Ok(bytes)
    }

    #[test]
    fn backend_auth_requires_live_subject_process_and_exact_destination() -> Result<()> {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let stream = stream();
        control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
        let request = signed_request(
            DELIVER,
            peers(stream)[0].1.clone(),
            NativeProcessIdentity::Backend(stream.source().task().backend_process_id()),
        )?;
        let caller = authenticate(&core, &request, 1)?;
        assert_eq!(caller, stream.source().task().backend_process_id());
        require_pair(&core, ExchangeProcessPair::for_stream(stream), caller, 1)?;
        assert!(require_pair(&core, ExchangeProcessPair::for_stream(stream), caller, 0).is_err());
        assert!(
            require_pair(
                &core,
                ExchangeProcessPair::for_stream(stream),
                BackendProcessId::new_v7(),
                1
            )
            .is_err()
        );
        for (subject, identity) in [
            (
                peers(stream)[0].1.clone(),
                NativeProcessIdentity::Backend(BackendProcessId::new_v7()),
            ),
            (
                NativeCallerSubject::parse("be@127.0.0.1:3333")?,
                NativeProcessIdentity::Backend(caller),
            ),
            (
                peers(stream)[0].1.clone(),
                NativeProcessIdentity::Frontend(FrontendProcessId::new_v7()),
            ),
        ] {
            assert!(authenticate(&core, &signed_request(DELIVER, subject, identity)?, 1).is_err());
        }
        Ok(())
    }

    #[test]
    fn one_target_slot_preserves_exact_scope_selector_and_join_before_reuse() -> Result<()> {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let stream = stream();
        assert!(
            control
                .begin_exchange_capture(
                    ExchangeFaultAction::HoldDataBeforeForward,
                    Instant::now() + Duration::from_secs(2)
                )
                .is_err()
        );
        control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
        control.begin_exchange_capture_on(
            ExchangeFaultAction::HoldDataBeforeForward,
            ExchangeFaultSelector {
                destination_node: Some(7),
                backend_index: Some(1),
                query_execution_id: Some(stream.destination().query_execution_id()),
            },
            Instant::now() + Duration::from_secs(2),
        )?;
        assert!(
            control
                .begin_capture(Instant::now() + Duration::from_secs(1))
                .is_err()
        );
        let exact = ExchangeFaultCandidate {
            target: ExchangeFaultTarget::Unit {
                stream,
                sequence: ExchangeUnitSequence::FIRST,
            },
            backend_index: 1,
            caller: stream.source().task().backend_process_id(),
            downstream_stream: 1,
            request_sha256: hash(&data(stream)),
        };
        let mut different = exact.clone();
        different.backend_index = 0;
        assert!(select_unit(&core, different)?.is_none());
        let first = select_unit(&core, exact.clone())?.unwrap();
        control.arm_exchange_exact(&exact)?;
        let mut changed = exact.clone();
        changed.request_sha256 = hash(b"different payload");
        assert!(select_unit(&core, changed).is_err());
        let mut changed = exact.clone();
        changed.target = ExchangeFaultTarget::Unit {
            stream,
            sequence: ExchangeUnitSequence::FIRST.checked_next()?,
        };
        let successor = select_unit(&core, changed)?.unwrap();
        assert!(!successor.mutate);
        drop(successor);
        control.release_exchange();
        assert!(
            control
                .begin_exchange_capture(
                    ExchangeFaultAction::DropAcceptedReply,
                    Instant::now() + Duration::from_secs(1)
                )
                .is_err()
        );
        drop(first);
        assert_eq!(control.exchange_observation().active_target_streams, 0);
        control.begin_exchange_capture(
            ExchangeFaultAction::DropAcceptedReply,
            Instant::now() + Duration::from_secs(1),
        )?;
        Ok(())
    }

    #[test]
    fn exchange_mode_root_read_is_non_target_but_original_root_target_stays_protected() -> Result<()>
    {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let stream = stream();
        let caller = FrontendProcessId::new_v7();
        {
            let state = core.state.lock().unwrap();
            assert_eq!(
                classify_frozen_read(&state, stream.destination(), caller),
                StreamClass::Unknown
            );
        }
        control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
        control.begin_exchange_capture(
            ExchangeFaultAction::HoldDataBeforeForward,
            Instant::now() + Duration::from_secs(1),
        )?;
        let mut state = core.state.lock().unwrap();
        assert_eq!(
            classify_frozen_read(&state, stream.destination(), caller),
            StreamClass::NonTarget
        );
        state.frozen_root = Some((stream.destination(), caller));
        state.frozen_backend_index = Some(1);
        assert_eq!(
            classify_frozen_read(&state, stream.destination(), caller),
            StreamClass::Target
        );
        assert!(!harmless_non_target_cancel(
            StreamClass::Target,
            &anyhow::Error::from(h2::Error::from(h2::Reason::CANCEL))
        ));
        Ok(())
    }

    #[test]
    fn growing_message_funds_old_and_new_capacity_and_last_alias() -> Result<()> {
        let core = super::super::tests::core();
        let mut body = OwnedBuffer::new(BufferContext::standalone(core.clone()), 0)?;
        append_growing(&mut body, b"abc")?;
        let first = body.bytes.capacity();
        append_growing(&mut body, b"def")?;
        assert!(core.peak_bytes.load(Ordering::Acquire) >= first + body.bytes.capacity());
        let body = body.into_bytes();
        let alias = body.slice(1..);
        drop(body);
        assert!(core.bytes.load(Ordering::Acquire) > 0);
        drop(alias);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        let mut body = OwnedBuffer::new(BufferContext::standalone(core.clone()), 0)?;
        let credit = ByteCredit::new(
            BufferContext::standalone(core.clone()),
            core.bounds.maximum_owned_buffer_bytes,
        )?;
        assert!(append_growing(&mut body, b"x").is_err());
        drop(credit);
        drop(body);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }

    async fn grant_rpc(
        sender: client::SendRequest<Bytes>,
        stream: ExchangeStreamIdentity,
        generation: u64,
    ) -> Result<Bytes> {
        let request = signed_request(
            GRANTS,
            peers(stream)[0].1.clone(),
            NativeProcessIdentity::Backend(stream.source().task().backend_process_id()),
        )?;
        let mut sender = sender.ready().await?;
        let (reply, mut send) = sender.send_request(request, false)?;
        let body = encoded(codec::encode_grant_subscription(
            DecodedExchangeGrantSubscription {
                pair: ExchangeProcessPair::for_stream(stream),
                generation: NonZeroU64::new(generation).unwrap(),
            },
        ));
        send_bytes(&mut send, body).await?;
        send.send_data(Bytes::new(), true)?;
        let reply = reply.await?;
        let (parts, mut body) = reply.into_parts();
        let core = super::super::tests::core();
        let (body, trailers) = unary_body(&mut body, BufferContext::standalone(core)).await?;
        success_status(&parts.headers, trailers.as_ref())?;
        Ok(body)
    }
    #[derive(Default)]
    struct WireState {
        seen: Mutex<BTreeSet<u64>>,
        received: AtomicUsize,
        applied: Notify,
        root_requests: AtomicUsize,
        root_arrived: Notify,
        root_release: Notify,
        pause_unit_reply: AtomicBool,
        unit_reply_release: Notify,
        partial_snapshot: AtomicBool,
    }
    async fn reply_message(mut reply: server::SendResponse<Bytes>, body: Bytes) -> Result<()> {
        let mut output = reply.send_response(Response::new(()), false)?;
        send_bytes(&mut output, body).await?;
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        output.send_trailers(trailers)?;
        Ok(())
    }
    // Actual H2 peers qualify actor routing, bytes and ownership only. Native
    // receiver-kernel and distributed SQL remain separate required evidence.
    async fn wire_service(
        listener: tokio::net::TcpListener,
        stream: ExchangeStreamIdentity,
        action: ExchangeFaultAction,
        state: Arc<WireState>,
        core: Arc<Core>,
    ) -> Result<()> {
        let (io, _) = listener.accept().await?;
        let mut connection = server::handshake::<_>(io).await?;
        let mut handlers = JoinSet::new();
        loop {
            tokio::select! {
                request=connection.accept()=>{
                    let Some(request)=request else{break};let (request,reply)=request?;
                    let state=state.clone();let core=core.clone();
                    handlers.spawn(async move {
                        let path=request.uri().path().to_owned();let (_,mut body)=request.into_parts();let (body,_)=unary_body(&mut body,BufferContext::standalone(core)).await?;
                        match path.as_str() {
                            DELIVER=>{
                                let decoded=codec::decode_delivery_request(wire::DeliverExchangeUnitRequest::decode(single_message(&body)?)?,FieldPath::root("real_h2_test"))?;
                                assert_eq!(decoded.stream,stream);let sequence=decoded.item.sequence();
                                let applied=state.seen.lock().unwrap().insert(sequence.get());state.received.fetch_add(1,Ordering::AcqRel);state.applied.notify_waiters();
                                if state.pause_unit_reply.load(Ordering::Acquire) {state.unit_reply_release.notified().await;}
                                reply_message(reply,encoded(codec::encode_delivery_response(stream,sequence,DecodedExchangeDeliveryOutcome::Observed {
                                    verdict:if applied {ExchangeAdmissionVerdict::Applied}else{ExchangeAdmissionVerdict::Idempotent},
                                    facts:novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts{custody_through:sequence.get(),..Default::default()}}))).await?;
                            },
                            GRANTS=>{
                                let expected=codec::decode_grant_subscription(&wire::SubscribeExchangeGrantsRequest::decode(single_message(&body)?)?,FieldPath::root("real_h2_subscription"))?;
                                if action==ExchangeFaultAction::DropAcceptedReply {
                                    loop {let changed=state.applied.notified();tokio::pin!(changed);changed.as_mut().enable();if !state.seen.lock().unwrap().is_empty(){break}changed.await;}
                                }
                                let custody=u64::from(state.seen.lock().unwrap().contains(&1));
                                let batch=codec::DecodedExchangeGrantBatch{generation:expected.generation,snapshot:true,snapshot_complete:!state.partial_snapshot.load(Ordering::Acquire),states:vec![codec::DecodedExchangeStreamState{stream,
                                    facts:novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts{custody_through:custody,granted_through:1,..Default::default()}}]};
                                reply_message(reply,encoded(codec::encode_grant_batch(&batch,expected.pair,FieldPath::root("real_h2_grant_batch"))?)).await?;
                            },
                            ROOT_PATH=>{
                                let request=decode_read(&wire::FetchRootResultRequest::decode(single_message(&body)?)?,FieldPath::root("real_h2_root_read"))?;
                                state.root_requests.fetch_add(1,Ordering::AcqRel);state.root_arrived.notify_waiters();state.root_release.notified().await;
                                let reply_body=novarocks_task_codec::root_result::encode_reply(&novarocks_execution_contract::root_result::RootResultReply {
                                    root_task:request.root_task(),profile:RootProfileId::V1,kind:RootOutputKind::ClientRows,accepted_consumed:0,outcome:RootReadOutcome::NotReady})?;
                                reply_message(reply,encoded(reply_body)).await?;
                            },
                            _=>bail!("Unexpected real H2 wire service method"),
                        }
                        Ok::<_,anyhow::Error>(())
                    });
                },
                joined=handlers.join_next(),if !handlers.is_empty()=>{joined.context("H2 service child absent")???;},
            }
        }
        while let Some(joined) = handlers.join_next().await {
            joined??;
        }
        Ok(())
    }
    async fn unit_case(action: ExchangeFaultAction) -> Result<()> {
        unit_case_with_obligation_probe(action, false).await
    }

    async fn probe_original_fault_grant_cancel(
        control: &RootReplyFaultControl,
        stream: ExchangeStreamIdentity,
        generation: u64,
        recover: bool,
    ) -> Result<()> {
        grant_reset_wire(
            control.core.clone(),
            stream,
            DecodedExchangeGrantSubscription {
                pair: ExchangeProcessPair::for_stream(stream),
                generation: NonZeroU64::new(generation).unwrap(),
            },
            GrantHeaderCase::HeldUnit,
            h2::Reason::CANCEL,
            GrantResetPhase::Body(GrantBodyPart::Message),
            GrantResetExpectation {
                recover,
                original_actors_live: true,
            },
        )
        .await
    }

    async fn wait_original_fault_callbacks(control: &RootReplyFaultControl) {
        loop {
            let changed = control.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let observed = control.exchange_observation();
            if observed.active_target_streams == 0
                && observed.active_custody_batches_held == 0
                && observed
                    .attempts
                    .iter()
                    .all(|attempt| attempt.stream_future_exited_at_millis.is_some())
            {
                return;
            }
            changed.await;
        }
    }

    // Additional pure H2 scopes share this original Core only in this explicit
    // mixed component mode. They grant no Native/32 or zero-standalone result.
    async fn unit_case_with_obligation_probe(
        action: ExchangeFaultAction,
        probe: bool,
    ) -> Result<()> {
        let stream = stream();
        let fixture_core = super::super::tests::core();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let upstream = listener.local_addr()?;
        let state = Arc::new(WireState::default());
        let service = tokio::spawn(wire_service(
            listener,
            stream,
            action,
            state.clone(),
            fixture_core.clone(),
        ));
        let mut proxy = RootReplyFaultProxy::start(
            upstream,
            1,
            None,
            RootReplyFaultBounds::default(),
            fixture_core.verifier.clone(),
            fixture_core.normal_subject.clone(),
        )?;
        let control = proxy.control();
        control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
        control.begin_exchange_capture(action, Instant::now() + Duration::from_secs(4))?;
        let io = tokio::net::TcpStream::connect(proxy.address()).await?;
        let (sender, driver) = client::handshake(io).await?;
        let driver = tokio::spawn(driver);
        let custody = if action == ExchangeFaultAction::DropAcceptedReply {
            Some(tokio::spawn(grant_rpc(sender.clone(), stream, 1)))
        } else {
            None
        };
        let first = if action == ExchangeFaultAction::CutGrantSubscription {
            tokio::spawn(grant_rpc(sender.clone(), stream, 1))
        } else {
            tokio::spawn(exchange_rpc(sender.clone(), stream, data(stream)))
        };
        let captured = candidate(&control).await?;
        assert_eq!(captured.target.stream(), stream);
        assert_eq!(captured.caller, stream.source().task().backend_process_id());
        control.arm_exchange_exact(&captured)?;
        if action == ExchangeFaultAction::DropAcceptedReply {
            let failure = first.await?.expect_err("accepted response must be lost");
            assert!(
                failure
                    .downcast_ref::<h2::Error>()
                    .is_some_and(|error| error.is_reset()
                        && error.is_remote()
                        && error.reason() == Some(h2::Reason::CANCEL))
            );
            assert_eq!(state.received.load(Ordering::Acquire), 1);
            loop {
                let changed = control.core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if control.exchange_observation().custody_batches_held > 0 {
                    break;
                }
                changed.await;
            }
            let custody = custody.unwrap();
            assert!(!custody.is_finished());
            if probe {
                assert!(control.exchange_observation().active_custody_batches_held > 0);
                probe_original_fault_grant_cancel(&control, stream, 1, false).await?;
                state.pause_unit_reply.store(true, Ordering::Release);
            }
            let reply = if probe {
                let replay = tokio::spawn(exchange_rpc(sender.clone(), stream, data(stream)));
                loop {
                    let changed = state.applied.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if state.received.load(Ordering::Acquire) == 2 {
                        break;
                    }
                    changed.await;
                }
                assert!(control.exchange_observation().active_target_streams > 0);
                probe_original_fault_grant_cancel(&control, stream, 1, false).await?;
                state.pause_unit_reply.store(false, Ordering::Release);
                state.unit_reply_release.notify_one();
                replay.await??
            } else {
                exchange_rpc(sender.clone(), stream, data(stream)).await?
            };
            let reply = wire::DeliverExchangeUnitResponse::decode(single_message(&reply)?)?;
            assert!(matches!(
                codec::decode_delivery_response(
                    &reply,
                    stream,
                    ExchangeUnitSequence::FIRST,
                    FieldPath::root("real_h2_replay")
                )?,
                DecodedExchangeDeliveryOutcome::Observed {
                    verdict: ExchangeAdmissionVerdict::Idempotent,
                    ..
                }
            ));
            let custody = custody.await??;
            let custody = codec::decode_grant_batch(
                &wire::ExchangeGrantBatch::decode(single_message(&custody)?)?,
                DecodedExchangeGrantSubscription {
                    pair: ExchangeProcessPair::for_stream(stream),
                    generation: NonZeroU64::new(1).unwrap(),
                },
                FieldPath::root("real_h2_custody"),
            )?;
            assert_eq!(custody.states[0].stream, stream);
            assert_eq!(custody.states[0].facts.custody_through, 1);
            let observed = control.exchange_observation();
            assert_eq!(observed.replies_dropped, 1);
            assert_eq!(observed.admissions_observed, 2);
            assert_eq!(observed.replay_idempotent, 1);
            assert!(observed.custody_released_after_replay);
            assert_eq!(observed.active_custody_batches_held, 0);
            assert_eq!(state.received.load(Ordering::Acquire), 2);
            assert_eq!(state.seen.lock().unwrap().len(), 1);
            if probe {
                wait_original_fault_callbacks(&control).await;
                assert!(!control.exchange_observation().released);
                probe_original_fault_grant_cancel(&control, stream, 1, true).await?;
            }
        } else if action == ExchangeFaultAction::CutGrantSubscription {
            let failure = first
                .await?
                .expect_err("selected grant subscription must be cut");
            assert!(
                failure
                    .downcast_ref::<h2::Error>()
                    .is_some_and(|error| error.is_reset()
                        && error.is_remote()
                        && error.reason() == Some(h2::Reason::CANCEL))
            );
            let unit = tokio::spawn(exchange_rpc(sender.clone(), stream, data(stream)));
            loop {
                let changed = control.core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if control.exchange_observation().data_forward_held > 0 {
                    break;
                }
                changed.await;
            }
            assert_eq!(state.received.load(Ordering::Acquire), 0);
            assert!(!unit.is_finished());
            if probe {
                probe_original_fault_grant_cancel(&control, stream, 2, false).await?;
                state.partial_snapshot.store(true, Ordering::Release);
                grant_rpc(sender.clone(), stream, 2).await?;
                assert_eq!(control.exchange_observation().rebuilt_generation, Some(2));
                assert!(!control.exchange_observation().rebuilt_snapshot_complete);
                assert!(!unit.is_finished());
                probe_original_fault_grant_cancel(&control, stream, 2, false).await?;
                state.partial_snapshot.store(false, Ordering::Release);
                state.pause_unit_reply.store(true, Ordering::Release);
            }
            grant_rpc(sender.clone(), stream, 2).await?;
            if probe {
                loop {
                    let changed = state.applied.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if state.received.load(Ordering::Acquire) == 1 {
                        break;
                    }
                    changed.await;
                }
                assert!(control.exchange_observation().rebuilt_snapshot_complete);
                assert!(control.exchange_observation().active_target_streams > 0);
                probe_original_fault_grant_cancel(&control, stream, 2, false).await?;
                state.pause_unit_reply.store(false, Ordering::Release);
                state.unit_reply_release.notify_one();
            }
            unit.await??;
            let observed = control.exchange_observation();
            assert_eq!(observed.subscriptions_cut, 1);
            assert_eq!(observed.rebuilt_generation, Some(2));
            assert!(observed.rebuilt_snapshot_complete);
            assert_eq!(observed.rebuilt_stream.unwrap().stream(), stream);
            assert_eq!(observed.rebuilt_stream_granted_through, Some(1));
            assert_eq!(state.received.load(Ordering::Acquire), 1);
            if probe {
                wait_original_fault_callbacks(&control).await;
                assert!(!control.exchange_observation().released);
                probe_original_fault_grant_cancel(&control, stream, 1, false).await?;
                probe_original_fault_grant_cancel(&control, stream, 3, false).await?;
                probe_original_fault_grant_cancel(&control, stream, 2, true).await?;
            }
        } else {
            assert_eq!(state.received.load(Ordering::Acquire), 0);
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            let next = tokio::spawn(exchange_rpc(sender.clone(), stream, data(stream)));
            loop {
                let changed = control.core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if control.exchange_observation().attempts.len() == 2 {
                    break;
                }
                changed.await;
            }
            assert_eq!(state.received.load(Ordering::Acquire), 0);
            assert!(!next.is_finished());
            assert_eq!(control.exchange_observation().requests_forwarded, 0);
            control.release_exchange();
            next.await??;
            let observed = control.exchange_observation();
            assert_eq!(observed.data_forward_held, 2);
            assert_eq!(observed.requests_forwarded, 1);
            assert_eq!(observed.admissions_observed, 1);
            assert_eq!(state.received.load(Ordering::Acquire), 1);
        }
        // An ordinary, fully authenticated RootFetch remains non-target while
        // the Exchange rule is selected. Cancel its real downstream H2 stream.
        let read = RootResultRead::try_new(
            stream.destination(),
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(100),
        )?;
        let root = signed_request(
            ROOT_PATH,
            fixture_core.normal_subject.clone(),
            NativeProcessIdentity::Frontend(FrontendProcessId::new_v7()),
        )?;
        let mut root_sender = sender.clone().ready().await?;
        let (root_reply, mut root_send) = root_sender.send_request(root, false)?;
        send_bytes(
            &mut root_send,
            encoded(novarocks_task_codec::root_result::encode_read(&read)),
        )
        .await?;
        root_send.send_data(Bytes::new(), true)?;
        loop {
            let changed = state.root_arrived.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if state.root_requests.load(Ordering::Acquire) == 1 {
                break;
            }
            changed.await;
        }
        drop(root_reply);
        drop(root_send);
        drop(root_sender);
        state.root_release.notify_one();
        loop {
            let changed = control.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let actor = control.observation();
            ensure!(
                actor.failures.is_empty(),
                "Authenticated non-target Root cancellation failed: {:?}",
                actor.failures
            );
            if actor.non_target_peer_cancels > 0 {
                break;
            }
            changed.await;
        }
        drop(sender);
        driver.await??;
        proxy.stop();
        service.await??;
        let observed = control.exchange_observation();
        assert_eq!(observed.active_target_streams, 0);
        assert!(
            observed
                .attempts
                .iter()
                .all(|attempt| attempt.stream_future_exited_at_millis.is_some())
        );
        let before_retirement = control.exchange_observation();
        assert_eq!(control.observation().target_slots, 1);
        RootReplyFaultProxy::retire_joined_group(std::iter::once(&proxy))?;
        assert_eq!(control.observation().target_slots, 0);
        let retained = control.exchange_observation();
        assert_eq!(retained.candidate, before_retirement.candidate);
        assert_eq!(
            retained.replay_idempotent,
            before_retirement.replay_idempotent
        );
        assert_eq!(
            retained.subscriptions_cut,
            before_retirement.subscriptions_cut
        );
        assert_eq!(retained.attempts.len(), before_retirement.attempts.len());
        RootReplyFaultProxy::retire_joined_group(std::iter::once(&proxy))?;
        let actors = control.observation();
        assert_eq!(actors.active_listeners, 0);
        assert_eq!(actors.active_connections, 0);
        assert_eq!(actors.active_streams, 0);
        assert!(actors.non_target_peer_cancels > 0);
        assert_eq!(actors.owned_buffer_bytes, 0);
        assert!(actors.connection_bound_buffer_charges > 0);
        assert_eq!(
            actors.connection_bound_buffer_charges,
            actors.connection_bound_buffer_drops
        );
        if probe {
            assert!(
                actors.standalone_buffer_charges > 0,
                "mixed pure H2 probes explicitly report their standalone allocations"
            );
        } else {
            assert_eq!(
                actors.standalone_buffer_charges, 0,
                "every real Exchange/Root allocation stays bound to its original connection"
            );
        }
        assert!(actors.shutdown_joined);
        assert!(actors.failures.is_empty(), "{:?}", actors.failures);
        assert_eq!(fixture_core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }
    #[tokio::test]
    async fn bound_unarmed_grant_subscription_cancels_without_losing_its_h2_connection()
    -> Result<()> {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let stream = stream();
        // Binding precedes all seed traffic. There is deliberately no capture,
        // candidate, target or mutation during this ordinary subscription.
        control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
        let expected = DecodedExchangeGrantSubscription {
            pair: ExchangeProcessPair::for_stream(stream),
            generation: NonZeroU64::new(7).unwrap(),
        };
        let mut drivers = JoinSet::new();
        let result=tokio::time::timeout(Duration::from_secs(3),async {
            let (up_client,up_server)=tokio::io::duplex(65536);let upstream_core=core.clone();
            let (up_reset_tx,up_reset_rx)=tokio::sync::oneshot::channel();
            drivers.spawn(async move {
                let mut connection=server::handshake::<_>(up_server).await?;
                let (request,mut response)=connection.accept().await.context("ordinary grant upstream request absent")??;
                let work=async {
                    assert_eq!(request.uri().path(),GRANTS);
                    let caller=authenticate(&upstream_core,&request,1)?;assert_eq!(caller,expected.pair.sender);
                    let mut inbound=request.into_body();let (body,trailers)=unary_body(&mut inbound,BufferContext::standalone(upstream_core)).await?;assert!(trailers.is_none());
                    let decoded=codec::decode_grant_subscription(&wire::SubscribeExchangeGrantsRequest::decode(single_message(&body)?)?,FieldPath::root("ordinary_unarmed_subscription"))?;
                    assert_eq!(decoded,expected);drop(body);
                    let batch=codec::DecodedExchangeGrantBatch {generation:expected.generation,snapshot:true,snapshot_complete:true,
                        states:vec![codec::DecodedExchangeStreamState {stream,facts:novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts {granted_through:1,..Default::default()}}]};
                    let body=encoded(codec::encode_grant_batch(&batch,expected.pair,FieldPath::root("ordinary_unarmed_snapshot"))?);
                    let mut output=response.send_response(Response::new(()),false)?;output.send_data(body,false)?;
                    let reset=poll_fn(|cx|output.poll_reset(cx)).await?;let _=up_reset_tx.send(reset);
                    std::future::pending::<Result<()>>().await
                };
                tokio::select! {result=work=>result,_=async {while connection.accept().await.is_some(){}}=>Ok(())}
            });
            let (up_sender,up_driver)=client::handshake(up_client).await?;
            // Keep the connection alive until its original server observes the
            // actor's actual upstream reset; dropping a driver is not evidence.
            let up_keep_alive=up_sender.clone();drivers.spawn(async move {up_driver.await.context("ordinary grant upstream driver")});
            let (down_client,down_server)=tokio::io::duplex(65536);let downstream_core=core.clone();
            let (outcome_tx,outcome_rx)=tokio::sync::oneshot::channel();
            drivers.spawn(async move {
                let mut connection=server::handshake::<_>(down_server).await?;
                let (request,response)=connection.accept().await.context("ordinary grant downstream request absent")??;
                assert!(enabled_method(&downstream_core,&request));
                assert!(authenticated_normal_frontend(&request,&downstream_core).is_none());
                let forwarding=async {
                    let outcome=forward_stream(request,response,up_sender,1,downstream_core).await;
                    let _=outcome_tx.send(outcome);
                    std::future::pending::<()>().await
                };
                let healthy=async {
                    let (request,mut response)=connection.accept().await.context("ordinary grant healthy next stream absent")??;
                    assert_eq!(request.uri().path(),"/healthy-next");response.send_response(Response::new(()),true)?;
                    while connection.accept().await.is_some(){}Ok::<_,anyhow::Error>(())
                };
                tokio::join!(forwarding,healthy).1
            });
            let mut builder=client::Builder::new();builder.initial_window_size(0);
            let (mut sender,driver)=builder.handshake::<_,Bytes>(down_client).await?;
            drivers.spawn(async move {driver.await.context("ordinary grant downstream driver")});
            let request=signed_request(GRANTS,peers(stream)[0].1.clone(),NativeProcessIdentity::Backend(expected.pair.sender))?;
            let (reply,mut send)=sender.send_request(request,false)?;
            send.send_data(encoded(codec::encode_grant_subscription(expected)),true)?;
            let reply=reply.await?;assert_eq!(reply.status(),http::StatusCode::OK);send.send_reset(h2::Reason::CANCEL);
            outcome_rx.await??;assert_eq!(up_reset_rx.await?,h2::Reason::CANCEL);
            assert!(control.exchange_candidate().is_none());let observed=control.observation();assert_eq!(observed.target_slots,0);assert_eq!(observed.claimed,0);assert!(observed.failures.is_empty());
            drop(reply);drop(send);sender=sender.ready().await?;
            let (next,next_send)=sender.send_request(Request::builder().uri("http://localhost/healthy-next").body(())?,true)?;
            assert_eq!(next.await?.status(),http::StatusCode::OK);drop(next_send);drop(sender);drop(up_keep_alive);Ok::<_,anyhow::Error>(())
        }).await;
        drivers.abort_all();
        let mut cleanup_error = None;
        while let Some(joined) = drivers.join_next().await {
            let failure = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if error.is_cancelled() => None,
                Err(error) => Some(error.into()),
            };
            if cleanup_error.is_none() {
                cleanup_error = failure;
            }
        }
        result.context("ordinary unarmed grant cancellation deadline")??;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }

    fn retirement_proxy(
        index: usize,
        shared: Option<&RootReplyFaultProxy>,
    ) -> Result<RootReplyFaultProxy> {
        let fixture = super::super::tests::core();
        RootReplyFaultProxy::start(
            "127.0.0.1:9".parse()?,
            index,
            shared,
            RootReplyFaultBounds::default(),
            fixture.verifier.clone(),
            fixture.normal_subject.clone(),
        )
    }

    fn retirement_capture(proxy: &RootReplyFaultProxy) -> Result<RootReplyFaultControl> {
        let control = proxy.control();
        control.bind_exchange_peers(peers(stream()), Instant::now() + Duration::from_secs(5))?;
        control.begin_exchange_capture(
            ExchangeFaultAction::HoldDataBeforeForward,
            Instant::now() + Duration::from_secs(2),
        )?;
        Ok(control)
    }

    #[test]
    fn capture_retirement_requires_every_original_registered_join_and_exact_core() -> Result<()> {
        let mut first = retirement_proxy(0, None)?;
        let mut second = retirement_proxy(1, Some(&first))?;
        let mut third = retirement_proxy(2, Some(&first))?;
        let control = retirement_capture(&first)?;
        assert!(RootReplyFaultProxy::retire_joined_group([&first, &second, &third]).is_err());
        assert_eq!(control.observation().target_slots, 1);
        first.stop();
        second.stop();
        third.stop();
        assert!(
            RootReplyFaultProxy::retire_joined_group([&first, &second]).is_err(),
            "a subset cannot replace the original registered listener group"
        );
        let mut foreign = retirement_proxy(2, None)?;
        foreign.stop();
        assert!(RootReplyFaultProxy::retire_joined_group([&first, &second, &foreign]).is_err());
        assert!(RootReplyFaultProxy::retire_joined_group([&first, &second, &first]).is_err());
        assert_eq!(control.observation().target_slots, 1);
        RootReplyFaultProxy::retire_joined_group([&first, &second, &third])?;
        assert_eq!(control.observation().target_slots, 0);
        assert_eq!(control.observation().joined_listeners, 3);
        assert!(!control.exchange_observation().armed);
        RootReplyFaultProxy::retire_joined_group([&first, &second, &third])?;
        assert_eq!(
            control.exchange_observation().action,
            Some(ExchangeFaultAction::HoldDataBeforeForward)
        );
        Ok(())
    }

    #[test]
    fn stopped_group_rejects_shared_listener_registration_and_preserves_original_proof()
    -> Result<()> {
        let mut proxy = retirement_proxy(1, None)?;
        let control = retirement_capture(&proxy)?;
        proxy.stop();
        let joined = RootReplyFaultProxy::joined_group([&proxy])?;
        let original_roster = control.observation().preapplication_backend_indices;
        assert!(retirement_proxy(2, Some(&proxy)).is_err());
        assert_eq!(
            control.observation().preapplication_backend_indices,
            original_roster
        );
        assert_eq!(control.observation().active_listeners, 0);
        assert_eq!(control.observation().joined_listeners, 1);
        joined.retire_exchange_capture()?;
        assert_eq!(control.observation().target_slots, 0);
        assert!(control.observation().failures.is_empty());
        Ok(())
    }

    #[test]
    fn original_sticky_stop_rejects_shared_registration_before_scope_close() -> Result<()> {
        let mut proxy = retirement_proxy(1, None)?;
        let control = retirement_capture(&proxy)?;
        let original_roster = control.observation().preapplication_backend_indices;
        assert!(!control.core.state.lock().unwrap().lifecycle_closed);
        control.core.stop.send_replace(true);
        assert!(retirement_proxy(2, Some(&proxy)).is_err());
        assert_eq!(
            control.observation().preapplication_backend_indices,
            original_roster
        );
        proxy.stop();
        RootReplyFaultProxy::retire_joined_group([&proxy])?;
        assert_eq!(control.observation().joined_listeners, 1);
        assert_eq!(control.observation().target_slots, 0);
        Ok(())
    }

    #[test]
    fn original_join_fact_can_recheck_only_after_the_last_real_byte_alias_exits() -> Result<()> {
        let mut proxy = retirement_proxy(1, None)?;
        let control = retirement_capture(&proxy)?;
        let body =
            OwnedBuffer::new(BufferContext::standalone(control.core.clone()), 8)?.into_bytes();
        let alias = body.clone();
        drop(body);
        control.release_exchange();
        proxy.stop();
        assert_eq!(control.observation().active_connections, 0);
        assert_eq!(control.observation().owned_buffer_bytes, 8);
        assert!(RootReplyFaultProxy::retire_joined_group([&proxy]).is_err());
        assert_eq!(control.observation().target_slots, 1);
        drop(alias);
        RootReplyFaultProxy::retire_joined_group([&proxy])?;
        assert_eq!(control.observation().owned_buffer_bytes, 0);
        assert_eq!(control.observation().target_slots, 0);
        assert_eq!(
            control.observation().joined_listeners,
            1,
            "rechecking the same actual join must not invent another join"
        );
        Ok(())
    }

    #[test]
    fn released_capture_cannot_retire_live_target_or_custody_owners() -> Result<()> {
        let mut proxy = retirement_proxy(1, None)?;
        let control = retirement_capture(&proxy)?;
        let stream = stream();
        let candidate = ExchangeFaultCandidate {
            target: ExchangeFaultTarget::Unit {
                stream,
                sequence: ExchangeUnitSequence::FIRST,
            },
            backend_index: 1,
            caller: stream.source().task().backend_process_id(),
            downstream_stream: 1,
            request_sha256: hash(&data(stream)),
        };
        let target = select_unit(&control.core, candidate)?.context("original target absent")?;
        control
            .core
            .state
            .lock()
            .unwrap()
            .exchange
            .live_capture_mut()
            .unwrap()
            .active_custody_batches_held = 1;
        let custody = CustodyBatchOwner {
            core: control.core.clone(),
        };
        control.release_exchange();
        proxy.stop();
        assert!(RootReplyFaultProxy::retire_joined_group([&proxy]).is_err());
        drop(target);
        assert!(RootReplyFaultProxy::retire_joined_group([&proxy]).is_err());
        assert_eq!(control.observation().target_slots, 1);
        drop(custody);
        let pointer = control
            .core
            .state
            .lock()
            .unwrap()
            .exchange
            .live_capture()
            .unwrap()
            .attempts
            .as_ptr();
        RootReplyFaultProxy::retire_joined_group([&proxy])?;
        {
            let state = control.core.state.lock().unwrap();
            assert!(state.exchange.live_capture().is_none());
            assert_eq!(
                state.exchange.receipt().unwrap().attempts.as_ptr(),
                pointer,
                "retirement moves the same bounded attempt allocation"
            );
        }
        assert_eq!(control.observation().target_slots, 0);
        assert_eq!(control.exchange_observation().active_target_streams, 0);
        assert_eq!(
            control.exchange_observation().active_custody_batches_held,
            0
        );
        assert!(
            control
                .begin_exchange_capture(
                    ExchangeFaultAction::DropAcceptedReply,
                    Instant::now() + Duration::from_secs(1)
                )
                .is_err()
        );
        let elapsed = control.exchange_observation().elapsed_millis;
        control.release_exchange();
        assert_eq!(control.exchange_observation().elapsed_millis, elapsed);
        Ok(())
    }

    #[test]
    fn retirement_preserves_unfired_failure_overflow_and_closed_scope_facts() -> Result<()> {
        let mut proxy = retirement_proxy(1, None)?;
        let control = retirement_capture(&proxy)?;
        control.core.failure("original actor fixture failure");
        control.core.failure(&"x".repeat(FAILURE_BYTES + 1));
        proxy.stop();
        let before = control.observation();
        RootReplyFaultProxy::retire_joined_group([&proxy])?;
        let after = control.observation();
        assert_eq!(after.target_slots, 0);
        assert_eq!(after.failures, before.failures);
        assert!(after.failure_overflow);
        assert_eq!(after.overflow_failures, before.overflow_failures);
        assert_eq!(
            after.first_failure.as_ref().unwrap().sha256,
            before.first_failure.as_ref().unwrap().sha256
        );
        let state = control.core.state.lock().unwrap();
        assert!(state.lifecycle_closed && state.used_capture);
        assert!(!state.exchange.receipt().unwrap().fired);
        assert!(!state.exchange.receipt().unwrap().released);
        Ok(())
    }

    #[test]
    fn missing_original_join_handle_cannot_mint_retirement_from_zero_counters() -> Result<()> {
        let mut proxy = retirement_proxy(1, None)?;
        let control = retirement_capture(&proxy)?;
        let original = proxy
            .thread
            .take()
            .context("original listener handle absent")?;
        proxy.stop();
        original.join().expect("original listener fixture panicked");
        assert_eq!(control.observation().active_listeners, 0);
        assert_eq!(control.observation().active_connections, 0);
        assert!(RootReplyFaultProxy::retire_joined_group([&proxy]).is_err());
        assert_eq!(control.observation().target_slots, 1);
        Ok(())
    }

    #[test]
    fn failed_original_thread_join_never_mints_a_group_capability() -> Result<()> {
        let core = super::super::tests::core();
        core.state
            .lock()
            .unwrap()
            .preapplication_closes
            .register(1)?;
        let control = RootReplyFaultControl { core: core.clone() };
        control.bind_exchange_peers(peers(stream()), Instant::now() + Duration::from_secs(5))?;
        control.begin_exchange_capture(
            ExchangeFaultAction::HoldDataBeforeForward,
            Instant::now() + Duration::from_secs(2),
        )?;
        // A real owned thread and real JoinError, without fabricating a success
        // record from the otherwise empty transport counters.
        let thread: JoinHandle<()> =
            std::thread::spawn(|| panic!("original actor join failure fixture"));
        let mut proxy = RootReplyFaultProxy {
            address: "127.0.0.1:9".parse()?,
            generation: watch::channel(0).0,
            core,
            thread: Some(thread),
            backend_index: 1,
            original_join: None,
        };
        proxy.stop();
        assert!(proxy.original_join.is_none());
        assert!(RootReplyFaultProxy::retire_joined_group([&proxy]).is_err());
        assert_eq!(control.observation().target_slots, 1);
        assert!(!control.observation().failures.is_empty());
        Ok(())
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum GrantHeaderCase {
        HeldUnit,
        HeldReply,
        Custody,
        SelectedSubscription,
        Rebuild,
        UnknownSubscription,
        ReclassifiedSubscription,
        ForeignScope,
        ExpiredScope,
    }

    #[derive(Clone, Copy, Debug)]
    enum GrantBodyPart {
        Message,
        Trailers,
        End,
    }

    #[derive(Clone, Copy, Debug)]
    enum GrantResetPhase {
        Headers(bool),
        Body(GrantBodyPart),
    }

    async fn grant_pre_headers_reset_case(
        case: GrantHeaderCase,
        reason: h2::Reason,
        after_failed_headers: bool,
    ) -> Result<()> {
        grant_reset_case(case, reason, GrantResetPhase::Headers(after_failed_headers)).await
    }

    async fn grant_reset_case(
        case: GrantHeaderCase,
        reason: h2::Reason,
        phase: GrantResetPhase,
    ) -> Result<()> {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let stream = stream();
        control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
        let action = match case {
            GrantHeaderCase::HeldReply => ExchangeFaultAction::HoldReplyAfterAdmission,
            GrantHeaderCase::Custody => ExchangeFaultAction::DropAcceptedReply,
            GrantHeaderCase::SelectedSubscription
            | GrantHeaderCase::Rebuild
            | GrantHeaderCase::UnknownSubscription => ExchangeFaultAction::CutGrantSubscription,
            _ => ExchangeFaultAction::HoldDataBeforeForward,
        };
        control.begin_exchange_capture(action, Instant::now() + Duration::from_secs(2))?;
        let expected = DecodedExchangeGrantSubscription {
            pair: ExchangeProcessPair::for_stream(stream),
            generation: NonZeroU64::new(if case == GrantHeaderCase::Rebuild {
                8
            } else {
                7
            })
            .unwrap(),
        };
        // This fixture controls the actor's target register. The request,
        // authentication, unary decoder, both H2 peers and reset remain real;
        // this is not qualification of a Native receiver's custody semantics.
        if case != GrantHeaderCase::UnknownSubscription {
            let candidate = ExchangeFaultCandidate {
                target: if matches!(
                    case,
                    GrantHeaderCase::SelectedSubscription | GrantHeaderCase::Rebuild
                ) {
                    ExchangeFaultTarget::Subscription {
                        stream,
                        pair: expected.pair,
                        generation: NonZeroU64::new(7).unwrap(),
                    }
                } else {
                    ExchangeFaultTarget::Unit {
                        stream,
                        sequence: ExchangeUnitSequence::FIRST,
                    }
                },
                backend_index: 1,
                caller: expected.pair.sender,
                downstream_stream: 1,
                request_sha256: if matches!(
                    case,
                    GrantHeaderCase::SelectedSubscription | GrantHeaderCase::Rebuild
                ) {
                    hash(&encoded(codec::encode_grant_subscription(
                        DecodedExchangeGrantSubscription {
                            pair: expected.pair,
                            generation: NonZeroU64::new(7).unwrap(),
                        },
                    )))
                } else {
                    hash(&data(stream))
                },
            };
            core.state
                .lock()
                .unwrap()
                .exchange
                .live_capture_mut()
                .unwrap()
                .candidate = Some(candidate.clone());
            control.arm_exchange_exact(&candidate)?;
            assert!(control.exchange_observation().armed);
        }
        let should_recover = matches!(case, GrantHeaderCase::HeldUnit | GrantHeaderCase::HeldReply)
            && reason == h2::Reason::CANCEL;
        grant_reset_wire(
            core,
            stream,
            expected,
            case,
            reason,
            phase,
            GrantResetExpectation {
                recover: should_recover,
                original_actors_live: false,
            },
        )
        .await
    }

    struct GrantResetExpectation {
        recover: bool,
        original_actors_live: bool,
    }

    async fn grant_reset_wire(
        core: Arc<Core>,
        stream: ExchangeStreamIdentity,
        expected: DecodedExchangeGrantSubscription,
        case: GrantHeaderCase,
        reason: h2::Reason,
        phase: GrantResetPhase,
        expectation: GrantResetExpectation,
    ) -> Result<()> {
        let GrantResetExpectation {
            recover: should_recover,
            original_actors_live,
        } = expectation;
        let control = RootReplyFaultControl { core: core.clone() };
        let initial_peer_cancels = control.exchange_observation().peer_cancels;
        let mut drivers = JoinSet::new();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            let (up_client, up_server) = tokio::io::duplex(65536);
            let upstream_core = core.clone();
            let (up_reset_tx, mut up_reset_rx) = tokio::sync::oneshot::channel();
            drivers.spawn(async move {
                let mut connection = server::handshake::<_>(up_server).await?;
                let (request, mut response) = connection
                    .accept()
                    .await
                    .context("pre-header grant upstream request absent")??;
                let work = async {
                    assert_eq!(request.uri().path(), GRANTS);
                    let caller = authenticate(&upstream_core, &request, 1)?;
                    let mut inbound = request.into_body();
                    let (body, trailers) = unary_body(
                        &mut inbound,
                        BufferContext::standalone(upstream_core.clone()),
                    )
                    .await?;
                    assert!(trailers.is_none());
                    let actual = codec::decode_grant_subscription(
                        &wire::SubscribeExchangeGrantsRequest::decode(single_message(&body)?)?,
                        FieldPath::root("pre_header_upstream_subscription"),
                    )?;
                    require_pair(&upstream_core, actual.pair, caller, 1)?;
                    assert_eq!(actual, expected);
                    let mut output = response.send_response(Response::new(()), false)?;
                    let actual = poll_fn(|cx| output.poll_reset(cx)).await?;
                    let _ = up_reset_tx.send(actual);
                    std::future::pending::<Result<()>>().await
                };
                let healthy = async {
                    let (request, mut response) = connection
                        .accept()
                        .await
                        .context("pre-header grant healthy upstream request absent")??;
                    assert_eq!(request.uri().path(), "/healthy-upstream");
                    response.send_response(Response::new(()), true)?;
                    while connection.accept().await.is_some() {}
                    Ok::<_, anyhow::Error>(())
                };
                tokio::join!(work, healthy).1
            });
            let (up_sender, up_driver) = client::handshake(up_client).await?;
            let mut up_keep_alive = up_sender.clone();
            drivers
                .spawn(async move { up_driver.await.context("pre-header grant upstream driver") });
            let (down_client, down_server) = tokio::io::duplex(65536);
            let downstream_core = core.clone();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
            drivers.spawn(async move {
                let mut connection = server::handshake::<_>(down_server).await?;
                let (request, mut response) = connection
                    .accept()
                    .await
                    .context("pre-header grant downstream request absent")??;
                let work = async {
                    assert_eq!(request.uri().path(), GRANTS);
                    let caller = authenticate(&downstream_core, &request, 1)?;
                    require_uncompressed(request.headers())?;
                    let (parts, mut inbound) = request.into_parts();
                    let (body, trailers) = unary_body(
                        &mut inbound,
                        BufferContext::standalone(downstream_core.clone()),
                    )
                    .await?;
                    assert!(trailers.is_none());
                    let actual = codec::decode_grant_subscription(
                        &wire::SubscribeExchangeGrantsRequest::decode(single_message(&body)?)?,
                        FieldPath::root("pre_header_downstream_subscription"),
                    )?;
                    require_pair(&downstream_core, actual.pair, caller, 1)?;
                    assert_eq!(actual, expected);
                    let mut scope = VerifiedGrantHeaderScope {
                        expected: actual,
                        caller,
                        backend_index: 1,
                        downstream: response.stream_id().as_u32(),
                    };
                    if case == GrantHeaderCase::ForeignScope {
                        scope.caller = BackendProcessId::new_v7();
                    }
                    let mut up_sender = up_sender.ready().await?;
                    let (up_reply, mut sending) =
                        up_sender.send_request(Request::from_parts(parts, ()), false)?;
                    sending.send_data(body, true)?;
                    let _up_reply_body = up_reply.await?.into_body();
                    let mut output = match phase {
                        GrantResetPhase::Headers(_) => None,
                        GrantResetPhase::Body(_) => Some(response.send_response(Response::new(()), false)?),
                    };
                    if let Some(output) = output.as_mut() {
                        assert!(matches!(recover_grant_cancel_after_headers(
                            &downstream_core, scope, output, &mut sending,
                        ).await, GrantCancelProbe::Pending),
                            "Pending body reset cannot authorize cancellation");
                    } else {
                        assert!(!recover_grant_cancel_before_headers(
                            &downstream_core, scope, &mut response, &mut inbound, &mut sending,
                        ).await, "Pending reset cannot authorize cancellation");
                    }
                    assert_eq!(
                        downstream_core
                            .state
                            .lock()
                            .unwrap()
                            .exchange
                            .live_capture()
                            .unwrap()
                            .peer_cancels,
                        initial_peer_cancels
                    );
                    let _ = ready_tx.send(());
                    let actual = if let Some(output) = output.as_mut() {
                        poll_fn(|cx| output.poll_reset(cx)).await?
                    } else { poll_fn(|cx| response.poll_reset(cx)).await? };
                    assert_eq!(actual, reason);
                    if case == GrantHeaderCase::ReclassifiedSubscription {
                        // Replace the formerly disjoint capture while original
                        // headers are awaited. Its unknown cut target is protected.
                        let control = RootReplyFaultControl {
                            core: downstream_core.clone(),
                        };
                        control.release_exchange();
                        control.begin_exchange_capture(
                            ExchangeFaultAction::CutGrantSubscription,
                            Instant::now() + Duration::from_secs(1),
                        )?;
                    }
                    if case == GrantHeaderCase::ExpiredScope {
                        // Expire the original bound authority after the real
                        // upstream await; retaining its roster is not renewal.
                        downstream_core
                            .state
                            .lock()
                            .unwrap()
                            .exchange
                            .session_deadline = Some(Instant::now());
                    }
                    let recovered = match phase {
                        GrantResetPhase::Headers(after_failed_headers) => {
                            if after_failed_headers {
                                assert!(response.send_response(Response::new(()), false).is_err());
                            }
                            let recovered = recover_grant_cancel_before_headers(
                                &downstream_core, scope, &mut response, &mut inbound, &mut sending,
                            ).await;
                            if !recovered {
                                assert!(response.send_response(Response::new(()), false).is_err());
                            }
                            recovered
                        }
                        GrantResetPhase::Body(part) => {
                            if matches!(case, GrantHeaderCase::Custody |
                                GrantHeaderCase::SelectedSubscription | GrantHeaderCase::Rebuild) {
                                let control = RootReplyFaultControl { core: downstream_core.clone() };
                                control.release_exchange();
                                assert!(control.exchange_observation().released,
                                    "release alone must not discharge the protected pair");
                            }
                            let is_message = matches!(part, GrantBodyPart::Message);
                            let (part, stage) = match part {
                                GrantBodyPart::Message => {
                                    let batch = codec::DecodedExchangeGrantBatch {
                                        generation: expected.generation, snapshot: true, snapshot_complete: true,
                                        states: vec![codec::DecodedExchangeStreamState { stream,
                                            facts: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts {
                                                granted_through: 1, ..Default::default()
                                            },
                                        }],
                                    };
                                    (GrantOutput::Message(encoded(codec::encode_grant_batch(
                                        &batch, expected.pair, FieldPath::root("grant_body_output_fixture"),
                                    )?)), "send-message")
                                }
                                GrantBodyPart::Trailers => {
                                    let mut trailers = HeaderMap::new();
                                    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                                    (GrantOutput::Trailers(trailers), "send-trailers")
                                }
                                GrantBodyPart::End => (GrantOutput::End, "send-eof"),
                            };
                            match forward_grant_output(
                                &downstream_core, scope, output.as_mut().unwrap(), &mut sending, part,
                            ).await {
                                Ok(forwarded) => {
                                    assert!(!forwarded, "closed output cannot forward successfully");
                                    true
                                }
                                Err(error) => {
                                    let diagnostic = error.downcast_ref::<GrantOutputFailure>()
                                        .context("grant output lost its typed send/reset diagnostic")?;
                                    assert!(!peer_cancel(&error), "the original catch cannot override a scoped grant rejection");
                                    if is_message {
                                        assert!(error.downcast_ref::<CapacityClosed>().is_some(),
                                            "the real closed H2 output must retain its original capacity/reset cause");
                                    }
                                    assert_eq!(diagnostic.stage, stage);
                                    assert_eq!(diagnostic.scope.expected, expected);
                                    if reason == h2::Reason::CANCEL {
                                        assert!(matches!(&diagnostic.reset,
                                            GrantCancelProbe::ReadyCancelRejected(_)),
                                            "actual CANCEL must remain diagnostic when rejected");
                                    } else {
                                        assert!(matches!(&diagnostic.reset,
                                            GrantCancelProbe::ReadyOther(actual) if *actual == u32::from(reason)));
                                    }
                                    false
                                }
                            }
                        }
                    };
                    let _ = outcome_tx.send(recovered);
                    std::future::pending::<Result<()>>().await
                };
                let healthy = async {
                    let (request, mut response) = connection
                        .accept()
                        .await
                        .context("pre-header grant healthy next request absent")??;
                    assert_eq!(request.uri().path(), "/healthy-next");
                    response.send_response(Response::new(()), true)?;
                    while connection.accept().await.is_some() {}
                    Ok::<_, anyhow::Error>(())
                };
                tokio::join!(work, healthy).1
            });
            let (mut sender, driver) = client::handshake(down_client).await?;
            drivers
                .spawn(async move { driver.await.context("pre-header grant downstream driver") });
            let request = signed_request(
                GRANTS,
                peers(stream)[0].1.clone(),
                NativeProcessIdentity::Backend(expected.pair.sender),
            )?;
            let (reply, mut send) = sender.send_request(request, false)?;
            send.send_data(encoded(codec::encode_grant_subscription(expected)), true)?;
            ready_rx.await?;
            assert!(matches!(
                up_reset_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            send.send_reset(reason);
            assert_eq!(outcome_rx.await?, should_recover);
            if should_recover {
                assert_eq!(up_reset_rx.await?, h2::Reason::CANCEL);
            } else {
                assert!(matches!(
                    up_reset_rx.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ));
            }
            let peer_cancels = control.exchange_observation().peer_cancels;
            if original_actors_live {
                assert!(peer_cancels >= initial_peer_cancels + u64::from(should_recover));
            } else {
                assert_eq!(peer_cancels, initial_peer_cancels + u64::from(should_recover));
            }
            assert!(control.observation().failures.is_empty());
            drop(reply);
            drop(send);
            sender = sender.ready().await?;
            let (next, next_send) = sender.send_request(
                Request::builder()
                    .uri("http://localhost/healthy-next")
                    .body(())?,
                true,
            )?;
            assert_eq!(next.await?.status(), http::StatusCode::OK);
            up_keep_alive = up_keep_alive.ready().await?;
            let (up_next, up_next_send) = up_keep_alive.send_request(
                Request::builder()
                    .uri("http://localhost/healthy-upstream")
                    .body(())?,
                true,
            )?;
            assert_eq!(up_next.await?.status(), http::StatusCode::OK);
            drop(up_next_send);
            drop(next_send);
            drop(sender);
            drop(up_keep_alive);
            Ok::<_, anyhow::Error>(())
        })
        .await;
        drivers.abort_all();
        let mut cleanup_error = None;
        while let Some(joined) = drivers.join_next().await {
            let failure = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if error.is_cancelled() => None,
                Err(error) => Some(error.into()),
            };
            if cleanup_error.is_none() {
                cleanup_error = failure;
            }
        }
        result.context("pre-header grant cancellation fixture deadline")??;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        if !original_actors_live {
            assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn armed_non_target_grant_pre_header_cancel_preserves_healthy_h2_successor() -> Result<()>
    {
        for case in [GrantHeaderCase::HeldUnit, GrantHeaderCase::HeldReply] {
            grant_pre_headers_reset_case(case, h2::Reason::CANCEL, false).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn grant_cancel_remains_observable_after_failed_response_headers() -> Result<()> {
        grant_pre_headers_reset_case(GrantHeaderCase::HeldUnit, h2::Reason::CANCEL, true).await
    }

    #[tokio::test]
    async fn custody_target_unknown_subscription_and_rebuild_protect_header_failures() -> Result<()>
    {
        for case in [
            GrantHeaderCase::Custody,
            GrantHeaderCase::SelectedSubscription,
            GrantHeaderCase::Rebuild,
            GrantHeaderCase::UnknownSubscription,
        ] {
            grant_pre_headers_reset_case(case, h2::Reason::CANCEL, true).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn non_cancel_grant_reset_does_not_authorize_header_recovery() -> Result<()> {
        grant_pre_headers_reset_case(GrantHeaderCase::HeldUnit, h2::Reason::REFUSED_STREAM, true)
            .await
    }

    #[tokio::test]
    async fn grant_header_recovery_rechecks_current_capture_after_await() -> Result<()> {
        grant_pre_headers_reset_case(
            GrantHeaderCase::ReclassifiedSubscription,
            h2::Reason::CANCEL,
            true,
        )
        .await
    }

    #[tokio::test]
    async fn grant_header_recovery_requires_the_original_exact_verified_scope() -> Result<()> {
        grant_pre_headers_reset_case(GrantHeaderCase::ForeignScope, h2::Reason::CANCEL, true).await
    }

    #[tokio::test]
    async fn grant_header_recovery_rejects_the_original_bound_scope_expired_after_await()
    -> Result<()> {
        grant_pre_headers_reset_case(GrantHeaderCase::ExpiredScope, h2::Reason::CANCEL, true).await
    }

    #[tokio::test]
    async fn armed_non_target_grant_body_cancel_preserves_healthy_h2_successors() -> Result<()> {
        for part in [
            GrantBodyPart::Message,
            GrantBodyPart::Trailers,
            GrantBodyPart::End,
        ] {
            for case in [GrantHeaderCase::HeldUnit, GrantHeaderCase::HeldReply] {
                grant_reset_case(case, h2::Reason::CANCEL, GrantResetPhase::Body(part)).await?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn protected_grant_body_cancel_preserves_real_reset_diagnostic_after_release()
    -> Result<()> {
        for case in [
            GrantHeaderCase::Custody,
            GrantHeaderCase::SelectedSubscription,
            GrantHeaderCase::Rebuild,
            GrantHeaderCase::UnknownSubscription,
        ] {
            for part in [GrantBodyPart::Message, GrantBodyPart::Trailers] {
                grant_reset_case(case, h2::Reason::CANCEL, GrantResetPhase::Body(part)).await?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn non_cancel_grant_body_reset_preserves_failure_and_exact_reason() -> Result<()> {
        for part in [
            GrantBodyPart::Message,
            GrantBodyPart::Trailers,
            GrantBodyPart::End,
        ] {
            grant_reset_case(
                GrantHeaderCase::HeldUnit,
                h2::Reason::REFUSED_STREAM,
                GrantResetPhase::Body(part),
            )
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn grant_body_recovery_rechecks_current_capture_and_original_bound_authority()
    -> Result<()> {
        for case in [
            GrantHeaderCase::ReclassifiedSubscription,
            GrantHeaderCase::ForeignScope,
            GrantHeaderCase::ExpiredScope,
        ] {
            grant_reset_case(
                case,
                h2::Reason::CANCEL,
                GrantResetPhase::Body(GrantBodyPart::Trailers),
            )
            .await?;
        }
        Ok(())
    }

    // The H2 error from trying headers on a reset stream is not evidence of
    // cancellation. Only the original stream's separately observed reset is.
    async fn pre_headers_reset_case(
        class: StreamClass,
        reason: h2::Reason,
        after_failed_headers: bool,
    ) -> Result<()> {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let stream = stream();
        let caller = FrontendProcessId::new_v7();
        if class == StreamClass::NonTarget {
            control.bind_exchange_peers(peers(stream), Instant::now() + Duration::from_secs(5))?;
            control.begin_exchange_capture(
                ExchangeFaultAction::HoldDataBeforeForward,
                Instant::now() + Duration::from_secs(2),
            )?;
        } else if class == StreamClass::Target {
            let mut state = core.state.lock().unwrap();
            state.frozen_root = Some((stream.destination(), caller));
            state.frozen_backend_index = Some(1);
        }
        let mut drivers = JoinSet::new();
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
            let server_core = core.clone();
            drivers.spawn(async move {
                let mut connection = server::handshake::<_>(server_io).await?;
                let (request, mut response) = connection
                    .accept()
                    .await
                    .context("pre-header Root request absent")??;
                let work = async {
                    assert_eq!(request.uri().path(), ROOT_PATH);
                    assert_eq!(
                        authenticated_normal_frontend(&request, &server_core),
                        Some(caller)
                    );
                    let mut inbound = request.into_body();
                    let (body, trailers) = read_bounded(
                        &mut inbound,
                        REQUEST_BYTES,
                        BufferContext::standalone(server_core.clone()),
                    )
                    .await?;
                    assert!(trailers.is_none());
                    let read = decode_read(
                        &wire::FetchRootResultRequest::decode(single_message(&body)?)?,
                        FieldPath::root("pre_header_root_read"),
                    )?;
                    assert_eq!(read.root_task(), stream.destination());
                    drop(body);
                    let observed_class = classify_frozen_read(
                        &server_core.state.lock().unwrap(),
                        read.root_task(),
                        caller,
                    );
                    assert_eq!(observed_class, class);
                    assert!(
                        !recover_exchange_root_cancel_before_headers(
                            observed_class,
                            &mut response,
                            &mut inbound,
                            &server_core
                        )
                        .await
                    );
                    assert_eq!(server_core.state.lock().unwrap().non_target_peer_cancels, 0);
                    let _ = ready_tx.send(());
                    let actual = poll_fn(|cx| response.poll_reset(cx)).await?;
                    assert_eq!(actual, reason);
                    if after_failed_headers {
                        // Reproduce the failed h2 transition, without inspecting
                        // or converting that error into a cancellation fact.
                        assert!(response.send_response(Response::new(()), false).is_err());
                    }
                    let recovered = recover_exchange_root_cancel_before_headers(
                        observed_class,
                        &mut response,
                        &mut inbound,
                        &server_core,
                    )
                    .await;
                    let _ = outcome_tx.send(recovered);
                    std::future::pending::<Result<()>>().await
                };
                tokio::select! {
                    result=work=>result,
                    _=async {while connection.accept().await.is_some(){}}=>Ok(()),
                }
            });
            let (mut sender, driver) = client::handshake(client_io).await?;
            drivers.spawn(async move { driver.await.context("pre-header client driver") });
            let read = RootResultRead::try_new(
                stream.destination(),
                RootProfileId::V1,
                RootOutputKind::ClientRows,
                NonZeroU64::new(1),
                0,
                Duration::from_millis(100),
            )?;
            let request = signed_request(
                ROOT_PATH,
                core.normal_subject.clone(),
                NativeProcessIdentity::Frontend(caller),
            )?;
            let (reply, mut send) = sender.send_request(request, false)?;
            send.send_data(
                encoded(novarocks_task_codec::root_result::encode_read(&read)),
                true,
            )?;
            ready_rx.await?;
            send.send_reset(reason);
            let recovered = outcome_rx.await?;
            assert_eq!(
                recovered,
                class == StreamClass::NonTarget && reason == h2::Reason::CANCEL
            );
            assert_eq!(
                control.observation().non_target_peer_cancels,
                u64::from(recovered)
            );
            drop(reply);
            drop(send);
            drop(sender);
            Ok::<_, anyhow::Error>(())
        })
        .await;
        // Cleanup still joins every original fixture task when an assertion,
        // protocol error or deadline fails; no disconnect is called an exit.
        drivers.abort_all();
        let mut cleanup_error = None;
        while let Some(joined) = drivers.join_next().await {
            let failure = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if error.is_cancelled() => None,
                Err(error) => Some(error.into()),
            };
            if cleanup_error.is_none() {
                cleanup_error = failure;
            }
        }
        result.context("pre-header reset fixture deadline")??;
        if let Some(error) = cleanup_error {
            return Err(error);
        }

        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }
    #[tokio::test]
    async fn non_target_root_reset_remains_observable_after_failed_response_headers() -> Result<()>
    {
        pre_headers_reset_case(StreamClass::NonTarget, h2::Reason::CANCEL, true).await
    }
    #[tokio::test]
    async fn unknown_root_reset_does_not_authorize_header_recovery() -> Result<()> {
        pre_headers_reset_case(StreamClass::Unknown, h2::Reason::CANCEL, true).await
    }
    #[tokio::test]
    async fn original_root_target_reset_does_not_authorize_header_recovery() -> Result<()> {
        pre_headers_reset_case(StreamClass::Target, h2::Reason::CANCEL, true).await
    }
    #[tokio::test]
    async fn non_cancel_root_reset_does_not_authorize_header_recovery() -> Result<()> {
        pre_headers_reset_case(StreamClass::NonTarget, h2::Reason::REFUSED_STREAM, true).await
    }

    #[tokio::test]
    async fn accepted_wire_reply_is_lost_once_and_exact_replay_reaches_original_service()
    -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(6),
            unit_case(ExchangeFaultAction::DropAcceptedReply),
        )
        .await
        .context("real H2 accepted-reply test deadline")?
    }
    #[tokio::test]
    async fn selected_subscription_rebuild_releases_the_original_held_unit() -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(6),
            unit_case(ExchangeFaultAction::CutGrantSubscription),
        )
        .await
        .context("real H2 subscription rebuild test deadline")?
    }
    #[tokio::test]
    async fn preforward_gate_covers_real_cancel_and_retry_until_explicit_release() -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(6),
            unit_case(ExchangeFaultAction::HoldDataBeforeForward),
        )
        .await
        .context("real H2 preforward test deadline")?
    }

    #[tokio::test]
    async fn real_dropped_reply_guard_and_exact_replay_exit_allow_later_grant_cancel() -> Result<()>
    {
        tokio::time::timeout(
            Duration::from_secs(6),
            unit_case_with_obligation_probe(ExchangeFaultAction::DropAcceptedReply, true),
        )
        .await
        .context("completed drop obligation component fixture deadline")?
    }

    #[tokio::test]
    async fn real_cut_snapshot_and_held_unit_exit_allow_only_the_exact_rebuilt_grant_scope()
    -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(6),
            unit_case_with_obligation_probe(ExchangeFaultAction::CutGrantSubscription, true),
        )
        .await
        .context("completed cut obligation component fixture deadline")?
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum DeliveryResetCase {
        NonTarget,
        Selected,
        SameStreamEnd,
        SameStreamSuccessor,
        Custody,
        Subscription,
        Unknown,
        ChangedCapture,
        Expired,
        ReplacedPeer,
    }

    // The original request, JWT, decoder, EOF and both reset handles are real.
    // This qualifies the actor's output proof, not a Native receiver or SQL.
    async fn delivery_reset_case(
        case: DeliveryResetCase,
        reason: h2::Reason,
        phase: GrantResetPhase,
    ) -> Result<()> {
        let core = super::super::tests::core();
        let control = RootReplyFaultControl { core: core.clone() };
        let selected = stream();
        let request_stream = if matches!(
            case,
            DeliveryResetCase::Selected
                | DeliveryResetCase::SameStreamEnd
                | DeliveryResetCase::SameStreamSuccessor
        ) {
            selected
        } else {
            ExchangeStreamIdentity::try_new(
                selected.source(),
                selected.destination(),
                selected.destination_fragment_instance_id(),
                FragmentNodeId::new(selected.destination_node().get() + 1),
            )?
        };
        control.bind_exchange_peers(peers(selected), Instant::now() + Duration::from_secs(5))?;
        let action = match case {
            DeliveryResetCase::Custody => ExchangeFaultAction::DropAcceptedReply,
            DeliveryResetCase::Subscription | DeliveryResetCase::Unknown => {
                ExchangeFaultAction::CutGrantSubscription
            }
            _ => ExchangeFaultAction::HoldDataBeforeForward,
        };
        control.begin_exchange_capture(action, Instant::now() + Duration::from_secs(2))?;
        if case != DeliveryResetCase::Unknown {
            let candidate = ExchangeFaultCandidate {
                target: if case == DeliveryResetCase::Subscription {
                    ExchangeFaultTarget::Subscription {
                        stream: selected,
                        pair: ExchangeProcessPair::for_stream(selected),
                        generation: NonZeroU64::new(7).unwrap(),
                    }
                } else {
                    ExchangeFaultTarget::Unit {
                        stream: selected,
                        sequence: ExchangeUnitSequence::FIRST,
                    }
                },
                backend_index: 1,
                caller: selected.source().task().backend_process_id(),
                downstream_stream: 1,
                request_sha256: hash(&data(selected)),
            };
            core.state
                .lock()
                .unwrap()
                .exchange
                .live_capture_mut()
                .unwrap()
                .candidate = Some(candidate.clone());
            control.arm_exchange_exact(&candidate)?;
        }
        let mut request_body = data(request_stream);
        if matches!(
            case,
            DeliveryResetCase::SameStreamEnd | DeliveryResetCase::SameStreamSuccessor
        ) {
            let mut frame = codec::decode_delivery_request(
                wire::DeliverExchangeUnitRequest::decode(single_message(&request_body)?)?,
                FieldPath::root("delivery_reset_request_fixture"),
            )?;
            let sequence = ExchangeUnitSequence::try_new(2)?;
            if case == DeliveryResetCase::SameStreamEnd {
                frame.item = ExchangeUnitItem::End(sequence);
                frame.payload = Bytes::new();
            } else {
                frame.item = ExchangeUnitItem::Unit { demand:
                    novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitDemand {
                        sequence, wire_bytes: frame.payload.len() as u64 }, end_after_unit: false };
            }
            request_body = encoded(codec::encode_delivery_request(frame));
        }
        let should_recover = case == DeliveryResetCase::NonTarget && reason == h2::Reason::CANCEL;
        let mut drivers = JoinSet::new();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            let (up_client, up_server) = tokio::io::duplex(65536);
            let upstream_core = core.clone();
            let (up_reset_tx, mut up_reset_rx) = tokio::sync::oneshot::channel();
            drivers.spawn(async move {
                let mut connection = server::handshake::<_>(up_server).await?;
                let (request, mut response) = connection.accept().await.context("delivery upstream request absent")??;
                let work = async {
                    assert_eq!(request.uri().path(), DELIVER);
                    let caller = authenticate(&upstream_core, &request, 1)?;
                    let (body, _) = unary_body(&mut request.into_body(), BufferContext::standalone(upstream_core.clone())).await?;
                    let frame = codec::decode_delivery_request(
                        wire::DeliverExchangeUnitRequest::decode(single_message(&body)?)?,
                        FieldPath::root("delivery_reset_upstream_request"))?;
                    require_pair(&upstream_core, ExchangeProcessPair::for_stream(frame.stream), caller, 1)?;
                    assert_eq!(frame.stream, request_stream);
                    let mut output = response.send_response(Response::new(()), false)?;
                    let observed = poll_fn(|cx| output.poll_reset(cx)).await?;
                    let _ = up_reset_tx.send(observed);
                    std::future::pending::<Result<()>>().await
                };
                let healthy = async {
                    let (request, mut response) = connection.accept().await.context("delivery healthy upstream request absent")??;
                    assert_eq!(request.uri().path(), "/healthy-upstream");
                    response.send_response(Response::new(()), true)?;
                    while connection.accept().await.is_some() {}
                    Ok::<_, anyhow::Error>(())
                };
                tokio::join!(work, healthy).1
            });
            let (up_sender, up_driver) = client::handshake(up_client).await?;
            let mut up_keep_alive = up_sender.clone();
            drivers.spawn(async move { up_driver.await.context("delivery upstream driver") });
            let (down_client, down_server) = tokio::io::duplex(65536);
            let downstream_core = core.clone();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
            drivers.spawn(async move {
                let mut connection = server::handshake::<_>(down_server).await?;
                let (request, mut response) = connection.accept().await.context("delivery downstream request absent")??;
                let work = async {
                    assert_eq!(request.uri().path(), DELIVER);
                    let caller = authenticate(&downstream_core, &request, 1)?;
                    require_uncompressed(request.headers())?;
                    let (parts, mut inbound) = request.into_parts();
                    let (body, trailers) = unary_body(&mut inbound, BufferContext::standalone(downstream_core.clone())).await?;
                    assert!(trailers.is_none());
                    let frame = codec::decode_delivery_request(
                        wire::DeliverExchangeUnitRequest::decode(single_message(&body)?)?,
                        FieldPath::root("delivery_reset_downstream_request"))?;
                    let scope = verified_delivery_scope(&downstream_core, &frame, caller, 1,
                        response.stream_id().as_u32(), false)?;
                    let mut sender = up_sender.ready().await?;
                    let (reply, mut sending) = sender.send_request(Request::from_parts(parts, ()), false)?;
                    sending.send_data(body, true)?;
                    let _upstream_body = reply.await?.into_body();
                    let mut output = match phase {
                        GrantResetPhase::Headers(_) => None,
                        GrantResetPhase::Body(_) => Some(response.send_response(Response::new(()), false)?),
                    };
                    let pending = if let Some(output) = output.as_mut() {
                        recover_delivery_output_cancel(&downstream_core, scope,
                            DeliveryResetHandle::Body(output), &mut sending).await
                    } else {
                        recover_delivery_output_cancel(&downstream_core, scope,
                            DeliveryResetHandle::Headers(&mut response, &mut inbound), &mut sending).await
                    };
                    assert!(matches!(pending, DeliveryCancelProbe::Pending));
                    assert_eq!(RootReplyFaultControl { core: downstream_core.clone() }.exchange_observation().peer_cancels, 0);
                    let _ = ready_tx.send(());
                    let actual = if let Some(output) = output.as_mut() {
                        poll_fn(|cx| output.poll_reset(cx)).await?
                    } else { poll_fn(|cx| response.poll_reset(cx)).await? };
                    assert_eq!(actual, reason);
                    let control = RootReplyFaultControl { core: downstream_core.clone() };
                    match case {
                        DeliveryResetCase::ChangedCapture => {
                            control.release_exchange();
                            control.begin_exchange_capture(ExchangeFaultAction::CutGrantSubscription,
                                Instant::now() + Duration::from_secs(1))?;
                        }
                        DeliveryResetCase::Expired => {
                            downstream_core.state.lock().unwrap().exchange.session_deadline = Some(Instant::now());
                        }
                        DeliveryResetCase::ReplacedPeer => {
                            let mut state = downstream_core.state.lock().unwrap();
                            state.exchange.peers.iter_mut().find(|(index, _, _)| *index == 1).unwrap().2 = BackendProcessId::new_v7();
                        }
                        DeliveryResetCase::Selected | DeliveryResetCase::SameStreamEnd |
                        DeliveryResetCase::SameStreamSuccessor | DeliveryResetCase::Custody | DeliveryResetCase::Subscription | DeliveryResetCase::Unknown => {
                            control.release_exchange();
                            assert!(control.exchange_observation().released, "release is not a protected obligation receipt");
                        }
                        _ => {},
                    }
                    let result = match phase {
                        GrantResetPhase::Headers(after_failed_headers) => {
                            if after_failed_headers {
                                assert!(response.send_response(Response::new(()), false).is_err());
                            }
                            delivery_response_headers(&downstream_core, scope, &mut response,
                                &mut inbound, &mut sending, Response::new(()).into_parts().0).await.map(|output| output.is_none())
                        }
                        GrantResetPhase::Body(part) => {
                            let part = match part {
                                GrantBodyPart::Message => GrantOutput::Message(encoded(codec::encode_delivery_response(
                                    frame.stream, frame.item.sequence(), DecodedExchangeDeliveryOutcome::Observed {
                                        verdict: ExchangeAdmissionVerdict::Applied,
                                        facts: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts {
                                            custody_through: frame.item.sequence().get(), ..Default::default() },
                                    }))),
                                GrantBodyPart::Trailers => {
                                    let mut trailers = HeaderMap::new();
                                    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                                    GrantOutput::Trailers(trailers)
                                }
                                GrantBodyPart::End => GrantOutput::End,
                            };
                            forward_delivery_output(&downstream_core, scope, output.as_mut().unwrap(), &mut sending, part).await.map(|forwarded| !forwarded)
                        }
                    };
                    let recovered = match result {
                        Ok(recovered) => { assert!(recovered); true },
                        Err(error) => {
                            let diagnostic = error.downcast_ref::<DeliveryOutputFailure>().context("delivery output lost scope/reset diagnostic")?;
                            assert!(!peer_cancel(&error), "the original outer catch cannot override a scoped delivery rejection");
                            if matches!(phase, GrantResetPhase::Body(GrantBodyPart::Message)) {
                                assert!(error.downcast_ref::<CapacityClosed>().is_some(),
                                    "the real closed H2 output must retain its original capacity/reset cause");
                            }
                            assert_eq!(diagnostic.scope.stream, request_stream);
                            assert_eq!(diagnostic.scope.item.sequence(), frame.item.sequence());
                            assert_eq!(diagnostic.stage, match phase {
                                GrantResetPhase::Headers(_) => "response-headers",
                                GrantResetPhase::Body(GrantBodyPart::Message) => "send-message",
                                GrantResetPhase::Body(GrantBodyPart::Trailers) => "send-trailers",
                                GrantResetPhase::Body(GrantBodyPart::End) => "send-eof",
                            });
                            if reason == h2::Reason::CANCEL {
                                assert!(matches!(diagnostic.reset, DeliveryCancelProbe::ReadyCancelRejected(_)));
                            } else {
                                assert!(matches!(diagnostic.reset, DeliveryCancelProbe::ReadyOther(actual) if actual == u32::from(reason)));
                            }
                            false
                        }
                    };
                    let _ = outcome_tx.send(recovered);
                    std::future::pending::<Result<()>>().await
                };
                let healthy = async {
                    let (request, mut response) = connection.accept().await.context("delivery healthy downstream request absent")??;
                    assert_eq!(request.uri().path(), "/healthy-next");
                    response.send_response(Response::new(()), true)?;
                    while connection.accept().await.is_some() {}
                    Ok::<_, anyhow::Error>(())
                };
                tokio::join!(work, healthy).1
            });
            let (mut sender, driver) = client::handshake(down_client).await?;
            drivers.spawn(async move { driver.await.context("delivery downstream driver") });
            let request = signed_request(DELIVER, peers(selected)[0].1.clone(),
                NativeProcessIdentity::Backend(request_stream.source().task().backend_process_id()))?;
            let (reply, mut send) = sender.send_request(request, false)?;
            send.send_data(request_body, true)?;
            ready_rx.await?;
            assert!(matches!(up_reset_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
            send.send_reset(reason);
            assert_eq!(outcome_rx.await?, should_recover);
            if should_recover { assert_eq!(up_reset_rx.await?, h2::Reason::CANCEL); }
            else { assert!(matches!(up_reset_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty))); }
            assert_eq!(control.exchange_observation().peer_cancels, u64::from(should_recover));
            drop(reply); drop(send);
            sender = sender.ready().await?;
            let (next, next_send) = sender.send_request(Request::builder().uri("http://localhost/healthy-next").body(())?, true)?;
            assert_eq!(next.await?.status(), http::StatusCode::OK);
            up_keep_alive = up_keep_alive.ready().await?;
            let (next_up, next_up_send) = up_keep_alive.send_request(Request::builder().uri("http://localhost/healthy-upstream").body(())?, true)?;
            assert_eq!(next_up.await?.status(), http::StatusCode::OK);
            drop(next_send); drop(next_up_send); drop(sender); drop(up_keep_alive);
            Ok::<_, anyhow::Error>(())
        }).await;
        drivers.abort_all();
        let mut cleanup_error = None;
        while let Some(joined) = drivers.join_next().await {
            let failure = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if error.is_cancelled() => None,
                Err(error) => Some(error.into()),
            };
            if cleanup_error.is_none() {
                cleanup_error = failure;
            }
        }
        result.context("delivery reset real H2 fixture deadline")??;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[tokio::test]
    async fn non_target_delivery_cancel_uses_real_header_and_body_handles_and_keeps_both_connections_healthy()
    -> Result<()> {
        for phase in [
            GrantResetPhase::Headers(false),
            GrantResetPhase::Headers(true),
            GrantResetPhase::Body(GrantBodyPart::Message),
            GrantResetPhase::Body(GrantBodyPart::Trailers),
            GrantResetPhase::Body(GrantBodyPart::End),
        ] {
            delivery_reset_case(DeliveryResetCase::NonTarget, h2::Reason::CANCEL, phase).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn delivery_cancel_cannot_discharge_selected_stream_custody_cut_or_unknown_scope_after_release()
    -> Result<()> {
        for case in [
            DeliveryResetCase::Selected,
            DeliveryResetCase::SameStreamEnd,
            DeliveryResetCase::SameStreamSuccessor,
            DeliveryResetCase::Custody,
            DeliveryResetCase::Subscription,
            DeliveryResetCase::Unknown,
        ] {
            delivery_reset_case(case, h2::Reason::CANCEL, GrantResetPhase::Headers(true)).await?;
            delivery_reset_case(
                case,
                h2::Reason::CANCEL,
                GrantResetPhase::Body(GrantBodyPart::Message),
            )
            .await?;
            delivery_reset_case(
                case,
                h2::Reason::CANCEL,
                GrantResetPhase::Body(GrantBodyPart::Trailers),
            )
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn delivery_output_rechecks_original_scope_live_pair_and_actual_non_cancel_reason()
    -> Result<()> {
        for phase in [
            GrantResetPhase::Headers(true),
            GrantResetPhase::Body(GrantBodyPart::Trailers),
        ] {
            delivery_reset_case(
                DeliveryResetCase::NonTarget,
                h2::Reason::REFUSED_STREAM,
                phase,
            )
            .await?;
            for case in [
                DeliveryResetCase::ChangedCapture,
                DeliveryResetCase::Expired,
                DeliveryResetCase::ReplacedPeer,
            ] {
                delivery_reset_case(case, h2::Reason::CANCEL, phase).await?;
            }
        }
        Ok(())
    }
}
