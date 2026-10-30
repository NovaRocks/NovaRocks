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
    capture: Option<Capture>,
}
struct Capture {
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
impl Capture {
    fn millis(&self) -> u64 {
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

impl ExchangeState {
    pub(super) fn occupied(&self) -> bool {
        self.capture.is_some()
    }
    pub(super) fn pending_deadline(&self) -> Option<Instant> {
        self.capture
            .as_ref()
            .filter(|capture| !capture.released && !capture.fired)
            .map(|capture| capture.deadline)
    }
    pub(super) fn expire(&mut self, deadline: Instant) -> bool {
        if let Some(capture) = self
            .capture
            .as_mut()
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
                .capture
                .as_ref()
                .is_none_or(|capture| capture.active == 0
                    && capture.active_custody_batches_held == 0
                    && (capture.released || capture.fired)),
            "Previous Exchange target owners have not exited"
        );
        state.used_capture = true;
        state.exchange.capture = Some(Capture {
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
        });
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
            .capture
            .as_ref()
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
            .capture
            .as_mut()
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
            .capture
            .as_mut()
        {
            capture.released = true;
        }
        self.core.changed.notify_waiters();
    }
    pub fn exchange_observation(&self) -> ExchangeFaultObservation {
        let state = self.core.state.lock().expect("root fault state lock");
        let capture = state.exchange.capture.as_ref();
        let attempts = capture
            .map(|capture| capture.attempts.clone())
            .unwrap_or_default();
        ExchangeFaultObservation {
            action: capture.map(|capture| capture.action),
            candidate: capture.and_then(|capture| capture.candidate.clone()),
            armed: capture.is_some_and(|capture| capture.armed),
            released: capture.is_some_and(|capture| capture.released),
            elapsed_millis: capture.map_or(0, Capture::millis),
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
        let mut credit = ByteCredit::new(buffer._credit.core.clone(), next)?;
        buffer
            .bytes
            .try_reserve_exact(next - buffer.bytes.len())
            .context("grow credited Exchange actor buffer")?;
        if buffer.bytes.capacity() > next {
            let mut extra =
                ByteCredit::new(buffer._credit.core.clone(), buffer.bytes.capacity() - next)?;
            credit.bytes += extra.bytes;
            extra.bytes = 0;
        }
        std::mem::swap(&mut buffer._credit, &mut credit);
    }
    buffer.bytes.extend_from_slice(value);
    Ok(())
}
async fn unary_body(body: &mut RecvStream, core: Arc<Core>) -> Result<(Bytes, Option<HeaderMap>)> {
    let mut buffer = OwnedBuffer::new(core, 0)?;
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
        if let Some(capture) = state.exchange.capture.as_mut() {
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
            .capture
            .as_mut()
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
    let Some(capture) = state.exchange.capture.as_mut() else {
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
    capture: &mut Capture,
    candidate: ExchangeFaultCandidate,
    core: Arc<Core>,
) -> Result<AttemptOwner> {
    ensure!(
        capture.attempts.len() < MAX_TARGET_ATTEMPTS,
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
                .capture
                .as_ref()
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
    error.downcast_ref::<h2::Error>().is_some_and(|error|error.is_reset() && error.is_remote() && error.reason()==Some(h2::Reason::CANCEL))
        || error.downcast_ref::<CapacityClosed>().is_some_and(|closed|
            matches!(closed.reset,CapacityResetObservation::Reason(reason) if reason==u32::from(h2::Reason::CANCEL)))
}

pub(super) async fn forward(
    request: Request<RecvStream>,
    mut response: server::SendResponse<Bytes>,
    sender: client::SendRequest<Bytes>,
    backend_index: usize,
    core: Arc<Core>,
) -> Result<()> {
    let caller = authenticate(&core, &request, backend_index)?;
    let path = request.uri().path().to_owned();
    let downstream = response.stream_id().as_u32();
    require_uncompressed(request.headers())?;
    let (parts, mut inbound) = request.into_parts();
    let mut verified = false;
    let mut owner: Option<AttemptOwner> = None;
    let work = async {
        let (body, trailers) = unary_body(&mut inbound, core.clone()).await?;
        ensure!(
            trailers.as_ref().is_none_or(HeaderMap::is_empty),
            "Exchange request contains unexpected trailers"
        );
        let mut unit = None;
        let mut subscription = None;
        match path.as_str() {
            DELIVER => {
                let wire = wire::DeliverExchangeUnitRequest::decode(single_message(&body)?)?;
                let frame = codec::decode_delivery_request(
                    wire,
                    FieldPath::root("exchange_fault_delivery_request"),
                )?;
                require_pair(
                    &core,
                    ExchangeProcessPair::for_stream(frame.stream),
                    caller,
                    backend_index,
                )?;
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
                subscription = Some(decoded);
            }
            _ => bail!("Exchange actor received an unowned method"),
        }
        verified = true;
        if let Some(owner) = owner.as_ref().filter(|owner| owner.mutate) {
            let hold = {
                let state = core.state.lock().expect("root fault state lock");
                let action = state
                    .exchange
                    .capture
                    .as_ref()
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
        let mut sender = sender.ready().await?;
        let (reply, mut sending) = sender.send_request(Request::from_parts(parts, ()), false)?;
        let upstream = sending.stream_id().as_u32();
        let dispatched = Instant::now();
        if let Some(owner) = owner.as_ref() {
            owner.update(|attempt, now| {
                attempt.upstream_stream = Some(upstream);
                attempt.request_forwarded_at_millis = Some(now);
            });
        }
        send_bytes(&mut sending, body.clone()).await?;
        sending.send_data(Bytes::new(), true)?;
        // Only connection/response establishment has the original forward bound.
        // A live grant subscription has no invented actor idle timeout.
        let upstream_response = tokio::time::timeout(
            Duration::from_millis(core.bounds.forward_timeout_millis),
            reply,
        )
        .await
        .context("Exchange upstream response establishment deadline")??;
        let (reply_parts, mut reply_body) = upstream_response.into_parts();
        if let Some(expected) = subscription {
            let mut output =
                response.send_response(Response::from_parts(reply_parts, ()), false)?;
            return forward_grants(
                &core,
                &mut reply_body,
                &mut output,
                &mut sending,
                expected,
                backend_index,
                caller,
                downstream,
                upstream,
                hash(&body),
                dispatched,
            )
            .await;
        }
        if let Some(owner) = owner.as_ref() {
            let (reply_bytes, reply_trailers) = unary_body(&mut reply_body, core.clone()).await?;
            let frame = unit.as_ref().context("Exchange target has no Unit frame")?;
            // Header-only RPC failures remain the real upstream RPC outcome.
            // They never authorize an accepted-receipt mutation.
            if !reply_parts.headers.contains_key("grpc-status") {
                success_status(&reply_parts.headers, reply_trailers.as_ref())?;
                let decoded =
                    wire::DeliverExchangeUnitResponse::decode(single_message(&reply_bytes)?)?;
                if let DecodedExchangeDeliveryOutcome::Observed { verdict, facts } =
                    codec::decode_delivery_response(
                        &decoded,
                        frame.stream,
                        frame.item.sequence(),
                        FieldPath::root("exchange_fault_delivery_response"),
                    )?
                {
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
                            .capture
                            .as_mut()
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
            let mut output =
                response.send_response(Response::from_parts(reply_parts, ()), false)?;
            send_bytes(&mut output, reply_bytes).await?;
            if let Some(trailers) = reply_trailers {
                output.send_trailers(trailers)?;
            } else {
                output.send_data(Bytes::new(), true)?;
            }
        } else {
            let output = response.send_response(Response::from_parts(reply_parts, ()), false)?;
            copy_body(reply_body, output, core.clone()).await?;
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
            if let Some(capture)=core.state.lock().expect("root fault state lock").exchange.capture.as_mut() {capture.peer_cancels+=1;}
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
            .capture
            .as_mut()
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
        if let Some(capture) = state.exchange.capture.as_mut() {
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
                .capture
                .as_mut()
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
    async fn next(&mut self, body: &mut RecvStream, core: Arc<Core>) -> Result<Option<Bytes>> {
        let mut buffer = OwnedBuffer::new(core, 0)?;
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
    body: &mut RecvStream,
    output: &mut SendStream<Bytes>,
    sending: &mut SendStream<Bytes>,
    expected: DecodedExchangeGrantSubscription,
    backend_index: usize,
    caller: BackendProcessId,
    downstream: u32,
    upstream: u32,
    request_sha256: String,
    dispatched: Instant,
) -> Result<()> {
    let mut reader = MessageReader {
        pending: Bytes::new(),
    };
    while let Some(message) = reader.next(body, core.clone()).await? {
        let wire = wire::ExchangeGrantBatch::decode(single_message(&message)?)?;
        let batch = codec::decode_grant_batch(
            &wire,
            expected,
            FieldPath::root("exchange_fault_grant_batch"),
        )?;
        if !hold_selected_custody(core, &batch, output, sending).await? {
            return Ok(());
        }
        let selected = {
            let mut state = core.state.lock().expect("root fault state lock");
            if let Some(capture) = state.exchange.capture.as_mut() {
                if let Some(ExchangeFaultCandidate {
                    target:
                        ExchangeFaultTarget::Subscription {
                            stream,
                            pair,
                            generation,
                        },
                    ..
                }) = capture.candidate.as_ref()
                {
                    if capture.fired && expected.pair == *pair && expected.generation > *generation
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
                        .capture
                        .as_ref()
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
                        .capture
                        .as_mut()
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
        send_bytes(output, message).await?;
    }
    if let Some(trailers) = body.trailers().await? {
        output.send_trailers(trailers)?;
    } else {
        output.send_data(Bytes::new(), true)?;
    }
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
        let (bytes, trailers) = unary_body(&mut body, core).await?;
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
        let mut body = OwnedBuffer::new(core.clone(), 0)?;
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
        let mut body = OwnedBuffer::new(core.clone(), 0)?;
        let credit = ByteCredit::new(core.clone(), core.bounds.maximum_owned_buffer_bytes)?;
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
        let (body, trailers) = unary_body(&mut body, core).await?;
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
                        let path=request.uri().path().to_owned();let (_,mut body)=request.into_parts();let (body,_)=unary_body(&mut body,core).await?;
                        match path.as_str() {
                            DELIVER=>{
                                let decoded=codec::decode_delivery_request(wire::DeliverExchangeUnitRequest::decode(single_message(&body)?)?,FieldPath::root("real_h2_test"))?;
                                assert_eq!(decoded.stream,stream);let sequence=decoded.item.sequence();
                                let applied=state.seen.lock().unwrap().insert(sequence.get());state.received.fetch_add(1,Ordering::AcqRel);state.applied.notify_waiters();
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
                                let batch=codec::DecodedExchangeGrantBatch{generation:expected.generation,snapshot:true,snapshot_complete:true,states:vec![codec::DecodedExchangeStreamState{stream,
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
        let (sender, driver) = client::handshake::<_, Bytes>(io).await?;
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
            let reply = exchange_rpc(sender.clone(), stream, data(stream)).await?;
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
            grant_rpc(sender.clone(), stream, 2).await?;
            unit.await??;
            let observed = control.exchange_observation();
            assert_eq!(observed.subscriptions_cut, 1);
            assert_eq!(observed.rebuilt_generation, Some(2));
            assert!(observed.rebuilt_snapshot_complete);
            assert_eq!(observed.rebuilt_stream.unwrap().stream(), stream);
            assert_eq!(observed.rebuilt_stream_granted_through, Some(1));
            assert_eq!(state.received.load(Ordering::Acquire), 1);
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
        let actors = control.observation();
        assert_eq!(actors.active_listeners, 0);
        assert_eq!(actors.active_connections, 0);
        assert_eq!(actors.active_streams, 0);
        assert!(actors.non_target_peer_cancels > 0);
        assert_eq!(actors.owned_buffer_bytes, 0);
        assert!(actors.shutdown_joined);
        assert!(actors.failures.is_empty(), "{:?}", actors.failures);
        assert_eq!(fixture_core.bytes.load(Ordering::Acquire), 0);
        Ok(())
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
}
