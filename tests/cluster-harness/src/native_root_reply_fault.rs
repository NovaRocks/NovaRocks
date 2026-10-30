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

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use h2::{RecvStream, SendStream, client, server};
use http::{HeaderMap, Request, Response};
use novarocks_execution_contract::{
    identity::TaskIdentity,
    root_result::{RootReadOutcome, RootReadResponse, RootResultRead},
};
use novarocks_native_trust::{NativeCallerSubject, NativeProcessIdentity, NativeServerAdmission};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{novarocks as wire, result as result_wire};
use novarocks_result_contract::{
    ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
};
use novarocks_task_codec::operation::decode_covered_subscribe_task_status;
use novarocks_task_codec::root_result::{decode_read, decode_response};
use novarocks_types::FrontendProcessId;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::future::poll_fn;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;

const ROOT_PATH: &str = "/novarocks.NovaRocksGrpc/FetchRootResult";
const COVERED_STATUS_PATH: &str = "/novarocks.NovaRocksGrpc/SubscribeTaskStatus";
const REQUEST_BYTES: usize = RootProfileV1::ENVELOPE_BYTES;
const RESPONSE_BYTES: usize = RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES;
const FRAME_BYTES: usize = 16 * 1024;
const HEADER_BYTES: u32 = 16 * 1024;
const H2_STREAMS: u32 = 128;
const H2_WINDOW: u32 = 256 * 1024;
const H2_CONNECTION_WINDOW: u32 = 1024 * 1024;
const H2_SEND_BUFFER: usize = 64 * 1024;
const H2_RESETS: usize = 32;
const FAILURE_COUNT: usize = 32;
const FAILURE_BYTES: usize = 4096;
const MAX_TARGET_ATTEMPTS: u64 = 32;

#[derive(Clone, Debug, Serialize)]
pub struct RootReplyFaultBounds {
    pub maximum_connections: usize,
    pub maximum_active_streams: usize,
    pub maximum_owned_buffer_bytes: usize,
    pub handshake_timeout_millis: u64,
    pub forward_timeout_millis: u64,
    pub maximum_capture_millis: u64,
}

impl Default for RootReplyFaultBounds {
    fn default() -> Self {
        Self {
            maximum_connections: 32,
            maximum_active_streams: 256,
            maximum_owned_buffer_bytes: 16 * 1024 * 1024,
            handshake_timeout_millis: 2000,
            forward_timeout_millis: 20_000,
            maximum_capture_millis: 5000,
        }
    }
}

impl RootReplyFaultBounds {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=32).contains(&self.maximum_connections),
            "invalid actor connection bound"
        );
        ensure!(
            (1..=256).contains(&self.maximum_active_streams),
            "invalid actor stream bound"
        );
        ensure!(
            self.maximum_owned_buffer_bytes >= 2 * RESPONSE_BYTES + REQUEST_BYTES
                && self.maximum_owned_buffer_bytes <= 16 * 1024 * 1024,
            "invalid actor owned-buffer bound"
        );
        for millis in [
            self.handshake_timeout_millis,
            self.forward_timeout_millis,
            self.maximum_capture_millis,
        ] {
            ensure!(
                (1..=20_000).contains(&millis),
                "invalid actor deadline bound"
            );
        }
        ensure!(
            self.maximum_capture_millis <= 5000,
            "capture ceiling cannot exceed five seconds"
        );
        Ok(())
    }

    pub(crate) fn semantics(&self) -> serde_json::Value {
        serde_json::json!({
            "mode":"authenticated-plaintext-root-reply-message-v1", "bounds":self,
            "target_slots":1, "maximum_data_listeners":3, "one_shot_per_actor_group":true,
            "maximum_target_attempts":MAX_TARGET_ATTEMPTS, "target_attempt_evidence_positions":MAX_TARGET_ATTEMPTS,
            "not_ready_transparency":true,
            "capture_deadline":"single-absolute-never-reset",
            "request_bytes":REQUEST_BYTES, "response_bytes":RESPONSE_BYTES,
            "h2_frame_bytes":FRAME_BYTES, "h2_header_bytes":HEADER_BYTES,
            "h2_streams_per_connection":H2_STREAMS, "h2_stream_window_bytes":H2_WINDOW,
            "h2_connection_window_bytes":H2_CONNECTION_WINDOW,
            "h2_send_buffer_bytes":H2_SEND_BUFFER, "h2_reset_positions":H2_RESETS,
            "client_hpack_table_bytes":4096, "server_hpack":"locked-h2-public-default",
            "failures":FAILURE_COUNT, "failure_bytes":FAILURE_BYTES,
            "failure_digest_positions":3,
            "covered_status_request_bytes":REQUEST_BYTES,
            "status_subscription_peer_cancel_digest_positions":1,
            "status_subscription_peer_cancel_policy":"authenticated-covered-request-response-capacity-none-actual-remote-cancel-only", "failure_digest_sha256_bytes":64,
            "overflow_policy":"fixed-first-cause-first-overflow-last-overflow-wholefailure",
            "accounting":"actor-owned-Vec-capacity-only-not-production-physical-envelope"
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum RootReplyMutation {
    ProfileTwo,
    ClientRowsFalse,
    FourBytePrefixOnly,
    ValidRowPrefixOnlySuffix,
}

/// No mutation is authorized by a candidate. The runner must independently
/// resolve this task as its installed root before calling `arm_exact`.
#[derive(Clone, Debug)]
pub struct RootReplyCandidate {
    pub read: RootResultRead,
    pub backend_index: usize,
    pub caller: FrontendProcessId,
    pub downstream_stream: u32,
    pub upstream_stream: u32,
    pub request_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RootReplyFaultObservation {
    pub active_listeners: usize,
    pub joined_listeners: u64,
    pub active_connections: usize,
    pub active_streams: usize,
    pub connection_positions: usize,
    pub stream_positions: usize,
    pub target_slots: usize,
    pub owned_buffer_bytes: usize,
    pub peak_owned_buffer_bytes: usize,
    pub claimed: u64,
    pub emitted_messages: u64,
    pub frozen_root_task: Option<String>,
    pub frozen_request: Option<serde_json::Value>,
    pub target_requests: u64,
    pub target_consumed_positive_requests: u64,
    pub joined_connections: u64,
    pub joined_children: u64,
    pub shutdown_joined: bool,
    pub failures: Vec<String>,
    pub failure_overflow: bool,
    pub first_failure: Option<FailureDigest>,
    pub first_overflow: Option<FailureDigest>,
    pub last_overflow: Option<FailureDigest>,
    pub overflow_failures: u64,
    pub non_target_peer_cancels: u64,
    pub status_subscription_peer_cancels: u64,
    pub last_status_subscription_peer_cancel: Option<FailureDigest>,
    pub preapplication_backend_indices: [Option<usize>; 3],
    pub preapplication_peer_closes: [u8; 3],
    pub target_attempts: u64,
    pub not_ready_observed: u64,
    pub not_ready_forwarded: u64,
    pub not_ready_replies: Vec<serde_json::Value>,
    pub target_attempt_requests: Vec<serde_json::Value>,
    pub original: Option<serde_json::Value>,
    pub mutated: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FailureDigest {
    pub class: &'static str,
    pub sha256: String,
    pub bytes: usize,
}
impl FailureDigest {
    fn new(error: &str) -> Self {
        let class = if error.contains("capture absolute deadline") {
            "capture-deadline"
        } else if error.contains("target attempt cap") {
            "target-attempt-cap"
        } else if error.starts_with("root actor") {
            "actor-lifecycle"
        } else if error.starts_with("root child") {
            "child-lifecycle"
        } else if error.starts_with("root cleanup") {
            "cleanup"
        } else if error.starts_with("root connection") {
            "connection-or-stream"
        } else {
            "actor-refusal"
        };
        Self {
            class,
            sha256: hash(error.as_bytes()),
            bytes: error.len(),
        }
    }
}
struct Slot {
    candidate: RootReplyCandidate,
}
#[derive(Default)]
struct State {
    used_capture: bool,
    lifecycle_closed: bool,
    preapplication_closes: PreapplicationCloseLedger,
    deadline: Option<Instant>,
    slot: Option<Slot>,
    pending_capture: bool,
    claimed: u64,
    emitted: u64,
    frozen_root: Option<(TaskIdentity, FrontendProcessId)>,
    frozen_request: Option<serde_json::Value>,
    frozen_backend_index: Option<usize>,
    armed_mutation: Option<RootReplyMutation>,
    target_attempts: u64,
    non_target_peer_cancels: u64,
    status_subscription_peer_cancels: u64,
    last_status_subscription_peer_cancel: Option<FailureDigest>,
    not_ready_observed: u64,
    not_ready_forwarded: u64,
    not_ready_replies: Vec<serde_json::Value>,
    target_attempt_requests: Vec<serde_json::Value>,
    target_requests: u64,
    target_consumed_positive_requests: u64,
    shutdown_joined: bool,
    failures: Vec<String>,
    failure_overflow: bool,
    first_failure: Option<FailureDigest>,
    first_overflow: Option<FailureDigest>,
    last_overflow: Option<FailureDigest>,
    overflow_failures: u64,
    original: Option<serde_json::Value>,
    mutated: Option<serde_json::Value>,
}
struct Core {
    bounds: RootReplyFaultBounds,
    verifier: NativeServerAdmission,
    normal_subject: NativeCallerSubject,
    state: Mutex<State>,
    changed: Notify,
    stop: watch::Sender<bool>,
    listeners: AtomicUsize,
    joined_listeners: AtomicU64,
    connection_positions: Arc<Semaphore>,
    stream_positions: Arc<Semaphore>,
    connections: AtomicUsize,
    streams: AtomicUsize,
    bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    joined_connections: AtomicU64,
    joined_children: AtomicU64,
}

impl Core {
    fn failure(&self, error: &str) {
        let mut state = self.state.lock().expect("root fault state lock");
        let digest = FailureDigest::new(error);
        if state.first_failure.is_none() {
            state.first_failure = Some(digest.clone());
        }
        if error.len() > FAILURE_BYTES || state.failures.len() == FAILURE_COUNT {
            state.failure_overflow = true;
            state.overflow_failures = state.overflow_failures.saturating_add(1);
            if state.first_overflow.is_none() {
                state.first_overflow = Some(digest.clone());
            }
            state.last_overflow = Some(digest);
        } else {
            state.failures.push(error.to_owned());
        }
        // A diagnostic overflow is itself a failure; observations cannot pass
        // on the strength of a truncated or partially preserved failure set.
        state.deadline = None;
        self.changed.notify_waiters();
    }
}

/// Non-owning control. No secret, JWT or arbitrary response bytes are exposed.
#[derive(Clone)]
pub struct RootReplyFaultControl {
    core: Arc<Core>,
}

impl RootReplyFaultControl {
    pub fn begin_capture(&self, deadline: Instant) -> Result<()> {
        let now = Instant::now();
        ensure!(
            deadline > now
                && deadline.duration_since(now)
                    <= Duration::from_millis(self.core.bounds.maximum_capture_millis),
            "invalid absolute capture deadline"
        );
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            !state.used_capture && state.failures.is_empty() && !state.failure_overflow,
            "root fault actor cannot capture again or after failure"
        );
        state.used_capture = true;
        state.deadline = Some(deadline);
        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }

    pub fn candidate(&self) -> Option<RootReplyCandidate> {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .slot
            .as_ref()
            .map(|slot| slot.candidate.clone())
    }

    pub fn arm_exact(
        &self,
        root: TaskIdentity,
        caller: FrontendProcessId,
        backend_index: usize,
        mutation: RootReplyMutation,
    ) -> Result<()> {
        let mut state = self.core.state.lock().expect("root fault state lock");
        ensure!(
            state
                .deadline
                .is_some_and(|deadline| Instant::now() < deadline)
                && state.claimed == 0
                && state.failures.is_empty()
                && !state.failure_overflow,
            "root fault cannot arm outside its original capture deadline"
        );
        ensure!(
            state.armed_mutation.is_none(),
            "root mutation already armed"
        );
        let slot = state.slot.as_ref().context("no captured root candidate")?;
        ensure!(
            slot.candidate.read.root_task() == root
                && slot.candidate.caller == caller
                && slot.candidate.backend_index == backend_index,
            "independent root or frontend identity differs from candidate"
        );
        let frozen_request = serde_json::json!({"backend_index":slot.candidate.backend_index,
            "task":root.to_string(), "frontend_process":caller.to_string(),
            "downstream_h2_stream":slot.candidate.downstream_stream,
            "upstream_h2_stream":slot.candidate.upstream_stream,
            "request_sha256":slot.candidate.request_sha256});
        state.armed_mutation = Some(mutation);
        state.frozen_backend_index = Some(backend_index);
        state.frozen_request = Some(frozen_request);
        state.frozen_root = Some((root, caller));
        // The captured request is counted only after independent exact arm.
        state.target_requests = 1;
        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }

    pub fn disarm(&self) {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .deadline = None;
        self.core.changed.notify_waiters();
    }

    pub fn observation(&self) -> RootReplyFaultObservation {
        let state = self.core.state.lock().expect("root fault state lock");
        RootReplyFaultObservation {
            active_listeners: self.core.listeners.load(Ordering::Acquire),
            joined_listeners: self.core.joined_listeners.load(Ordering::Acquire),
            active_connections: self.core.connections.load(Ordering::Acquire),
            active_streams: self.core.streams.load(Ordering::Acquire),
            connection_positions: self.core.bounds.maximum_connections
                - self.core.connection_positions.available_permits(),
            stream_positions: self.core.bounds.maximum_active_streams
                - self.core.stream_positions.available_permits(),
            target_slots: usize::from(state.pending_capture || state.slot.is_some()),
            owned_buffer_bytes: self.core.bytes.load(Ordering::Acquire),
            peak_owned_buffer_bytes: self.core.peak_bytes.load(Ordering::Acquire),
            claimed: state.claimed,
            emitted_messages: state.emitted,
            frozen_root_task: state.frozen_root.map(|(root, _)| root.to_string()),
            frozen_request: state.frozen_request.clone(),
            target_requests: state.target_requests,
            target_consumed_positive_requests: state.target_consumed_positive_requests,
            joined_connections: self.core.joined_connections.load(Ordering::Acquire),
            joined_children: self.core.joined_children.load(Ordering::Acquire),
            shutdown_joined: state.shutdown_joined,
            failures: state.failures.clone(),
            failure_overflow: state.failure_overflow,
            first_failure: state.first_failure.clone(),
            first_overflow: state.first_overflow.clone(),
            last_overflow: state.last_overflow.clone(),
            overflow_failures: state.overflow_failures,
            non_target_peer_cancels: state.non_target_peer_cancels,
            status_subscription_peer_cancels: state.status_subscription_peer_cancels,
            last_status_subscription_peer_cancel: state
                .last_status_subscription_peer_cancel
                .clone(),
            preapplication_backend_indices: state.preapplication_closes.backend_indices,
            preapplication_peer_closes: state.preapplication_closes.closes,
            target_attempts: state.target_attempts,
            not_ready_observed: state.not_ready_observed,
            not_ready_forwarded: state.not_ready_forwarded,
            not_ready_replies: state.not_ready_replies.clone(),
            target_attempt_requests: state.target_attempt_requests.clone(),
            original: state.original.clone(),
            mutated: state.mutated.clone(),
        }
    }
}

pub(crate) struct RootReplyFaultProxy {
    address: SocketAddr,
    generation: watch::Sender<u64>,
    core: Arc<Core>,
    thread: Option<JoinHandle<()>>,
}

impl RootReplyFaultProxy {
    pub(crate) fn start(
        upstream: SocketAddr,
        backend_index: usize,
        shared: Option<&Self>,
        bounds: RootReplyFaultBounds,
        verifier: NativeServerAdmission,
        normal_subject: NativeCallerSubject,
    ) -> Result<Self> {
        bounds.validate()?;
        let listener = TcpListener::bind("127.0.0.1:0").context("bind root fault actor")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let (stop, _) = watch::channel(false);
        let (generation, _) = watch::channel(0);
        let core = if let Some(shared) = shared {
            shared.core.clone()
        } else {
            Arc::new(Core {
                connection_positions: Arc::new(Semaphore::new(bounds.maximum_connections)),
                stream_positions: Arc::new(Semaphore::new(bounds.maximum_active_streams)),
                bounds,
                verifier,
                normal_subject,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                stop,
                listeners: AtomicUsize::new(0),
                joined_listeners: AtomicU64::new(0),
                connections: AtomicUsize::new(0),
                streams: AtomicUsize::new(0),
                bytes: AtomicUsize::new(0),
                peak_bytes: AtomicUsize::new(0),
                joined_connections: AtomicU64::new(0),
                joined_children: AtomicU64::new(0),
            })
        };
        core.state
            .lock()
            .expect("root fault state lock")
            .preapplication_closes
            .register(backend_index)?;
        core.listeners.fetch_add(1, Ordering::AcqRel);
        core.state
            .lock()
            .expect("root fault state lock")
            .shutdown_joined = false;
        let thread_core = core.clone();
        let thread_generation = generation.clone();
        let thread = std::thread::Builder::new()
            .name("native-root-reply-fault".into())
            .spawn(move || {
                let outcome = (|| -> Result<()> {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    // serve_listener joins all connection owners before returning;
                    // each connection joins its stream tasks and upstream driver.
                    runtime.block_on(serve_listener(
                        listener,
                        upstream,
                        backend_index,
                        thread_generation,
                        thread_core.clone(),
                    ))
                })();
                if let Err(error) = outcome {
                    thread_core.failure(&format!("root actor exit: {error:#}"));
                }
                if thread_core.listeners.fetch_sub(1, Ordering::AcqRel) == 1 {
                    thread_core
                        .state
                        .lock()
                        .expect("root fault state lock")
                        .shutdown_joined = true;
                }
            })
            .inspect_err(|_error| {
                core.listeners.fetch_sub(1, Ordering::AcqRel);
                core.failure("root actor listener thread spawn failed");
            })?;
        Ok(Self {
            address,
            generation,
            core,
            thread: Some(thread),
        })
    }
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
    pub(crate) fn control(&self) -> RootReplyFaultControl {
        RootReplyFaultControl {
            core: self.core.clone(),
        }
    }
    pub(crate) fn disconnect_all(&self) {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .lifecycle_closed = true;
        self.control().disarm();
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    pub(crate) fn stop(&mut self) {
        self.core
            .state
            .lock()
            .expect("root fault state lock")
            .lifecycle_closed = true;
        self.control().disarm();
        self.core.stop.send_replace(true);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                self.core
                    .failure("root actor thread panicked before its join barrier");
            }
            self.core.joined_listeners.fetch_add(1, Ordering::AcqRel);
        }
    }
}
impl Drop for RootReplyFaultProxy {
    fn drop(&mut self) {
        self.stop();
    }
}

struct CountGuard {
    core: Arc<Core>,
    connection: bool,
}
impl CountGuard {
    fn new(core: Arc<Core>, connection: bool) -> Self {
        if connection {
            core.connections.fetch_add(1, Ordering::AcqRel);
        } else {
            core.streams.fetch_add(1, Ordering::AcqRel);
        }
        Self { core, connection }
    }
}
impl Drop for CountGuard {
    fn drop(&mut self) {
        if self.connection {
            self.core.connections.fetch_sub(1, Ordering::AcqRel);
        } else {
            self.core.streams.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// The credit survives every H2 DATA alias through Bytes::from_owner. It
/// returns after the Vec backing has been destroyed, never at send_data.
struct OwnedBuffer {
    bytes: Vec<u8>,
    _credit: ByteCredit,
}
impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct ByteCredit {
    core: Arc<Core>,
    bytes: usize,
}
impl ByteCredit {
    fn new(core: Arc<Core>, bytes: usize) -> Result<Self> {
        let previous = core
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= core.bounds.maximum_owned_buffer_bytes)
            })
            .map_err(|_| anyhow::anyhow!("root actor owned-buffer positions exhausted"))?;
        core.peak_bytes
            .fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(Self { core, bytes })
    }
}
impl Drop for ByteCredit {
    fn drop(&mut self) {
        self.core.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl OwnedBuffer {
    fn new(core: Arc<Core>, capacity: usize) -> Result<Self> {
        let mut credit = ByteCredit::new(core.clone(), capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .context("allocate bounded actor buffer")?;
        if bytes.capacity() > capacity {
            let mut extra = ByteCredit::new(core, bytes.capacity() - capacity)?;
            credit.bytes += extra.bytes;
            extra.bytes = 0;
            drop(extra);
        }
        Ok(Self {
            bytes,
            _credit: credit,
        })
    }
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            self.bytes
                .len()
                .checked_add(bytes.len())
                .is_some_and(|length| length <= self.bytes.capacity()),
            "root actor buffer exceeds fixed capacity"
        );
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

async fn serve_listener(
    listener: TcpListener,
    upstream: SocketAddr,
    backend_index: usize,
    generation: watch::Sender<u64>,
    core: Arc<Core>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let mut stop = core.stop.subscribe();
    if *stop.borrow() {
        return Ok(());
    }
    let mut connections = JoinSet::new();
    let outcome = async {
        loop {
            tokio::select! {
                _ = stop.changed() => break Ok::<_, anyhow::Error>(()),
                _ = capture_expiry(core.clone()) => {},
                joined = connections.join_next(), if !connections.is_empty() => {
                    core.joined_connections.fetch_add(1, Ordering::AcqRel);
                    if let Some(result) = joined {
                        match result {Ok(Ok(())) => {}, Ok(Err(error)) => core.failure(&format!("root connection: {error:#}")),
                            Err(error) => core.failure(&format!("root connection join: {error}"))}
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accept root fault actor")?;
                    let Ok(position) = core.connection_positions.clone().try_acquire_owned() else {
                        core.failure("root actor connection positions exhausted"); drop(stream); continue;
                    };
                    let child_core = core.clone();
                    let child_generation = generation.subscribe();
                    connections.spawn(async move {
                        let _position = position;
                        let _count = CountGuard::new(child_core.clone(), true);
                        serve_connection(stream, upstream, backend_index, child_generation, child_core).await
                    });
                }
            }
        }
    }.await;
    // Do not abort connection owners: each must first abort AND JOIN its own
    // descendants. A shared stop signal cancels their accept/hold operations.
    core.stop.send_replace(true);
    while let Some(joined) = connections.join_next().await {
        core.joined_connections.fetch_add(1, Ordering::AcqRel);
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => core.failure(&format!("root cleanup: {error:#}")),
            Err(error) => core.failure(&format!("root cleanup join: {error}")),
        }
    }
    outcome
}

async fn capture_expiry(core: Arc<Core>) {
    loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let deadline = core.state.lock().expect("root fault state lock").deadline;
        if let Some(deadline) = deadline {
            tokio::select! {
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    let expired = {
                        let mut state = core.state.lock().expect("root fault state lock");
                        if state.deadline == Some(deadline) && state.claimed == 0 {
                            state.deadline = None;
                            true
                        } else { false }
                    };
                    if expired {
                        core.failure("capture absolute deadline expired without mutated Data1");
                        return;
                    }
                }
            }
        } else {
            changed.await;
        }
    }
}

// Observe only public control framing. Header fields are accumulated as
// scalars; one fixed 8-byte unit validates settings/window/GOAWAY payloads.
// Never retain application, HPACK, authorization, PING or GOAWAY debug bytes.
#[derive(Default)]
struct IngressFrames {
    read_bytes: u64,
    preface_bytes: usize,
    preface_valid: bool,
    header_bytes: usize,
    header_length: usize,
    header_kind: u8,
    header_flags: u8,
    header_stream: u32,
    payload_remaining: usize,
    payload_unit_target: usize,
    unit: [u8; 8],
    unit_bytes: usize,
    frames: u64,
    completed_frames: u64,
    first_frame_non_ack_settings: bool,
    settings: u64,
    setting_entries: u64,
    window_updates: u64,
    pings: u64,
    goaways: u64,
    goaway_payloads_validated: u64,
    goaway_no_error_last_stream_zero: u64,
    last_goaway_stream_id: Option<u32>,
    last_goaway_error_code: Option<u32>,
    goaway_debug_bytes: u64,
    application_or_unknown_frame: bool,
}
impl std::fmt::Debug for IngressFrames {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngressFrames")
            .field("read_bytes", &self.read_bytes)
            .field("preface_bytes", &self.preface_bytes)
            .field("preface_valid", &self.preface_valid)
            .field("header_bytes", &self.header_bytes)
            .field("payload_remaining", &self.payload_remaining)
            .field("frames", &self.frames)
            .field("completed_frames", &self.completed_frames)
            .field(
                "first_frame_non_ack_settings",
                &self.first_frame_non_ack_settings,
            )
            .field("settings", &self.settings)
            .field("setting_entries", &self.setting_entries)
            .field("window_updates", &self.window_updates)
            .field("pings", &self.pings)
            .field("goaways", &self.goaways)
            .field("goaway_payloads_validated", &self.goaway_payloads_validated)
            .field(
                "goaway_no_error_last_stream_zero",
                &self.goaway_no_error_last_stream_zero,
            )
            .field("last_goaway_stream_id", &self.last_goaway_stream_id)
            .field("last_goaway_error_code", &self.last_goaway_error_code)
            .field("goaway_debug_bytes", &self.goaway_debug_bytes)
            .field(
                "application_or_unknown_frame",
                &self.application_or_unknown_frame,
            )
            .finish()
    }
}
impl IngressFrames {
    fn new() -> Self {
        Self {
            preface_valid: true,
            ..Self::default()
        }
    }
    fn count(counter: &mut u64) -> bool {
        if let Some(next) = counter.checked_add(1) {
            *counter = next;
            true
        } else {
            false
        }
    }
    fn complete_control_sequence(&self) -> bool {
        self.preface_valid
            && self.preface_bytes == 24
            && self.header_bytes == 0
            && self.payload_remaining == 0
            && self.unit_bytes == 0
            && self.frames > 0
            && self.completed_frames == self.frames
            && self.first_frame_non_ack_settings
            && !self.application_or_unknown_frame
            && self.goaway_payloads_validated == self.goaways
            && self.goaway_no_error_last_stream_zero == self.goaways
    }
    fn observe(&mut self, mut bytes: &[u8]) {
        const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        match self.read_bytes.checked_add(bytes.len() as u64) {
            Some(total) => self.read_bytes = total,
            None => self.application_or_unknown_frame = true,
        }
        if self.preface_bytes < PREFACE.len() {
            let take = bytes.len().min(PREFACE.len() - self.preface_bytes);
            self.preface_valid &=
                bytes[..take] == PREFACE[self.preface_bytes..self.preface_bytes + take];
            self.preface_bytes += take;
            bytes = &bytes[take..];
            if !self.preface_valid {
                self.application_or_unknown_frame = true;
            }
        }
        while !bytes.is_empty() {
            if self.payload_remaining != 0 {
                let take = if self.payload_unit_target != 0 {
                    bytes
                        .len()
                        .min(self.payload_remaining)
                        .min(self.payload_unit_target - self.unit_bytes)
                } else {
                    bytes.len().min(self.payload_remaining)
                };
                if self.payload_unit_target != 0 {
                    self.unit[self.unit_bytes..self.unit_bytes + take]
                        .copy_from_slice(&bytes[..take]);
                    self.unit_bytes += take;
                }
                self.payload_remaining -= take;
                bytes = &bytes[take..];
                if self.payload_unit_target != 0 && self.unit_bytes == self.payload_unit_target {
                    self.validate_unit();
                    self.unit.fill(0);
                    self.unit_bytes = 0;
                }
                if self.payload_remaining == 0 {
                    self.finish_frame();
                }
                continue;
            }
            let byte = bytes[0];
            bytes = &bytes[1..];
            match self.header_bytes {
                0 => self.header_length = (byte as usize) << 16,
                1 => self.header_length |= (byte as usize) << 8,
                2 => self.header_length |= byte as usize,
                3 => self.header_kind = byte,
                4 => self.header_flags = byte,
                5 => self.header_stream = byte as u32,
                _ => self.header_stream = (self.header_stream << 8) | byte as u32,
            }
            self.header_bytes += 1;
            if self.header_bytes == 9 {
                self.start_frame();
            }
        }
    }
    fn start_frame(&mut self) {
        let first = self.frames == 0;
        self.application_or_unknown_frame |= !Self::count(&mut self.frames);
        let stream = self.header_stream & 0x7fff_ffff;
        let length = self.header_length;
        let flags = self.header_flags;
        let legal_shape = stream == 0
            && length <= FRAME_BYTES
            && match self.header_kind {
                4 => (flags == 0 && length.is_multiple_of(6)) || (flags == 1 && length == 0),
                8 => flags == 0 && length == 4,
                6 => (flags == 0 || flags == 1) && length == 8,
                7 => flags == 0 && length >= 8,
                _ => false,
            };
        if first {
            self.first_frame_non_ack_settings = self.header_kind == 4 && flags == 0 && legal_shape;
        }
        self.application_or_unknown_frame |=
            !legal_shape || (first && !self.first_frame_non_ack_settings);
        self.payload_unit_target = 0;
        match self.header_kind {
            4 => {
                self.application_or_unknown_frame |= !Self::count(&mut self.settings);
            }
            8 => {
                self.application_or_unknown_frame |= !Self::count(&mut self.window_updates);
            }
            6 => {
                self.application_or_unknown_frame |= !Self::count(&mut self.pings);
            }
            7 => {
                self.application_or_unknown_frame |= !Self::count(&mut self.goaways);
            }
            _ => {}
        }
        // An ineligible frame cannot copy arbitrary bytes into control scratch.
        if legal_shape && self.preface_valid && !self.application_or_unknown_frame {
            self.payload_unit_target = match self.header_kind {
                4 if flags == 0 => 6,
                8 => 4,
                7 => 8,
                _ => 0,
            };
        }
        self.payload_remaining = length;
        if length == 0 {
            self.finish_frame();
        }
    }
    fn validate_unit(&mut self) {
        match self.header_kind {
            4 => {
                let id = u16::from_be_bytes([self.unit[0], self.unit[1]]);
                let value =
                    u32::from_be_bytes(self.unit[2..6].try_into().expect("six-byte setting unit"));
                // Matches the known setting domains in locked h2 0.4.12.
                // Unknown IDs may be legal HTTP/2 extensions, but are outside
                // this narrow candidate; do not pretend to validate them.
                let valid = match id {
                    1 | 3 | 6 => true,
                    2 | 8 => value <= 1,
                    4 => value <= 0x7fff_ffff,
                    5 => (16_384..=16_777_215).contains(&value),
                    _ => false,
                };
                self.application_or_unknown_frame |=
                    !valid || !Self::count(&mut self.setting_entries);
            }
            8 => {
                let increment =
                    u32::from_be_bytes(self.unit[..4].try_into().expect("four-byte window unit"))
                        & 0x7fff_ffff;
                self.application_or_unknown_frame |= increment == 0;
                self.payload_unit_target = 0;
            }
            7 => {
                let last =
                    u32::from_be_bytes(self.unit[..4].try_into().expect("GOAWAY last stream"))
                        & 0x7fff_ffff;
                let error =
                    u32::from_be_bytes(self.unit[4..8].try_into().expect("GOAWAY error code"));
                self.last_goaway_stream_id = Some(last);
                self.last_goaway_error_code = Some(error);
                self.application_or_unknown_frame |=
                    !Self::count(&mut self.goaway_payloads_validated);
                if last == 0 && error == 0 {
                    self.application_or_unknown_frame |=
                        !Self::count(&mut self.goaway_no_error_last_stream_zero);
                } else {
                    self.application_or_unknown_frame = true;
                }
                match self
                    .goaway_debug_bytes
                    .checked_add((self.header_length - 8) as u64)
                {
                    Some(total) => self.goaway_debug_bytes = total,
                    None => self.application_or_unknown_frame = true,
                }
                self.payload_unit_target = 0; // Discard all remaining debug data.
            }
            _ => self.application_or_unknown_frame = true,
        }
        if self.application_or_unknown_frame {
            self.payload_unit_target = 0;
        }
    }
    fn finish_frame(&mut self) {
        self.application_or_unknown_frame |= !Self::count(&mut self.completed_frames);
        self.header_bytes = 0;
        self.header_length = 0;
        self.header_kind = 0;
        self.header_flags = 0;
        self.header_stream = 0;
        self.payload_unit_target = 0;
        self.unit.fill(0);
        self.unit_bytes = 0;
    }
}

#[cfg(test)]
mod preapplication_framing_tests {
    use super::*;
    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let length = payload.len();
        let mut wire = vec![
            (length >> 16) as u8,
            (length >> 8) as u8,
            length as u8,
            kind,
            flags,
        ];
        wire.extend_from_slice(&stream.to_be_bytes());
        wire.extend_from_slice(payload);
        wire
    }
    fn wire(frames: &[Vec<u8>]) -> Vec<u8> {
        let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        for frame in frames {
            wire.extend_from_slice(frame);
        }
        wire
    }
    fn facts(bytes: &[u8]) -> IngressFrames {
        let mut facts = IngressFrames::new();
        facts.observe(bytes);
        facts
    }
    #[test]
    fn full_controls_and_goaway_facts_are_valid_across_every_byte_split() {
        let mut settings = Vec::new();
        for (id, value) in [
            (1u16, 4096u32),
            (2, 0),
            (3, 128),
            (4, 256 * 1024),
            (5, 16384),
            (6, 16384),
            (8, 0),
        ] {
            settings.extend_from_slice(&id.to_be_bytes());
            settings.extend_from_slice(&value.to_be_bytes());
        }
        let mut goaway = vec![0; 8];
        goaway.extend_from_slice(b"discarded-debug");
        let input = wire(&[
            frame(4, 0, 0, &settings),
            frame(4, 1, 0, &[]),
            frame(8, 0, 0, &1u32.to_be_bytes()),
            frame(6, 0, 0, b"12345678"),
            frame(6, 1, 0, b"87654321"),
            frame(7, 0, 0, &goaway),
        ]);
        for split in 0..=input.len() {
            let mut facts = IngressFrames::new();
            facts.observe(&input[..split]);
            facts.observe(&input[split..]);
            assert!(
                facts.complete_control_sequence(),
                "split {split}: {facts:?}"
            );
            assert_eq!(
                (
                    facts.frames,
                    facts.completed_frames,
                    facts.settings,
                    facts.setting_entries
                ),
                (6, 6, 2, 7)
            );
            assert_eq!(
                (
                    facts.goaways,
                    facts.goaway_payloads_validated,
                    facts.goaway_no_error_last_stream_zero
                ),
                (1, 1, 1)
            );
            assert_eq!(
                (facts.last_goaway_stream_id, facts.last_goaway_error_code),
                (Some(0), Some(0))
            );
            assert_eq!(facts.goaway_debug_bytes, 15);
            assert_eq!(facts.unit, [0; 8]);
            assert!(!format!("{facts:?}").contains("discarded-debug"));
        }
        let mut bytewise = IngressFrames::new();
        for byte in &input {
            bytewise.observe(&[*byte]);
        }
        assert!(bytewise.complete_control_sequence());
    }
    #[test]
    fn no_partial_preface_header_or_payload_can_be_a_candidate() {
        let input = wire(&[
            frame(4, 0, 0, &[0, 3, 0, 0, 0, 128]),
            frame(7, 0, 0, &[0; 8]),
        ]);
        for end in 0..=input.len() {
            let facts = facts(&input[..end]);
            // Only the completed SETTINGS boundary and full final GOAWAY are legal.
            let boundary = end == 24 + 9 + 6 || end == input.len();
            assert_eq!(
                facts.complete_control_sequence(),
                boundary,
                "prefix {end}: {facts:?}"
            );
        }
        let mut bad = input.clone();
        bad[0] = b'x';
        assert!(!facts(&bad).complete_control_sequence());
        assert!(!facts(&wire(&[])).complete_control_sequence());
        assert!(!facts(&wire(&[frame(4, 1, 0, &[])])).complete_control_sequence());
        assert!(!facts(&wire(&[frame(8, 0, 0, &1u32.to_be_bytes())])).complete_control_sequence());
    }
    #[test]
    fn malformed_shapes_known_setting_values_unknown_apps_and_goaway_are_ineligible() {
        let mut cases = vec![
            frame(4, 2, 0, &[]),
            frame(4, 1, 0, &[0; 6]),
            frame(4, 0, 0, &[0; 5]),
            frame(8, 0, 0, &[0; 4]),
            frame(8, 1, 0, &[0, 0, 0, 1]),
            frame(8, 0, 0, &[0; 3]),
            frame(6, 2, 0, &[0; 8]),
            frame(6, 0, 0, &[0; 7]),
            frame(7, 1, 0, &[0; 8]),
            frame(7, 0, 0, &[0; 7]),
            frame(7, 0, 0, &[0, 0, 0, 1, 0, 0, 0, 0]),
            frame(7, 0, 0, &[0, 0, 0, 0, 0, 0, 0, 1]),
            frame(1, 4, 1, b"not-retained-authorization"),
            frame(0, 0, 0, b"not-retained-body"),
            frame(99, 0, 0, &[]),
            frame(4, 0, 1, &[]),
            frame(6, 0, 1, &[0; 8]),
            frame(4, 0, 0, &vec![0; 16_386]),
        ];
        for (id, value) in [
            (2u16, 2u32),
            (8, 2),
            (4, 0x8000_0000),
            (5, 16_383),
            (5, 16_777_216),
            (999, 0),
        ] {
            let mut entry = id.to_be_bytes().to_vec();
            entry.extend_from_slice(&value.to_be_bytes());
            cases.push(frame(4, 0, 0, &entry));
        }
        for (ordinal, bad) in cases.into_iter().enumerate() {
            let facts = facts(&wire(&[frame(4, 0, 0, &[]), bad]));
            assert!(
                !facts.complete_control_sequence(),
                "case {ordinal}: {facts:?}"
            );
            assert_eq!(facts.unit, [0; 8]);
            assert!(!format!("{facts:?}").contains("not-retained"));
        }
    }
}
// Only completed pre-capture, no-RPC peer shutdowns can enter this ledger.
const PREAPPLICATION_CLOSES_PER_BACKEND: u8 = 3;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
enum ConnectionFailureStage {
    DownstreamAccept,
    DownstreamHandshake,
    UpstreamConnect,
    UpstreamHandshake,
    UpstreamDriver,
    ResponseCopy,
    Timeout,
    Shutdown,
    GenerationChange,
}
#[derive(Clone, Copy)]
struct PreapplicationExitFacts {
    stage: ConnectionFailureStage,
    accepted_rpcs: u64,
    unresolved_rpcs: u64,
    never_provisional_target: bool,
    all_children_joined: bool,
    cleanup_failed: bool,
}
// At most three explicitly enabled actor listeners, with arbitrary actual BE
// indexes. This is not a guessed BE0 placement or a process-identity authority.
#[derive(Default)]
struct PreapplicationCloseLedger {
    backend_indices: [Option<usize>; 3],
    closes: [u8; 3],
}
impl PreapplicationCloseLedger {
    #[cfg(test)]
    fn new(indices: &[usize]) -> Result<Self> {
        ensure!(
            !indices.is_empty() && indices.len() <= 3,
            "invalid candidate listener inventory"
        );
        let mut result = Self {
            backend_indices: [None; 3],
            closes: [0; 3],
        };
        for (slot, index) in indices.iter().copied().enumerate() {
            ensure!(
                !result.backend_indices[..slot].contains(&Some(index)),
                "duplicate candidate listener index"
            );
            result.backend_indices[slot] = Some(index);
        }
        Ok(result)
    }
    fn register(&mut self, backend_index: usize) -> Result<()> {
        ensure!(
            self.slot(backend_index).is_none(),
            "duplicate root fault listener index"
        );
        let slot = self
            .backend_indices
            .iter()
            .position(Option::is_none)
            .context("root reply fault supports at most three explicit listeners")?;
        self.backend_indices[slot] = Some(backend_index);
        Ok(())
    }
    fn slot(&self, backend_index: usize) -> Option<usize> {
        self.backend_indices
            .iter()
            .position(|index| *index == Some(backend_index))
    }
}
fn preapplication_close_candidate(
    state: &State,
    ledger: &PreapplicationCloseLedger,
    backend_index: usize,
    facts: PreapplicationExitFacts,
    frames: &IngressFrames,
    error: &anyhow::Error,
) -> bool {
    // Sticky used_capture is the phase fence. deadline None after expiry,
    // disarm, or a successful claim never returns this actor to bootstrap.
    !state.used_capture
        && !state.lifecycle_closed
        && state.deadline.is_none()
        && state.slot.is_none()
        && !state.pending_capture
        && state.claimed == 0
        && state.frozen_root.is_none()
        && state.armed_mutation.is_none()
        && state.target_attempts == 0
        && state.failures.is_empty()
        && !state.failure_overflow
        && facts.stage == ConnectionFailureStage::DownstreamAccept
        && facts.accepted_rpcs == 0
        && facts.unresolved_rpcs == 0
        && facts.never_provisional_target
        && facts.all_children_joined
        && !facts.cleanup_failed
        && frames.complete_control_sequence()
        // An inert readiness channel can disappear before GOAWAY is flushed.
        // If present, exactly one GOAWAY must retain the existing NO_ERROR /
        // last_stream_zero / complete-boundary validation and zero debug bytes.
        && frames.goaways <= 1
        && frames.goaway_debug_bytes == 0
        && ledger
            .slot(backend_index)
            .is_some_and(|slot| ledger.closes[slot] < PREAPPLICATION_CLOSES_PER_BACKEND)
        && error.downcast_ref::<h2::Error>().is_some_and(|error| {
            error.is_io()
                && !error.is_reset()
                && error.reason().is_none()
                && matches!(
                    error.get_io().map(std::io::Error::kind),
                    Some(std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe)
                )
        })
}
// The production path holds this same Core mutex through eligibility and
// increment, atomically with begin_capture. The helper tests each fence.
#[cfg(test)]
fn settle_preapplication_close_locked(
    state: &State,
    ledger: &mut PreapplicationCloseLedger,
    backend_index: usize,
    facts: PreapplicationExitFacts,
    frames: &IngressFrames,
    error: &anyhow::Error,
) -> bool {
    if !preapplication_close_candidate(state, ledger, backend_index, facts, frames, error) {
        return false;
    }
    let slot = ledger
        .slot(backend_index)
        .expect("validated candidate listener");
    ledger.closes[slot] += 1;
    true
}

#[cfg(test)]
mod preapplication_close_candidate_tests {
    use super::*;
    use std::io::ErrorKind;
    // Typed h2 IO errors are obtained from a real h2 server accept operation;
    // h2::Error::from(Reason::CANCEL) would not be a typed RST_STREAM cancel.
    struct FailingRead {
        bytes: std::io::Cursor<Vec<u8>>,
        kind: ErrorKind,
    }
    impl tokio::io::AsyncRead for FailingRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let position = this.bytes.position() as usize;
            if position == this.bytes.get_ref().len() {
                return std::task::Poll::Ready(Err(this.kind.into()));
            }
            let count = buf.remaining().min(this.bytes.get_ref().len() - position);
            buf.put_slice(&this.bytes.get_ref()[position..position + count]);
            this.bytes.set_position((position + count) as u64);
            std::task::Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for FailingRead {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    fn controls() -> IngressFrames {
        let mut frames = IngressFrames::new();
        frames.observe(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        frames.observe(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
        frames.observe(&[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        frames
    }
    fn exit() -> PreapplicationExitFacts {
        PreapplicationExitFacts {
            stage: ConnectionFailureStage::DownstreamAccept,
            accepted_rpcs: 0,
            unresolved_rpcs: 0,
            never_provisional_target: true,
            all_children_joined: true,
            cleanup_failed: false,
        }
    }
    async fn typed_accept_error(kind: ErrorKind) -> anyhow::Error {
        let mut bytes = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
        typed_accept_error_with_wire(kind, bytes).await
    }
    async fn typed_accept_error_with_wire(kind: ErrorKind, bytes: Vec<u8>) -> anyhow::Error {
        let io = FailingRead {
            bytes: std::io::Cursor::new(bytes),
            kind,
        };
        let mut connection = server::Builder::new()
            .handshake::<_, Bytes>(io)
            .await
            .expect("complete preface handshake");
        let accepted = tokio::time::timeout(Duration::from_millis(500), connection.accept())
            .await
            .expect("finite typed accept");
        let error = match accepted {
            Some(Err(error)) => error,
            _ => panic!("expected typed H2 accept IO error"),
        };
        assert!(error.is_io());
        assert_eq!(error.get_io().map(std::io::Error::kind), Some(kind));
        anyhow::Error::new(error).context("actual downstream accept stage")
    }
    // This is a legal 79-byte control-only shape matching the observed frame
    // counts. Values are valid fixture settings, not recovered peer settings.
    fn readiness_wire_without_goaway() -> Vec<u8> {
        let mut bytes = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        bytes.extend_from_slice(&[0, 0, 24, 4, 0, 0, 0, 0, 0]);
        for (id, value) in [(1u16, 4096u32), (2, 0), (4, 65535), (6, 16384)] {
            bytes.extend_from_slice(&id.to_be_bytes());
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes.extend_from_slice(&[0, 0, 0, 4, 1, 0, 0, 0, 0]);
        bytes.extend_from_slice(&[0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        bytes
    }
    #[tokio::test]
    async fn no_goaway_readiness_keeps_all_pre_capture_and_actual_exit_fences() {
        let bytes = readiness_wire_without_goaway();
        assert_eq!(bytes.len(), 79);
        for kind in [ErrorKind::ConnectionReset, ErrorKind::BrokenPipe] {
            let error = typed_accept_error_with_wire(kind, bytes.clone()).await;
            for cut in 0..=bytes.len() {
                let mut frames = IngressFrames::new();
                frames.observe(&bytes[..cut]);
                frames.observe(&bytes[cut..]);
                assert!(frames.complete_control_sequence());
                assert_eq!(
                    (
                        frames.frames,
                        frames.settings,
                        frames.setting_entries,
                        frames.window_updates,
                        frames.goaways
                    ),
                    (3, 2, 4, 1, 0)
                );
                let mut state = State::default();
                let mut ledger = PreapplicationCloseLedger::new(&[2, 7, 11]).unwrap();
                assert!(preapplication_close_candidate(
                    &state,
                    &ledger,
                    7,
                    exit(),
                    &frames,
                    &error
                ));
                state.used_capture = true;
                assert!(!preapplication_close_candidate(
                    &state,
                    &ledger,
                    7,
                    exit(),
                    &frames,
                    &error
                ));
                state.used_capture = false;
                state.lifecycle_closed = true;
                assert!(!preapplication_close_candidate(
                    &state,
                    &ledger,
                    7,
                    exit(),
                    &frames,
                    &error
                ));
                state.lifecycle_closed = false;
                for facts in [
                    PreapplicationExitFacts {
                        accepted_rpcs: 1,
                        ..exit()
                    },
                    PreapplicationExitFacts {
                        unresolved_rpcs: 1,
                        ..exit()
                    },
                    PreapplicationExitFacts {
                        never_provisional_target: false,
                        ..exit()
                    },
                    PreapplicationExitFacts {
                        all_children_joined: false,
                        ..exit()
                    },
                    PreapplicationExitFacts {
                        cleanup_failed: true,
                        ..exit()
                    },
                    PreapplicationExitFacts {
                        stage: ConnectionFailureStage::ResponseCopy,
                        ..exit()
                    },
                    PreapplicationExitFacts {
                        stage: ConnectionFailureStage::Timeout,
                        ..exit()
                    },
                ] {
                    assert!(!preapplication_close_candidate(
                        &state, &ledger, 7, facts, &frames, &error
                    ));
                }
                for _ in 0..3 {
                    assert!(settle_preapplication_close_locked(
                        &state,
                        &mut ledger,
                        7,
                        exit(),
                        &frames,
                        &error
                    ));
                }
                assert!(!settle_preapplication_close_locked(
                    &state,
                    &mut ledger,
                    7,
                    exit(),
                    &frames,
                    &error
                ));
                assert_eq!(ledger.closes, [0, 3, 0]);
            }
        }
        let error = typed_accept_error_with_wire(ErrorKind::ConnectionAborted, bytes.clone()).await;
        let mut frames = IngressFrames::new();
        frames.observe(&bytes);
        let ledger = PreapplicationCloseLedger::new(&[7]).unwrap();
        assert!(!preapplication_close_candidate(
            &State::default(),
            &ledger,
            7,
            exit(),
            &frames,
            &error
        ));
        for tail in [
            vec![0, 0],
            vec![0, 0, 0, 1, 4, 0, 0, 0, 1],
            vec![0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0],
            vec![0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
            vec![0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        ] {
            let mut rejected = IngressFrames::new();
            rejected.observe(&bytes);
            rejected.observe(&tail);
            assert!(!rejected.complete_control_sequence());
        }
    }
    #[tokio::test]
    async fn only_two_typed_accept_io_kinds_can_settle_three_closes_per_actual_backend() {
        for kind in [ErrorKind::ConnectionReset, ErrorKind::BrokenPipe] {
            let error = typed_accept_error(kind).await;
            let state = State::default();
            let mut ledger = PreapplicationCloseLedger::new(&[2, 7, 11]).unwrap();
            let frames = controls();
            for _ in 0..3 {
                assert!(settle_preapplication_close_locked(
                    &state,
                    &mut ledger,
                    7,
                    exit(),
                    &frames,
                    &error
                ));
            }
            assert!(!settle_preapplication_close_locked(
                &state,
                &mut ledger,
                7,
                exit(),
                &frames,
                &error
            ));
            assert!(settle_preapplication_close_locked(
                &state,
                &mut ledger,
                2,
                exit(),
                &frames,
                &error
            ));
            assert!(!settle_preapplication_close_locked(
                &state,
                &mut ledger,
                0,
                exit(),
                &frames,
                &error
            ));
            assert_eq!(ledger.closes, [1, 3, 0]);
        }
        let error = typed_accept_error(ErrorKind::ConnectionAborted).await;
        assert!(!preapplication_close_candidate(
            &State::default(),
            &PreapplicationCloseLedger::new(&[0]).unwrap(),
            0,
            exit(),
            &controls(),
            &error
        ));
    }
    #[tokio::test]
    async fn sticky_capture_join_cleanup_stage_and_rpc_evidence_are_mandatory() {
        let error = typed_accept_error(ErrorKind::ConnectionReset).await;
        let frames = controls();
        let mut ledger = PreapplicationCloseLedger::new(&[1]).unwrap();
        let mut state = State::default();
        assert!(preapplication_close_candidate(
            &state,
            &ledger,
            1,
            exit(),
            &frames,
            &error
        ));
        state.used_capture = true;
        state.deadline = None;
        assert!(!settle_preapplication_close_locked(
            &state,
            &mut ledger,
            1,
            exit(),
            &frames,
            &error
        ));
        state.used_capture = false;
        state.lifecycle_closed = true;
        assert!(!preapplication_close_candidate(
            &state,
            &ledger,
            1,
            exit(),
            &frames,
            &error
        ));
        state.lifecycle_closed = false;
        for stage in [
            ConnectionFailureStage::DownstreamHandshake,
            ConnectionFailureStage::UpstreamConnect,
            ConnectionFailureStage::UpstreamHandshake,
            ConnectionFailureStage::UpstreamDriver,
            ConnectionFailureStage::ResponseCopy,
            ConnectionFailureStage::Timeout,
            ConnectionFailureStage::Shutdown,
            ConnectionFailureStage::GenerationChange,
        ] {
            assert!(!preapplication_close_candidate(
                &state,
                &ledger,
                1,
                PreapplicationExitFacts { stage, ..exit() },
                &frames,
                &error
            ));
        }
        for facts in [
            PreapplicationExitFacts {
                accepted_rpcs: 1,
                ..exit()
            },
            PreapplicationExitFacts {
                unresolved_rpcs: 1,
                ..exit()
            },
            PreapplicationExitFacts {
                never_provisional_target: false,
                ..exit()
            },
            PreapplicationExitFacts {
                all_children_joined: false,
                ..exit()
            },
            PreapplicationExitFacts {
                cleanup_failed: true,
                ..exit()
            },
        ] {
            assert!(!preapplication_close_candidate(
                &state, &ledger, 1, facts, &frames, &error
            ));
        }
        state.failures.push("cleanup failed".to_owned());
        assert!(!preapplication_close_candidate(
            &state,
            &ledger,
            1,
            exit(),
            &frames,
            &error
        ));
        state.failures.clear();
        state.failure_overflow = true;
        assert!(!preapplication_close_candidate(
            &state,
            &ledger,
            1,
            exit(),
            &frames,
            &error
        ));
        state.failure_overflow = false;
        for (goaways, debug_bytes) in [(2, 0), (1, 1)] {
            let mut changed = controls();
            changed.goaways = goaways;
            changed.goaway_payloads_validated = goaways;
            changed.goaway_no_error_last_stream_zero = goaways;
            changed.goaway_debug_bytes = debug_bytes;
            assert!(
                changed.complete_control_sequence(),
                "fixture must isolate the additional exact GOAWAY condition"
            );
            assert!(!preapplication_close_candidate(
                &state,
                &ledger,
                1,
                exit(),
                &changed,
                &error
            ));
        }
        assert_eq!(ledger.closes, [0, 0, 0]);
    }
    #[test]
    fn raw_io_strings_protocol_reasons_partial_and_application_frames_never_qualify() {
        let state = State::default();
        let ledger = PreapplicationCloseLedger::new(&[0]).unwrap();
        let frames = controls();
        for error in [
            anyhow::Error::new(std::io::Error::from(ErrorKind::ConnectionReset)),
            anyhow::anyhow!("root downstream accept ConnectionReset"),
            anyhow::Error::new(h2::Error::from(h2::Reason::PROTOCOL_ERROR)),
            anyhow::Error::new(h2::Error::from(h2::Reason::CANCEL)),
            anyhow::anyhow!("root output capacity closed reset=CANCEL"),
        ] {
            assert!(!preapplication_close_candidate(
                &state,
                &ledger,
                0,
                exit(),
                &frames,
                &error
            ));
        }
        // All parser failure facts remain sticky even if the tail is legal.
        let mut bad = controls();
        bad.observe(&[0, 0, 0, 1, 4, 0, 0, 0, 1]);
        assert!(!bad.complete_control_sequence());
        let mut partial = controls();
        partial.observe(&[0, 0]);
        assert!(!partial.complete_control_sequence());
    }
}

struct ObservedIngress {
    stream: tokio::net::TcpStream,
    frames: Arc<Mutex<IngressFrames>>,
}
impl tokio::io::AsyncRead for ObservedIngress {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut this.stream).poll_read(cx, buf);
        if buf.filled().len() != before {
            this.frames
                .lock()
                .expect("root ingress facts lock")
                .observe(&buf.filled()[before..]);
        }
        result
    }
}
impl tokio::io::AsyncWrite for ObservedIngress {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_write(cx, bytes)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

enum ChildExit {
    Upstream,
    Stream,
}
async fn serve_connection(
    stream: tokio::net::TcpStream,
    upstream: SocketAddr,
    backend_index: usize,
    mut generation: watch::Receiver<u64>,
    core: Arc<Core>,
) -> Result<()> {
    let mut stop = core.stop.subscribe();
    if *stop.borrow() {
        return Ok(());
    }
    let handshake_deadline =
        tokio::time::Instant::now() + Duration::from_millis(core.bounds.handshake_timeout_millis);
    let downstream_frames = Arc::new(Mutex::new(IngressFrames::new()));
    let observed_stream = ObservedIngress {
        stream,
        frames: downstream_frames.clone(),
    };
    let connect = async {
        let mut server_builder = server::Builder::new();
        server_builder
            .initial_window_size(H2_WINDOW)
            .initial_connection_window_size(H2_CONNECTION_WINDOW)
            .max_frame_size(FRAME_BYTES as u32)
            .max_header_list_size(HEADER_BYTES)
            .max_concurrent_streams(H2_STREAMS)
            .max_pending_accept_reset_streams(H2_RESETS)
            .max_concurrent_reset_streams(H2_RESETS)
            .max_local_error_reset_streams(Some(H2_RESETS))
            .max_send_buffer_size(H2_SEND_BUFFER);
        let downstream = server_builder
            .handshake::<_, Bytes>(observed_stream)
            .await
            .with_context(|| {
                format!(
                    "root downstream H2 handshake (frames={:?})",
                    downstream_frames.lock().expect("root ingress facts lock")
                )
            })?;
        let socket = tokio::net::TcpStream::connect(upstream)
            .await
            .context("root upstream TCP connect")?;
        let mut client_builder = client::Builder::new();
        client_builder
            .initial_window_size(H2_WINDOW)
            .initial_connection_window_size(H2_CONNECTION_WINDOW)
            .max_frame_size(FRAME_BYTES as u32)
            .max_header_list_size(HEADER_BYTES)
            .max_concurrent_streams(H2_STREAMS)
            .initial_max_send_streams(H2_STREAMS as usize)
            .max_pending_accept_reset_streams(H2_RESETS)
            .max_concurrent_reset_streams(H2_RESETS)
            .max_local_error_reset_streams(Some(H2_RESETS))
            .max_send_buffer_size(H2_SEND_BUFFER)
            .enable_push(false)
            .header_table_size(4096);
        let (sender, driver) = client_builder
            .handshake::<_, Bytes>(socket)
            .await
            .context("root upstream H2 handshake")?;
        Ok::<_, anyhow::Error>((downstream, sender, driver))
    };
    let (mut downstream, sender, driver) = tokio::select! {
        _ = stop.changed() => return Ok(()),
        _ = generation.changed() => return Ok(()),
        result = tokio::time::timeout_at(handshake_deadline, connect) => result.context("root actor handshake deadline")??,
    };
    let mut children = JoinSet::new();
    children.spawn(async move {
        driver.await.context("root upstream H2 driver")?;
        Ok::<_, anyhow::Error>(ChildExit::Upstream)
    });
    let mut accepted_rpcs = 0u64;
    let mut failed_at_downstream_accept = false;
    let mut unresolved_rpcs_at_failure = 0;
    let outcome = async {
        loop {
            tokio::select! {
                _ = stop.changed() => break Ok::<_,anyhow::Error>(()),
                _ = generation.changed() => break Ok(()),
                joined = children.join_next(), if !children.is_empty() => {
                    core.joined_children.fetch_add(1, Ordering::AcqRel);
                    match joined {
                        Some(Ok(Ok(ChildExit::Stream))) => {},
                        Some(Ok(Ok(ChildExit::Upstream))) | None => break Ok(()),
                        Some(Ok(Err(error))) => break Err(error),
                        Some(Err(error)) => break Err(anyhow::anyhow!("root child join: {error}")),
                    }
                }
                accepted = downstream.accept() => {
                    let Some((request,mut response)) = accepted.transpose()
                        .map_err(|error| {
                            failed_at_downstream_accept = true;
                            // With zero accepted RPCs the sole child is the
                            // upstream driver, never an application stream.
                            unresolved_rpcs_at_failure = children.len().saturating_sub(1) as u64;
                            let context = format!("root downstream H2 accept (accepted_rpcs={}, children={}, frames={:?}, io={}, io_kind={:?}, raw_os_error={:?}, reset={}, remote={}, reason={:?})",
                                accepted_rpcs, children.len(), downstream_frames.lock().expect("root ingress facts lock"),
                                error.is_io(), error.get_io().map(std::io::Error::kind), error.get_io().and_then(std::io::Error::raw_os_error),
                                error.is_reset(), error.is_remote(), error.reason());
                            anyhow::Error::new(error).context(context)
                        })?
                        else {break Ok(())};
                    accepted_rpcs = accepted_rpcs.saturating_add(1);
                    let Ok(position) = core.stream_positions.clone().try_acquire_owned() else {
                        response.send_reset(h2::Reason::REFUSED_STREAM);
                        core.failure("root actor stream positions exhausted"); continue;
                    };
                    let child_core = core.clone();
                    let child_sender = sender.clone();
                    children.spawn(async move {
                        let _position = position;
                        let _count = CountGuard::new(child_core.clone(), false);
                        forward_stream(request, response, child_sender, backend_index, child_core).await?;
                        Ok(ChildExit::Stream)
                    });
                }
            }
        }
    }
    .await;
    // Abort is only harness shutdown/failure cleanup, never a production task
    // terminal. Await every original child JoinHandle before returning.
    let mut cleanup_failed = false;
    children.abort_all();
    while let Some(joined) = children.join_next().await {
        core.joined_children.fetch_add(1, Ordering::AcqRel);
        match joined {
            Ok(Err(error)) => {
                cleanup_failed = true;
                core.failure(&format!("root child cleanup failure: {error:#}"));
            }
            Err(error) if !error.is_cancelled() => {
                cleanup_failed = true;
                core.failure(&format!("root child cleanup join: {error}"))
            }
            Ok(Ok(_)) | Err(_) => {}
        }
    }
    drop(sender);
    drop(downstream);
    if failed_at_downstream_accept && let Err(error) = &outcome {
        let frames = downstream_frames.lock().expect("root ingress facts lock");
        let mut state = core.state.lock().expect("root fault state lock");
        let facts = PreapplicationExitFacts {
            stage: ConnectionFailureStage::DownstreamAccept,
            accepted_rpcs,
            unresolved_rpcs: unresolved_rpcs_at_failure,
            never_provisional_target: accepted_rpcs == 0,
            all_children_joined: children.is_empty(),
            cleanup_failed,
        };
        if preapplication_close_candidate(
            &state,
            &state.preapplication_closes,
            backend_index,
            facts,
            &frames,
            error,
        ) {
            let slot = state
                .preapplication_closes
                .slot(backend_index)
                .expect("validated actual root fault listener");
            state.preapplication_closes.closes[slot] += 1;
            return Ok(());
        }
    }
    outcome
}

// Classification is monotonic within one RPC. A provisional capture remains
// protected even after its SlotExit drops; an unknown identity is never exempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamClass {
    Unknown,
    NonTarget,
    Target,
    ProvisionalCapture,
    CoveredStatusSubscription,
}

fn classify_frozen_read(
    state: &State,
    root: TaskIdentity,
    caller: FrontendProcessId,
) -> StreamClass {
    match (state.frozen_root, state.frozen_backend_index) {
        (Some(frozen), Some(_)) if frozen == (root, caller) => StreamClass::Target,
        (Some(_), Some(_)) => StreamClass::NonTarget,
        _ => StreamClass::Unknown,
    }
}

fn harmless_non_target_cancel(class: StreamClass, error: &anyhow::Error) -> bool {
    class == StreamClass::NonTarget
        && error.downcast_ref::<h2::Error>().is_some_and(|error| {
            error.is_reset() && error.is_remote() && error.reason() == Some(h2::Reason::CANCEL)
        })
}

fn authenticated_normal_frontend<T>(
    request: &Request<T>,
    core: &Core,
) -> Option<FrontendProcessId> {
    if request.method() != http::Method::POST
        || !matches!(request.uri().path(), ROOT_PATH | COVERED_STATUS_PATH)
    {
        return None;
    }
    let caller = core.verifier.admit_headers(request.headers()).ok()?;
    if caller.subject() != &core.normal_subject {
        return None;
    }
    match caller.process_identity() {
        Some(NativeProcessIdentity::Frontend(id)) => Some(id),
        _ => None,
    }
}

fn validate_covered_status(body: &Bytes, caller: FrontendProcessId) -> Result<()> {
    let wire = wire::SubscribeTaskStatusRequest::decode(single_message(body)?)
        .context("covered status request protobuf")?;
    let decoded = decode_covered_subscribe_task_status(
        &wire,
        FieldPath::root("root_fault_covered_status_request"),
    )?;
    ensure!(
        decoded.context.frontend_process_id() == caller,
        "covered status context differs from authenticated frontend process"
    );
    Ok(())
}

// Only send_bytes' poll_capacity(None) constructs this private cause. Neither
// send_data errors nor reset Reasons nor matching error text can construct it.
#[derive(Debug)]
struct CapacityClosed {
    stream: u32,
    reset: CapacityResetObservation,
}
#[derive(Debug)]
enum CapacityResetObservation {
    Pending,
    Reason(u32),
    Error(FailureDigest),
}
impl std::fmt::Display for CapacityClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "root output capacity closed (stream={}, reset_observation=",
            self.stream
        )?;
        match &self.reset {
            CapacityResetObservation::Pending => f.write_str("Pending")?,
            CapacityResetObservation::Reason(reason) => write!(f, "Reason({reason})")?,
            CapacityResetObservation::Error(digest) => write!(
                f,
                "Error(class={}, sha256={}, bytes={})",
                digest.class, digest.sha256, digest.bytes
            )?,
        }
        f.write_str(")")
    }
}
impl std::error::Error for CapacityClosed {}

fn digest_display(class: &'static str, value: &impl std::fmt::Display) -> FailureDigest {
    struct Writer {
        sha: Sha256,
        bytes: usize,
    }
    impl std::fmt::Write for Writer {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.sha.update(s.as_bytes());
            self.bytes = self.bytes.saturating_add(s.len());
            Ok(())
        }
    }
    let mut writer = Writer {
        sha: Sha256::new(),
        bytes: 0,
    };
    let _ = std::fmt::write(&mut writer, format_args!("{value}"));
    FailureDigest {
        class,
        bytes: writer.bytes,
        sha256: format!("{:x}", writer.sha.finalize()),
    }
}

async fn recover_covered_status_cancel(
    class: StreamClass,
    failure: &anyhow::Error,
    inbound: &mut RecvStream,
    core: &Core,
) -> bool {
    let Some(closed) = failure
        .downcast_ref::<CapacityClosed>()
        .filter(|closed| closed.stream == inbound.stream_id().as_u32())
    else {
        return false;
    };
    if class != StreamClass::CoveredStatusSubscription {
        return false;
    }
    // One nonblocking poll, using the same request stream after its EOF/trailers.
    // No diagnostic write, additional IO wait, retry or synthesized H2 error.
    let observed = poll_fn(|cx| std::task::Poll::Ready(inbound.poll_data(cx))).await;
    let digest = match observed {
        // A reset received before the request reached END_STREAM stays typed on
        // the inbound half.
        std::task::Poll::Ready(Some(Err(error))) => {
            if !(error.is_reset()
                && error.is_remote()
                && error.reason() == Some(h2::Reason::CANCEL))
            {
                return false;
            }
            digest_display("covered-status-subscription-remote-cancel", &error)
        }
        // h2 0.4.16+ keeps a reset received after END_STREAM for the send half
        // only, so the inbound half reports a clean EOF. This actor never resets a
        // forwarded stream with CANCEL, so an output-side CANCEL is the peer's.
        // The public h2 API cannot separate it from a GOAWAY carrying CANCEL.
        std::task::Poll::Ready(None) => {
            if !matches!(
                closed.reset,
                CapacityResetObservation::Reason(reason) if reason == u32::from(h2::Reason::CANCEL)
            ) {
                return false;
            }
            digest_display("covered-status-subscription-remote-cancel", closed)
        }
        _ => return false,
    };
    let mut state = core.state.lock().expect("root fault state lock");
    state.status_subscription_peer_cancels =
        state.status_subscription_peer_cancels.saturating_add(1);
    state.last_status_subscription_peer_cancel = Some(digest);
    true
}

async fn forward_stream(
    request: Request<RecvStream>,
    mut response: server::SendResponse<Bytes>,
    sender: client::SendRequest<Bytes>,
    backend_index: usize,
    core: Arc<Core>,
) -> Result<()> {
    let rpc_path = request.uri().path();
    let rpc_label = if rpc_path.len() <= 128 {
        rpc_path.to_owned()
    } else {
        format!("oversize-path-sha256={}", hash(rpc_path.as_bytes()))
    };
    let downstream_stream = response.stream_id().as_u32();
    let mut stream_class = StreamClass::Unknown;
    let normal_caller = authenticated_normal_frontend(&request, &core);
    let normal = normal_caller.filter(|_| request.uri().path() == ROOT_PATH);
    let covered_status_caller =
        normal_caller.filter(|_| request.uri().path() == COVERED_STATUS_PATH);
    let capture = normal.and_then(|caller| {
        let state = core.state.lock().expect("root fault state lock");
        state.deadline.map(|deadline| (caller, deadline))
    });
    let inspect = normal.is_some() && {
        let state = core.state.lock().expect("root fault state lock");
        state.deadline.is_some() || state.frozen_root.is_some()
    };
    let deadline = capture.map(|(_, deadline)| deadline).unwrap_or_else(|| {
        Instant::now() + Duration::from_millis(core.bounds.forward_timeout_millis)
    });
    let work = async {
        let (parts, mut inbound) = request.into_parts();
        let mut sender = sender
            .ready()
            .await
            .context("ready root upstream request")?;
        let saved_request = if inspect {
            require_uncompressed(&parts.headers)?;
            let (body, trailers) = read_bounded(&mut inbound, REQUEST_BYTES, core.clone()).await?;
            ensure!(
                trailers.as_ref().is_none_or(HeaderMap::is_empty),
                "captured root request has trailers"
            );
            let message = single_message(&body)?;
            let wire = wire::FetchRootResultRequest::decode(message)
                .context("captured root request protobuf")?;
            let read = decode_read(&wire, FieldPath::root("root_fault_request"))?;
            {
                let mut state = core.state.lock().expect("root fault state lock");
                if let Some(caller) = normal {
                    stream_class = classify_frozen_read(&state, read.root_task(), caller);
                }
                if state.frozen_root == normal.map(|caller| (read.root_task(), caller)) {
                    state.target_requests += 1;
                    state.target_consumed_positive_requests += u64::from(read.consumed() > 0);
                }
                // Observation never rejects ACKs. A healthy query with another
                // exact TaskIdentity is excluded from these counters.
            }
            Some((body, read))
        } else {
            None
        };
        // This path consumes the bounded request once, but retains its original
        // RecvStream until response forwarding exits. No new buffer or clock.
        let saved_status = if let Some(caller) = covered_status_caller {
            require_uncompressed(&parts.headers)?;
            let (body, trailers) = read_bounded(&mut inbound, REQUEST_BYTES, core.clone()).await?;
            ensure!(
                trailers.as_ref().is_none_or(HeaderMap::is_empty),
                "covered status request has trailers"
            );
            validate_covered_status(&body, caller)?;
            stream_class = StreamClass::CoveredStatusSubscription;
            Some(body)
        } else {
            None
        };
        // FE may start its next exact read as soon as queued NotReady trailers
        // arrive, before the previous worker drops its slot guard. That read
        // waits under the same absolute clock instead of bypassing injection.
        if let (Some((caller, _)), Some((_, read))) = (capture, saved_request.as_ref()) {
            loop {
                let changed = core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let wait_for_exact_slot = {
                    let state = core.state.lock().expect("root fault state lock");
                    let exact = state.frozen_root == Some((read.root_task(), caller))
                        && state.frozen_backend_index == Some(backend_index);
                    if exact && state.claimed == 0 {
                        ensure!(
                            state.deadline == Some(deadline)
                                && state.first_failure.is_none()
                                && !state.failure_overflow,
                            "exact root phase failed while awaiting its prior RPC slot"
                        );
                    }
                    exact && state.claimed == 0 && (state.pending_capture || state.slot.is_some())
                };
                if !wait_for_exact_slot {
                    break;
                }
                changed.await;
            }
        }
        // A single candidate is published, but only independent controller
        // arm authorizes mutation. Other calls retain bounded normal forwarding.
        let selected_capture = capture.filter(|(caller, _)| {
            let state = core.state.lock().expect("root fault state lock");
            if state.claimed != 0 || state.pending_capture || state.slot.is_some() {
                return false;
            }
            let Some((_, read)) = saved_request.as_ref() else {
                return false;
            };
            match state.frozen_root {
                Some(exact) => {
                    exact == (read.root_task(), *caller)
                        && state.frozen_backend_index == Some(backend_index)
                }
                None => state.target_attempts == 0,
            }
        });
        if selected_capture.is_some() {
            stream_class = StreamClass::ProvisionalCapture;
            let read = &saved_request.as_ref().context("candidate body absent")?.1;
            ensure!(
                read.profile() == RootProfileId::V1
                    && read.kind() == RootOutputKind::ClientRows
                    && read.wanted().map(|n| n.get()) == Some(1)
                    && read.consumed() == 0,
                "capture requires exact ClientRows V1 wanted1 consumed0 request"
            );
        }
        let _capture_guard = if selected_capture.is_some() {
            let mut state = core.state.lock().expect("root fault state lock");
            ensure!(
                !state.pending_capture
                    && state.slot.is_none()
                    && state.claimed == 0
                    && state.deadline == Some(deadline)
                    && Instant::now() < deadline,
                "root capture lost its unique slot or original phase"
            );
            ensure!(
                state.target_attempts < MAX_TARGET_ATTEMPTS,
                "target attempt cap exhausted before forwarding another request"
            );
            state.target_attempts += 1;
            state.pending_capture = true;
            Some(SlotExit(core.clone()))
        } else {
            None
        };
        let (upstream_response, mut upstream_send) =
            sender.send_request(Request::from_parts(parts, ()), false)?;
        let upstream_id = upstream_send.stream_id().as_u32();
        let downstream_id = response.stream_id().as_u32();
        if let Some((caller, _)) = selected_capture {
            let (body, read) = saved_request.context("captured request body is absent")?;
            {
                let mut state = core.state.lock().expect("root fault state lock");
                ensure!(
                    state.pending_capture
                        && state.slot.is_none()
                        && state.claimed == 0
                        && state.deadline == Some(deadline),
                    "root capture has multiple candidates or lost its exact phase"
                );
                let evidence = serde_json::json!({"attempt":state.target_attempts,
                    "backend_index":backend_index,"task":read.root_task().to_string(),
                    "frontend_process":caller.to_string(),"downstream_h2_stream":downstream_id,
                    "upstream_h2_stream":upstream_id,"request_sha256":hash(&body),
                    "wanted":read.wanted().map(|n| n.get()),"consumed":read.consumed()});
                state.target_attempt_requests.push(evidence);
                state.pending_capture = false;
                state.slot = Some(Slot {
                    candidate: RootReplyCandidate {
                        read: read.clone(),
                        backend_index,
                        caller,
                        downstream_stream: downstream_id,
                        upstream_stream: upstream_id,
                        request_sha256: hash(&body),
                    },
                });
            }
            core.changed.notify_waiters();
            send_bytes(&mut upstream_send, body).await?;
            upstream_send.send_data(Bytes::new(), true)?;
            let upstream_response = upstream_response.await?;
            inject_reply(upstream_response, response, read, deadline, core.clone()).await
        } else if let Some(body) = saved_status {
            let request_forward = async {
                send_bytes(&mut upstream_send, body).await?;
                upstream_send.send_data(Bytes::new(), true)?;
                Ok::<_, anyhow::Error>(())
            };
            let response_forward = async {
                let upstream_response = upstream_response.await?;
                let (parts, body) = upstream_response.into_parts();
                let output = response.send_response(Response::from_parts(parts, ()), false)?;
                match copy_body(body, output, core.clone()).await {
                    Err(error) => {
                        if recover_covered_status_cancel(stream_class, &error, &mut inbound, &core)
                            .await
                        {
                            Ok(())
                        } else {
                            Err(error).context("root downstream response copy")
                        }
                    }
                    Ok(()) => Ok(()),
                }
            };
            tokio::try_join!(request_forward, response_forward)?;
            Ok(())
        } else {
            let request_forward = async {
                if let Some((body, _read)) = saved_request {
                    send_bytes(&mut upstream_send, body).await?;
                    upstream_send.send_data(Bytes::new(), true)?;
                    Ok::<_, anyhow::Error>(())
                } else {
                    copy_body(inbound, upstream_send, core.clone())
                        .await
                        .context("root upstream request copy")
                }
            };
            tokio::try_join!(request_forward, async {
                let upstream_response = upstream_response.await?;
                let (parts, body) = upstream_response.into_parts();
                let output = response.send_response(Response::from_parts(parts, ()), false)?;
                copy_body(body, output, core.clone())
                    .await
                    .context("root downstream response copy")
            })?;
            Ok(())
        }
    };
    // A timeout drops the work future before classification. It never becomes
    // a cancellation exemption, and errors from driver/accept never enter here.
    let outcome = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
        .await
        .context("root stream absolute deadline")?;
    match outcome {
        Err(error) if harmless_non_target_cancel(stream_class, &error) => {
            let mut state = core.state.lock().expect("root fault state lock");
            state.non_target_peer_cancels = state.non_target_peer_cancels.saturating_add(1);
            Ok(())
        }
        outcome => outcome.with_context(|| format!("root RPC (path={rpc_label}, downstream_stream={downstream_stream}, authenticated_root={}, class={stream_class:?})", normal.is_some())),
    }
}

struct SlotExit(Arc<Core>);
impl Drop for SlotExit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("root fault state lock");
        state.slot = None;
        state.pending_capture = false;
        // A legal NotReady closes only this RPC slot. Its original phase clock
        // and independently frozen target/mutation remain for the next exact read.
        if state.claimed != 0 || !state.failures.is_empty() || state.failure_overflow {
            state.deadline = None;
        }
        self.0.changed.notify_waiters();
    }
}

async fn read_bounded(
    body: &mut RecvStream,
    cap: usize,
    core: Arc<Core>,
) -> Result<(Bytes, Option<HeaderMap>)> {
    let mut buffer = OwnedBuffer::new(core, cap)?;
    while let Some(bytes) = body.data().await {
        let bytes = bytes?;
        ensure!(
            bytes.len() <= FRAME_BYTES,
            "root actor received oversized DATA frame"
        );
        buffer.append(&bytes)?;
        body.flow_control().release_capacity(bytes.len())?;
    }
    let trailers = body.trailers().await?;
    Ok((buffer.into_bytes(), trailers))
}

async fn copy_body(
    mut input: RecvStream,
    mut output: SendStream<Bytes>,
    core: Arc<Core>,
) -> Result<()> {
    while let Some(bytes) = input.data().await {
        let bytes = bytes?;
        ensure!(
            bytes.len() <= FRAME_BYTES,
            "root forwarding received oversized DATA frame"
        );
        let mut copy = OwnedBuffer::new(core.clone(), FRAME_BYTES)?;
        copy.append(&bytes)?;
        input.flow_control().release_capacity(bytes.len())?;
        drop(bytes);
        send_bytes(&mut output, copy.into_bytes()).await?;
    }
    match input.trailers().await? {
        Some(trailers) => output.send_trailers(trailers)?,
        None => output.send_data(Bytes::new(), true)?,
    }
    Ok(())
}

async fn send_bytes(output: &mut SendStream<Bytes>, mut bytes: Bytes) -> Result<()> {
    while !bytes.is_empty() {
        output.reserve_capacity(bytes.len().min(FRAME_BYTES));
        let available = if output.capacity() > 0 {
            output.capacity()
        } else {
            match poll_fn(|cx| output.poll_capacity(cx)).await {
                Some(capacity) => capacity?,
                None => {
                    // Recovery consumes this output-side Reason only after the
                    // original inbound reached a clean END_STREAM.
                    let reset = poll_fn(|cx| std::task::Poll::Ready(output.poll_reset(cx))).await;
                    return Err(CapacityClosed {
                        stream: output.stream_id().as_u32(),
                        reset: match reset {
                            std::task::Poll::Pending => CapacityResetObservation::Pending,
                            std::task::Poll::Ready(Ok(reason)) => {
                                CapacityResetObservation::Reason(u32::from(reason))
                            }
                            std::task::Poll::Ready(Err(error)) => CapacityResetObservation::Error(
                                digest_display("output-reset-observation", &error),
                            ),
                        },
                    }
                    .into());
                }
            }
        };
        ensure!(available > 0, "root output returned zero capacity");
        let take = bytes.len().min(available).min(FRAME_BYTES);
        output.send_data(bytes.split_to(take), false)?;
    }
    Ok(())
}

fn require_uncompressed(headers: &HeaderMap) -> Result<()> {
    let mut types = headers.get_all(http::header::CONTENT_TYPE).iter();
    let content_type = types
        .next()
        .context("missing gRPC content type")?
        .to_str()?;
    ensure!(
        types.next().is_none()
            && (content_type == "application/grpc" || content_type == "application/grpc+proto"),
        "unsupported or duplicate gRPC content type"
    );
    let mut encodings = headers.get_all("grpc-encoding").iter();
    if let Some(encoding) = encodings.next() {
        ensure!(
            encoding == "identity" && encodings.next().is_none(),
            "unsupported or duplicate gRPC encoding"
        );
    }
    Ok(())
}
fn single_message(bytes: &Bytes) -> Result<Bytes> {
    ensure!(
        bytes.len() >= 5 && bytes[0] == 0,
        "root message prefix is truncated or compressed"
    );
    let length = u32::from_be_bytes(bytes[1..5].try_into()?) as usize;
    ensure!(
        length == bytes.len() - 5,
        "root response is not exactly one complete gRPC message"
    );
    Ok(bytes.slice(5..))
}
fn success_status(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> Result<()> {
    ensure!(
        headers.get_all("grpc-status").iter().next().is_none(),
        "Data response unexpectedly contains header grpc-status"
    );
    let trailers = trailers.context("real Data response is missing final gRPC trailers")?;
    ensure!(
        trailers.get_all("grpc-status").iter().count() == 1
            && trailers
                .get("grpc-status")
                .is_some_and(|status| status == "0"),
        "real root reply needs exactly one successful final gRPC status"
    );
    Ok(())
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn require_canonical_reply(
    wire: &wire::FetchRootResultResponse,
    original_message: &Bytes,
    core: Arc<Core>,
) -> Result<()> {
    let canonical_length = wire.encoded_len();
    ensure!(
        canonical_length
            .checked_add(5)
            .is_some_and(|length| length <= RESPONSE_BYTES),
        "canonical real root protobuf exceeds existing root envelope"
    );
    ensure!(
        canonical_length == original_message.len(),
        "real root protobuf length differs from canonical prost encoding"
    );
    // Byte equality, rather than length alone, establishes this actor's
    // canonical wire precondition. The temporary Vec shares the same credit;
    // OwnedBuffer also charges any excess actual allocation capacity.
    let mut canonical = OwnedBuffer::new(core, canonical_length)?;
    wire.encode(&mut canonical.bytes)
        .context("encode bounded canonical real root protobuf")?;
    ensure!(
        canonical.bytes.len() == canonical_length
            && canonical.bytes.as_slice() == original_message.as_ref(),
        "real root protobuf bytes differ from canonical prost encoding"
    );
    drop(canonical);
    Ok(())
}

// Preserve the exact real scalar row; only append the closed malformed suffix.
// The new body backing is charged alongside the original until its last alias exits.
fn append_prefix_only_suffix(core: Arc<Core>, original: &Bytes) -> Result<Bytes> {
    ensure!(
        original.len() == 69,
        "suffix fixture requires the real 69-byte scalar body"
    );
    let length = original
        .len()
        .checked_add(4)
        .context("suffix body length overflow")?;
    ensure!(
        length <= RootProfileV1::SEGMENT_BYTES,
        "suffix body exceeds frozen segment"
    );
    let mut body = OwnedBuffer::new(core, length)?;
    body.append(original)?;
    body.append(&[1, 0, 0, 0])?;
    Ok(body.into_bytes())
}

fn reply_data(response: &wire::FetchRootResultResponse) -> Result<&result_wire::RootData> {
    let Some(wire::fetch_root_result_response::Result::Reply(receipt)) = response.result.as_ref()
    else {
        bail!("fault mutation requires an actual Reply receipt");
    };
    let Some(wire::root_read_receipt::Outcome::Data(data)) = receipt.outcome.as_ref() else {
        bail!("verified Data1 outcome unexpectedly disappeared");
    };
    Ok(data)
}

fn reply_data_mut(
    response: &mut wire::FetchRootResultResponse,
) -> Result<&mut result_wire::RootData> {
    let Some(wire::fetch_root_result_response::Result::Reply(receipt)) = response.result.as_mut()
    else {
        bail!("fault mutation requires an actual Reply receipt");
    };
    let Some(wire::root_read_receipt::Outcome::Data(data)) = receipt.outcome.as_mut() else {
        bail!("closed mutation unexpectedly changed real Data outcome");
    };
    Ok(data)
}

async fn inject_reply(
    upstream: Response<RecvStream>,
    mut downstream: server::SendResponse<Bytes>,
    expected: RootResultRead,
    deadline: Instant,
    core: Arc<Core>,
) -> Result<()> {
    let (mut parts, mut body) = upstream.into_parts();
    ensure!(
        parts.status == http::StatusCode::OK,
        "real root response HTTP status is not 200"
    );
    require_uncompressed(&parts.headers)?;
    let (original, trailers) = read_bounded(&mut body, RESPONSE_BYTES, core.clone()).await?;
    success_status(&parts.headers, trailers.as_ref())?;
    let original_message = single_message(&original)?;
    let mut wire = wire::FetchRootResultResponse::decode(original_message.clone())
        .context("real root reply protobuf")?;
    ensure!(
        parts
            .headers
            .get_all(http::header::CONTENT_LENGTH)
            .iter()
            .count()
            <= 1,
        "real root response has duplicate content-length"
    );
    if let Some(length) = parts.headers.get(http::header::CONTENT_LENGTH) {
        ensure!(
            length.to_str()?.parse::<usize>()? == original.len(),
            "real root content-length differs from body"
        );
    }
    let decoded = decode_response(
        wire.clone(),
        &expected,
        expected.consumed(),
        FieldPath::root("root_fault_original"),
    )?;
    let reply = match decoded {
        RootReadResponse::Reply(reply) => reply,
        RootReadResponse::Deferred(_) | RootReadResponse::Refused(_) => {
            // Neither branch acknowledges a watermark. Preserve the complete
            // response and let the original FE owner decide the next action.
            // SlotExit retains the same absolute capture deadline and mutation.
            drop(wire);
            drop(original_message);
            drop(body);
            let mut send = downstream.send_response(Response::from_parts(parts, ()), false)?;
            send_bytes(&mut send, original).await?;
            send.send_trailers(trailers.context("verified root trailers disappeared")?)?;
            return Ok(());
        }
    };
    require_canonical_reply(&wire, &original_message, core.clone())?;
    drop(original_message);
    let is_not_ready = matches!(&reply.outcome, RootReadOutcome::NotReady);
    ensure!(
        is_not_ready || matches!(&reply.outcome, RootReadOutcome::Data(_)),
        "one-shot root phase requires a legal NotReady or real Data1 reply"
    );
    if is_not_ready {
        let mut state = core.state.lock().expect("root fault state lock");
        ensure!(
            state.not_ready_observed < MAX_TARGET_ATTEMPTS,
            "target NotReady observation cap exhausted"
        );
        state.not_ready_observed += 1;
        let evidence = serde_json::json!({"attempt":state.target_attempts,
            "task":reply.root_task.to_string(),"profile":wire.profile_id,
            "kind":"ClientRows","accepted_consumed":reply.accepted_consumed,
            "wire_bytes":original.len(),"wire_sha256":hash(&original)});
        state.not_ready_replies.push(evidence);
    }
    let mutation = loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        {
            let state = core.state.lock().expect("root fault state lock");
            ensure!(
                state.deadline == Some(deadline)
                    && state.failures.is_empty()
                    && !state.failure_overflow,
                "root fault target was disarmed or failed"
            );
            if let Some(mutation) = state.armed_mutation {
                break mutation;
            }
        }
        changed.await;
    };
    if is_not_ready {
        drop(wire);
        drop(reply);
        drop(body);
        let mut send = downstream.send_response(Response::from_parts(parts, ()), false)?;
        send_bytes(&mut send, original).await?;
        send.send_trailers(trailers.context("verified NotReady trailers disappeared")?)?;
        core.state
            .lock()
            .expect("root fault state lock")
            .not_ready_forwarded += 1;
        // No claim, mutation, deadline renewal or fabricated Data on NotReady.
        return Ok(());
    }
    let original_data = reply_data(&wire)?.clone();
    // Validate the untouched first body with the same carrier-neutral grammar
    // used by the real FE relay; a preexisting malformed reply is never a fixture.
    let profile = ClientRowProfile::try_new(
        RootProfileV1::SEGMENT_BYTES,
        RootProfileV1::ROW_PAYLOAD_BYTES,
    )?;
    let after = ClientRowStreamCursor::new()
        .validate_body(profile, &original_data.body)?
        .after();
    if let Some(end) = original_data.end_after_data.as_ref() {
        after.validate_end()?;
        ensure!(
            after.completed_rows() == end.output_rows,
            "real initial Data End row count differs from validated ClientRows body"
        );
    }
    let before = serde_json::json!({"task":reply.root_task.to_string(),"profile":wire.profile_id,
        "kind":"ClientRows","accepted_consumed":reply.accepted_consumed,
        "sequence":original_data.sequence,"body_bytes":original_data.body.len(),
        "body_sha256":hash(&original_data.body),"wire_bytes":original.len(),"wire_sha256":hash(&original),
        "end_after_data":original_data.end_after_data.as_ref().map(|end|
            serde_json::json!({"sequence":end.sequence,"output_rows":end.output_rows}))});
    match mutation {
        RootReplyMutation::ProfileTwo => wire.profile_id = 2,
        RootReplyMutation::ClientRowsFalse => {
            wire.output_kind = Some(result_wire::RootOutputKind {
                kind: Some(result_wire::root_output_kind::Kind::ClientRows(false)),
            });
        }
        RootReplyMutation::FourBytePrefixOnly => {
            reply_data_mut(&mut wire)?.body = Bytes::from_static(&[1, 0, 0, 0]);
        }
        RootReplyMutation::ValidRowPrefixOnlySuffix => {
            let data = reply_data_mut(&mut wire)?;
            data.body = append_prefix_only_suffix(core.clone(), &data.body)?;
        }
    }
    // The mutation list is closed. Identity, watermark, sequence and optional
    // End are preserved by the operations above, not re-created from guesses.
    let length = wire.encoded_len();
    let frame_length = length
        .checked_add(5)
        .context("mutated root message length overflow")?;
    ensure!(
        frame_length <= RESPONSE_BYTES,
        "mutated root exceeds existing root envelope"
    );
    let mut frame = OwnedBuffer::new(core.clone(), frame_length)?;
    frame.append(&[0])?;
    frame.append(&(u32::try_from(length)?).to_be_bytes())?;
    wire.encode(&mut frame.bytes)?;
    ensure!(
        frame.bytes.len() == frame_length,
        "mutated root protobuf length differs from preflight"
    );
    let frame = frame.into_bytes();
    let changed_data = reply_data(&wire)?;
    let altered = serde_json::json!({"mutation":mutation,"task":reply.root_task.to_string(),
        "profile":wire.profile_id,"output_kind":format!("{:?}",wire.output_kind),
        "accepted_consumed":reply.accepted_consumed,
        "sequence":changed_data.sequence,"body_bytes":changed_data.body.len(),
        "body_sha256":hash(&changed_data.body),
        "end_after_data":changed_data.end_after_data.as_ref().map(|end|
            serde_json::json!({"sequence":end.sequence,"output_rows":end.output_rows})),
        "wire_bytes":frame.len(),"wire_sha256":hash(&frame),
        "body_hex":if matches!(mutation,RootReplyMutation::FourBytePrefixOnly) {Some("01000000")} else {None}});
    {
        let mut state = core.state.lock().expect("root fault state lock");
        ensure!(
            state.claimed == 0 && state.deadline == Some(deadline) && Instant::now() < deadline,
            "root fault claim is not exact and within deadline"
        );
        state.claimed = 1;
        state.deadline = None;
        state.original = Some(before);
        state.mutated = Some(altered);
    }
    core.changed.notify_waiters();
    if let Some(length) = parts.headers.get(http::header::CONTENT_LENGTH) {
        let _validated_original_length = length;
        parts.headers.insert(
            http::header::CONTENT_LENGTH,
            http::HeaderValue::from_str(&frame.len().to_string())?,
        );
    }
    drop(wire);
    drop(reply);
    drop(original_data);
    drop(original);
    drop(body);
    let mut send = downstream.send_response(Response::from_parts(parts, ()), false)?;
    send_bytes(&mut send, frame).await?;
    match trailers {
        Some(trailers) => send.send_trailers(trailers)?,
        None => send.send_data(Bytes::new(), true)?,
    }
    // Queueing a complete message is observable, not proof that FE accepted it
    // or that the original H2 DATA aliases physically exited.
    core.state.lock().expect("root fault state lock").emitted += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultReply};
    use novarocks_native_trust::{
        DeploymentId, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_task_codec::root_result::encode_reply;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU64;

    #[test]
    fn ingress_diagnostics_keep_public_frame_facts_across_every_chunk_cut() {
        let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        wire.extend_from_slice(&[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 128]);
        wire.extend_from_slice(&[0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 1, 0, 0]);
        for cut in 0..=wire.len() {
            let mut facts = IngressFrames::new();
            facts.observe(&wire[..cut]);
            facts.observe(&wire[cut..]);
            assert!(facts.preface_valid);
            assert_eq!(
                (
                    facts.preface_bytes,
                    facts.header_bytes,
                    facts.payload_remaining
                ),
                (24, 0, 0)
            );
            assert_eq!(
                (facts.frames, facts.settings, facts.window_updates),
                (2, 1, 1)
            );
            assert!(!facts.application_or_unknown_frame);
            facts.observe(&[0, 0, 0, 1, 4, 0, 0, 0, 1]);
            assert!(facts.application_or_unknown_frame);
        }
        let mut partial = IngressFrames::new();
        partial.observe(&wire[..wire.len() - 1]);
        assert_eq!(partial.payload_remaining, 1);
        let mut bad = IngressFrames::new();
        bad.observe(&[b'x'; 24]);
        assert!(!bad.preface_valid);
    }

    fn core() -> Arc<Core> {
        let bounds = RootReplyFaultBounds::default();
        let subject = NativeCallerSubject::parse("fe@127.0.0.1:9080").unwrap();
        let trust = NativeTrust::new(
            DeploymentId::parse("root-actor-component").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            subject.clone(),
            NativeTransportMode::Disabled,
        );
        let (stop, _) = watch::channel(false);
        Arc::new(Core {
            connection_positions: Arc::new(Semaphore::new(bounds.maximum_connections)),
            stream_positions: Arc::new(Semaphore::new(bounds.maximum_active_streams)),
            bounds,
            verifier: trust.server_admission(),
            normal_subject: subject,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            stop,
            listeners: AtomicUsize::new(0),
            joined_listeners: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            streams: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
            joined_connections: AtomicU64::new(0),
            joined_children: AtomicU64::new(0),
        })
    }

    #[test]
    fn valid_real_row_plus_prefix_suffix_preserves_bytes_and_last_body_alias() {
        let core = core();
        let mut original = vec![65, 0, 0, 0, 64];
        original.extend_from_slice(&[b'x'; 64]);
        let original = Bytes::from(original);
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let valid = ClientRowStreamCursor::new()
            .validate_body(profile, &original)
            .unwrap();
        assert_eq!(valid.after().completed_rows(), 1);
        valid.after().validate_end().unwrap();
        let body = append_prefix_only_suffix(core.clone(), &original).unwrap();
        assert_eq!(&body[..69], original.as_ref());
        assert_eq!(&body[69..], &[1, 0, 0, 0]);
        assert_eq!(
            hash(&body),
            "112eb22b8f4cb517da83112c0bcc142bfbb7c37e4d09dfe5b2e0b1f0df53fbd7"
        );
        assert!(
            ClientRowStreamCursor::new()
                .validate_body(profile, &body)
                .is_err()
        );
        let alias = body.clone();
        let charged = core.bytes.load(Ordering::Acquire);
        assert!(charged >= 73);
        drop(body);
        assert_eq!(core.bytes.load(Ordering::Acquire), charged);
        drop(alias);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        assert!(
            append_prefix_only_suffix(core.clone(), &Bytes::from_static(&[1, 0, 0, 0])).is_err()
        );
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn same_length_reordered_protobuf_decodes_but_fails_canonical_bytes() {
        let core = core();
        // Profile 1 and reply { ack_only: true }; only the outer field order differs.
        let canonical = Bytes::from_static(&[0x10, 1, 0x22, 2, 0x10, 1]);
        let reordered = Bytes::from_static(&[0x22, 2, 0x10, 1, 0x10, 1]);
        let expected = wire::FetchRootResultResponse::decode(canonical.clone()).unwrap();
        let actual = wire::FetchRootResultResponse::decode(reordered.clone()).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.encoded_len(), reordered.len());
        require_canonical_reply(&expected, &canonical, core.clone()).unwrap();
        assert!(require_canonical_reply(&actual, &reordered, core.clone()).is_err());
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn credited_vec_refuses_global_cap_and_preserves_live_alias_credit() {
        let core = core();
        assert!(
            OwnedBuffer::new(core.clone(), core.bounds.maximum_owned_buffer_bytes + 1).is_err()
        );
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        let mut owner = OwnedBuffer::new(core.clone(), 8).unwrap();
        owner.append(b"12345678").unwrap();
        let capacity = owner.bytes.capacity();
        let body = owner.into_bytes();
        let alias = body.clone();
        let slice = body.slice(2..6);
        assert_eq!(core.bytes.load(Ordering::Acquire), capacity);
        drop(body);
        drop(alias);
        assert_eq!(core.bytes.load(Ordering::Acquire), capacity);
        assert_eq!(slice.as_ref(), b"3456");
        let remaining = core.bounds.maximum_owned_buffer_bytes - capacity;
        let credit = ByteCredit::new(core.clone(), remaining).unwrap();
        assert!(OwnedBuffer::new(core.clone(), 1).is_err());
        drop(credit);
        assert_eq!(core.bytes.load(Ordering::Acquire), capacity);
        drop(slice);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn canonical_validation_cannot_bypass_shared_credit() {
        let core = core();
        let message = Bytes::from_static(&[0x10, 1, 0x22, 2, 0x10, 1]);
        let wire = wire::FetchRootResultResponse::decode(message.clone()).unwrap();
        assert_eq!(wire.encoded_len(), message.len());
        let credit =
            ByteCredit::new(core.clone(), core.bounds.maximum_owned_buffer_bytes - 3).unwrap();
        assert!(require_canonical_reply(&wire, &message, core.clone()).is_err());
        assert_eq!(
            core.bytes.load(Ordering::Acquire),
            core.bounds.maximum_owned_buffer_bytes - 3
        );
        drop(credit);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn full_identity_is_required_for_non_target_classification() {
        let request = expected();
        let caller = FrontendProcessId::new_v7();
        let different = expected();
        let mut state = State::default();
        assert_eq!(
            classify_frozen_read(&state, request.root_task(), caller),
            StreamClass::Unknown
        );
        state.frozen_root = Some((request.root_task(), caller));
        assert_eq!(
            classify_frozen_read(&state, different.root_task(), caller),
            StreamClass::Unknown
        );
        state.frozen_backend_index = Some(1);
        assert_eq!(
            classify_frozen_read(&state, request.root_task(), caller),
            StreamClass::Target
        );
        assert_eq!(
            classify_frozen_read(&state, different.root_task(), caller),
            StreamClass::NonTarget
        );
    }

    #[test]
    fn reason_cancel_without_typed_stream_reset_is_not_exempt() {
        let reason_only = anyhow::Error::from(h2::Error::from(h2::Reason::CANCEL));
        assert!(!harmless_non_target_cancel(
            StreamClass::NonTarget,
            &reason_only
        ));
        let textual = anyhow::anyhow!("stream error: received RST_STREAM CANCEL");
        assert!(!harmless_non_target_cancel(
            StreamClass::NonTarget,
            &textual
        ));
    }

    // Real H2 peer reset, rather than manufacturing a Reason-only error. The
    // bounded duplex transport never starts a service or provider process.
    #[tokio::test]
    async fn typed_cancel_is_exempt_only_for_proven_non_target_stream() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let server = tokio::spawn(async move {
                let mut connection = server::handshake::<_>(server_io).await?;
                let (_request, mut reply) = connection
                    .accept()
                    .await
                    .context("component request absent")??;
                reply.send_reset(h2::Reason::CANCEL);
                let (_request, mut next) = connection
                    .accept()
                    .await
                    .context("next component request absent")??;
                next.send_response(Response::builder().status(200).body(())?, true)?;
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let (mut sender, driver) = client::handshake::<_>(client_io).await?;
            let driver = tokio::spawn(driver);
            let (reply, _send) = sender.send_request(
                Request::builder()
                    .uri("http://localhost/component")
                    .body(())?,
                true,
            )?;
            let reset = reply
                .await
                .expect_err("peer reset unexpectedly returned a response");
            assert!(reset.is_reset());
            assert_eq!(reset.reason(), Some(h2::Reason::CANCEL));
            let error = anyhow::Error::from(reset).context("actual H2 response stream");
            assert!(harmless_non_target_cancel(StreamClass::NonTarget, &error));
            for class in [
                StreamClass::Unknown,
                StreamClass::Target,
                StreamClass::ProvisionalCapture,
            ] {
                assert!(!harmless_non_target_cancel(class, &error));
            }
            let (next_reply, next_send) = sender.send_request(
                Request::builder().uri("http://localhost/next").body(())?,
                true,
            )?;
            assert_eq!(next_reply.await?.status(), http::StatusCode::OK);
            drop(next_send);
            drop(sender);
            drop(_send);
            driver.abort();
            let _ = driver.await;
            server.abort();
            let _ = server.await;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("component reset deadline")?
    }

    fn status_request(core: &Arc<Core>) -> Result<(Request<()>, Bytes, FrontendProcessId)> {
        use novarocks_execution_contract::identity::QueryContextRef;
        use novarocks_task_codec::identity::encode_query_context_ref;
        let caller = FrontendProcessId::new_v7();
        let trust = NativeTrust::new(
            DeploymentId::parse("root-actor-component")?,
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))?,
            core.normal_subject.clone(),
            NativeTransportMode::Disabled,
        );
        trust.bind_process_identity(NativeProcessIdentity::Frontend(caller))?;
        // Use the existing auth crate to mint the test-owned FE JWT; never
        // parse, modify or fabricate token claims in the harness.
        let mut metadata = Default::default();
        trust.apply_client_authorization(&mut metadata)?;
        let task = expected().root_task();
        let context =
            QueryContextRef::new(task.query_execution_id(), caller, task.backend_process_id());
        let wire = wire::SubscribeTaskStatusRequest {
            query_context: Some(encode_query_context_ref(context)),
            generation: 1,
            required_identities: vec![novarocks_task_codec::identity::encode_task_identity(task)],
            ..Default::default()
        };
        let mut frame = OwnedBuffer::new(core.clone(), REQUEST_BYTES)?;
        frame.append(&[0])?;
        frame.append(&(u32::try_from(wire.encoded_len())?).to_be_bytes())?;
        wire.encode(&mut frame.bytes)?;
        let mut request = Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://localhost{COVERED_STATUS_PATH}"))
            .header("content-type", "application/grpc")
            .body(())?;
        request.headers_mut().extend(metadata.into_headers());
        Ok((request, frame.into_bytes(), caller))
    }

    #[test]
    fn covered_status_requires_exact_route_auth_generation_and_all_context_scope() -> Result<()> {
        let core = core();
        let (mut request, body, caller) = status_request(&core)?;
        assert_eq!(authenticated_normal_frontend(&request, &core), Some(caller));
        validate_covered_status(&body, caller)?;
        assert!(validate_covered_status(&body, FrontendProcessId::new_v7()).is_err());
        request.headers_mut().remove("authorization");
        assert!(authenticated_normal_frontend(&request, &core).is_none());
        let (mut request, _, _) = status_request(&core)?;
        *request.method_mut() = http::Method::GET;
        assert!(authenticated_normal_frontend(&request, &core).is_none());
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = "http://localhost/SubscribeTaskStatus".parse()?;
        assert!(authenticated_normal_frontend(&request, &core).is_none());
        let mut wire = wire::SubscribeTaskStatusRequest::decode(single_message(&body)?)?;
        wire.generation = 0;
        assert!(decode_covered_subscribe_task_status(&wire, FieldPath::root("test")).is_err());
        wire.generation = 1;
        wire.required_identities
            .push(novarocks_task_codec::identity::encode_task_identity(
                expected().root_task(),
            ));
        assert!(decode_covered_subscribe_task_status(&wire, FieldPath::root("test")).is_err());
        drop(body);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }

    // Complete request END and auth/covered decode precede an actual peer RST.
    // The response is blocked by a real zero stream window. A healthy next
    // stream uses the same downstream H2 connection, and every fixture driver
    // is joined even if the bounded observation fails.
    #[tokio::test]
    async fn covered_status_capacity_close_recovers_actual_remote_cancel_only() -> Result<()> {
        for reason in [h2::Reason::CANCEL, h2::Reason::REFUSED_STREAM] {
            let core = core();
            let (request, body, caller) = status_request(&core)?;
            core.failure("component preexisting failure retained");
            let mut drivers = JoinSet::new();
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                let (up_client, up_server) = tokio::io::duplex(65536);
                let upstream_core = core.clone();
                drivers.spawn(async move {
                    let mut connection = server::handshake::<_>(up_server).await?;
                    let (request, mut response) = connection
                        .accept()
                        .await
                        .context("status upstream absent")??;
                    let upstream_work = async {
                        assert_eq!(
                            authenticated_normal_frontend(&request, &upstream_core),
                            Some(caller)
                        );
                        let mut inbound = request.into_body();
                        let (body, trailers) =
                            read_bounded(&mut inbound, REQUEST_BYTES, upstream_core).await?;
                        assert!(trailers.is_none());
                        validate_covered_status(&body, caller)?;
                        drop(body);
                        let mut output = response
                            .send_response(Response::builder().status(200).body(())?, false)?;
                        output.send_data(Bytes::from_static(b"bounded-status-response"), false)?;
                        // Drive until the actor releases its upstream response.
                        let _ = poll_fn(|cx| output.poll_reset(cx)).await;
                        // Keep the server transport owner alive until fixture
                        // cleanup aborts and joins both ends. Reset observation
                        // alone is not permission to race the client driver.
                        std::future::pending::<Result<()>>().await
                    };
                    tokio::select! {
                        result = upstream_work => result,
                        _ = async { while connection.accept().await.is_some() {} } => Ok(()),
                    }
                });
                let (up_sender, up_driver) = client::handshake(up_client).await?;
                drivers.spawn(async move { up_driver.await.context("status upstream driver") });
                let (down_client, down_server) = tokio::io::duplex(65536);
                let downstream_core = core.clone();
                let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
                drivers.spawn(async move {
                    let mut connection = server::handshake::<_>(down_server).await?;
                    let (request, response) = connection
                        .accept()
                        .await
                        .context("status downstream absent")??;
                    let forwarding =
                        forward_stream(request, response, up_sender, 1, downstream_core);
                    let next_stream = async {
                        let (request, mut response) = connection
                            .accept()
                            .await
                            .context("healthy next stream absent")??;
                        assert_eq!(request.uri().path(), "/healthy-next");
                        response.send_response(Response::builder().status(200).body(())?, true)?;
                        // Continue driving the same connection until the test closes it.
                        while connection.accept().await.is_some() {}
                        Ok::<_, anyhow::Error>(())
                    };
                    let forwarding = async {
                        let outcome = forwarding.await;
                        let _ = outcome_tx.send(outcome);
                    };
                    tokio::join!(forwarding, next_stream).1
                });
                let mut builder = client::Builder::new();
                builder.initial_window_size(0);
                let (mut sender, driver) = builder.handshake::<_, Bytes>(down_client).await?;
                drivers.spawn(async move { driver.await.context("status downstream driver") });
                let (reply, mut send) = sender.send_request(request, false)?;
                send.send_data(body, true)?;
                let reply = reply.await?;
                assert_eq!(reply.status(), http::StatusCode::OK);
                // Headers establish that the request was consumed/validated and
                // response copying has started; no test sleep or race guess.
                send.send_reset(reason);
                let outcome = outcome_rx.await?;
                if reason == h2::Reason::CANCEL {
                    outcome?;
                    let observed = RootReplyFaultControl { core: core.clone() }.observation();
                    assert_eq!(observed.status_subscription_peer_cancels, 1);
                    assert_eq!(observed.non_target_peer_cancels, 0);
                    assert_eq!(
                        observed.failures,
                        ["component preexisting failure retained"]
                    );
                    let digest = observed
                        .last_status_subscription_peer_cancel
                        .context("missing actual cancel digest")?;
                    assert_eq!(digest.class, "covered-status-subscription-remote-cancel");
                    assert_eq!(digest.sha256.len(), 64);
                    assert!(digest.bytes > 0);
                } else {
                    let error = outcome.expect_err("non-CANCEL reset was incorrectly recovered");
                    assert!(
                        error.downcast_ref::<CapacityClosed>().is_some()
                            || error.downcast_ref::<h2::Error>().is_some()
                    );
                    assert_eq!(
                        core.state.lock().unwrap().status_subscription_peer_cancels,
                        0
                    );
                }
                drop(reply);
                drop(send);
                sender = sender.ready().await?;
                let (reply, send) = sender.send_request(
                    Request::builder()
                        .uri("http://localhost/healthy-next")
                        .body(())?,
                    true,
                )?;
                assert_eq!(reply.await?.status(), http::StatusCode::OK);
                drop(send);
                drop(sender);
                assert_eq!(core.bytes.load(Ordering::Acquire), 0);
                Ok::<_, anyhow::Error>(())
            })
            .await;
            let result =
                match result {
                    Ok(outcome) => outcome,
                    Err(error) => Err(anyhow::Error::from(error)
                        .context("covered status fixture absolute deadline")),
                };
            drivers.abort_all();
            // Abort is a fixture cleanup request; actual exit is joined here.
            let mut cleanup = None;
            while let Some(joined) = drivers.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        cleanup.get_or_insert(error);
                    }
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        cleanup.get_or_insert(anyhow::Error::from(error));
                    }
                }
            }
            finish_fixture(result, cleanup)?;
            assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    fn finish_fixture(primary: Result<()>, cleanup: Option<anyhow::Error>) -> Result<()> {
        match (primary, cleanup) {
            (Err(primary), Some(cleanup)) => {
                let digest = digest_display("fixture-cleanup", &cleanup);
                Err(primary).with_context(|| {
                    format!(
                        "fixture cleanup also failed (class={}, sha256={}, bytes={})",
                        digest.class, digest.sha256, digest.bytes
                    )
                })
            }
            (Err(primary), None) => Err(primary),
            (Ok(()), Some(cleanup)) => Err(cleanup).context("fixture cleanup"),
            (Ok(()), None) => Ok(()),
        }
    }

    // Exercise actual receive-stream causes, including late errors after EOF.
    // These are local duplex classification fixtures, not FE/BE acceptance.
    #[tokio::test]
    async fn covered_status_refuses_wrong_class_none_goaway_and_io() -> Result<()> {
        for mode in ["cancel", "none", "goaway", "io"] {
            let core = core();
            let (request, request_body, caller) = status_request(&core)?;
            let mut drivers = JoinSet::new();
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                let (client_io, server_io) = tokio::io::duplex(65536);
                let server_core = core.clone();
                let (facts_tx, facts_rx) = tokio::sync::oneshot::channel();
                let (close_tx, close_rx) = tokio::sync::oneshot::channel();
                drivers.spawn(async move {
                    let mut connection = server::handshake::<_>(server_io).await?;
                    let (request, mut response) = connection
                        .accept()
                        .await
                        .context("negative fixture request absent")??;
                    let mut inbound = request.into_body();
                    let work = async {
                        let (body, trailers) =
                            read_bounded(&mut inbound, REQUEST_BYTES, server_core.clone()).await?;
                        validate_covered_status(&body, caller)?;
                        assert!(trailers.is_none());
                        drop(body);
                        let mut output = response.send_response(
                            Response::builder().status(200).body(())?,
                            mode == "none",
                        )?;
                        if mode == "cancel" || mode == "none" {
                            let error = send_bytes(
                                &mut output,
                                Bytes::from_static(b"never-sent-zero-window"),
                            )
                            .await
                            .expect_err("closed output unexpectedly sent DATA");
                            assert!(error.downcast_ref::<CapacityClosed>().is_some());
                            for class in [
                                StreamClass::Unknown,
                                StreamClass::Target,
                                StreamClass::ProvisionalCapture,
                                StreamClass::NonTarget,
                            ] {
                                assert!(
                                    !recover_covered_status_cancel(
                                        class,
                                        &error,
                                        &mut inbound,
                                        &server_core
                                    )
                                    .await
                                );
                            }
                            for error in [
                                anyhow::anyhow!("root output capacity closed"),
                                anyhow::Error::from(h2::Error::from(h2::Reason::CANCEL)),
                                anyhow::Error::from(std::io::Error::new(
                                    std::io::ErrorKind::ConnectionReset,
                                    "fixture IO",
                                )),
                            ] {
                                assert!(
                                    !recover_covered_status_cancel(
                                        StreamClass::CoveredStatusSubscription,
                                        &error,
                                        &mut inbound,
                                        &server_core
                                    )
                                    .await
                                );
                            }
                            let recovered = recover_covered_status_cancel(
                                StreamClass::CoveredStatusSubscription,
                                &error,
                                &mut inbound,
                                &server_core,
                            )
                            .await;
                            assert_eq!(recovered, mode == "cancel");
                            let _ = facts_tx.send(());
                            close_rx.await?;
                            Ok::<_, anyhow::Error>(None)
                        } else {
                            // Return the connection to the owner after response
                            // HEADERS; the owner makes a real GOAWAY or IO close.
                            close_rx.await?;
                            Ok(Some(output))
                        }
                    };
                    let driving = async {
                        while connection.accept().await.is_some() {}
                        Ok::<_, anyhow::Error>(())
                    };
                    if mode == "goaway" || mode == "io" {
                        // The driver must flush HEADERS before the termination
                        // handshake requests abrupt shutdown or drops transport.
                        let held_output = tokio::select! {
                            result = work => result?,
                            result = driving => { result?; None },
                        };
                        if mode == "goaway" {
                            connection.abrupt_shutdown(h2::Reason::PROTOCOL_ERROR);
                            while connection.accept().await.is_some() {}
                        }
                        // Preserve the SendStream until its connection owner
                        // actually performs the requested termination.
                        drop(connection);
                        drop(held_output);
                        Ok::<_, anyhow::Error>(())
                    } else {
                        tokio::select! { result = work => result.map(|_| ()), result = driving => result }
                    }
                });
                let mut builder = client::Builder::new();
                builder.initial_window_size(0);
                let (mut sender, driver) = builder.handshake::<_, Bytes>(client_io).await?;
                let (driver_cause_tx, driver_cause_rx) = tokio::sync::oneshot::channel();
                drivers.spawn(async move {
                    let outcome = driver.await;
                    if mode == "goaway" {
                        let _ = driver_cause_tx.send(outcome.err());
                        Ok(())
                    } else {
                        outcome.context("negative fixture client driver")
                    }
                });
                let (reply, mut send) = sender.send_request(request, false)?;
                send.send_data(request_body, true)?;
                let mut inbound = reply.await?.into_body();
                let mut close_tx = Some(close_tx);
                if mode == "cancel" {
                    send.send_reset(h2::Reason::CANCEL);
                    facts_rx.await?;
                } else if mode == "none" {
                    facts_rx.await?;
                    assert!(inbound.data().await.is_none());
                } else {
                    let _ = close_tx.take().unwrap().send(());
                    if mode == "goaway" {
                        // The actual connection driver supplies the GOAWAY
                        // error. It is not manufactured from its Reason.
                        let cause = driver_cause_rx.await?.context("GOAWAY driver cause absent")?;
                        assert!(cause.is_go_away());
                        assert!(cause.is_remote());
                        let failure = anyhow::Error::from(cause);
                        assert!(!recover_covered_status_cancel(StreamClass::CoveredStatusSubscription, &failure, &mut inbound, &core).await);
                    } else {
                        // IO discrimination uses an actual received response
                        // stream; this is not a request-EOF recovery claim.
                        let actual = inbound.data().await.context("negative cause absent")?.expect_err("negative cause unexpectedly produced DATA");
                        assert!(actual.is_io());
                        let failure = anyhow::Error::from(actual);
                        assert!(!recover_covered_status_cancel(StreamClass::CoveredStatusSubscription, &failure, &mut inbound, &core).await);
                    }
                }
                if let Some(close_tx) = close_tx.take() { let _ = close_tx.send(()); }
                assert_eq!(
                    core.state.lock().unwrap().status_subscription_peer_cancels,
                    u64::from(mode == "cancel")
                );
                drop(inbound);
                drop(send);
                drop(sender);
                Ok::<_, anyhow::Error>(())
            })
            .await;
            let result = result
                .map_err(anyhow::Error::from)
                .and_then(|result| result);
            drivers.abort_all();
            let mut cleanup = None;
            while let Some(joined) = drivers.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(error))
                        if error.downcast_ref::<h2::Error>().is_some_and(|cause| {
                            (mode == "io" && cause.is_io())
                                || (mode == "goaway" && cause.is_go_away())
                        }) => {}
                    Ok(Err(error)) => {
                        cleanup.get_or_insert(error);
                    }
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        cleanup.get_or_insert(anyhow::Error::from(error));
                    }
                }
            }
            finish_fixture(result, cleanup)?;
            assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[test]
    fn slot_exit_preserves_not_ready_phase_and_existing_failure() {
        let core = core();
        let deadline = Instant::now() + Duration::from_secs(1);
        {
            let mut state = core.state.lock().unwrap();
            state.pending_capture = true;
            state.deadline = Some(deadline);
            state.not_ready_forwarded = 1;
        }
        drop(SlotExit(core.clone()));
        let control = RootReplyFaultControl { core: core.clone() };
        assert_eq!(control.observation().claimed, 0);
        assert_eq!(control.observation().emitted_messages, 0);
        assert_eq!(core.state.lock().unwrap().deadline, Some(deadline));
        core.failure("component first failure");
        drop(SlotExit(core.clone()));
        assert_eq!(control.observation().failures, ["component first failure"]);
        assert!(core.state.lock().unwrap().deadline.is_none());
    }

    fn expected() -> RootResultRead {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        RootResultRead::try_new(
            TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(100),
        )
        .unwrap()
    }

    fn frame(core: Arc<Core>, wire: &wire::FetchRootResultResponse) -> Result<Bytes> {
        let length = wire.encoded_len();
        let mut frame = OwnedBuffer::new(core, length + 5)?;
        frame.append(&[0])?;
        frame.append(&(length as u32).to_be_bytes())?;
        wire.encode(&mut frame.bytes)?;
        Ok(frame.into_bytes())
    }

    async fn assert_actual_transparent_response(
        make_wire: impl FnOnce(&RootResultRead) -> Result<wire::FetchRootResultResponse>,
        is_not_ready: bool,
        deadline_context: &'static str,
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let core = core();
            let expected = expected();
            let wire = make_wire(&expected)?;
            let original = frame(core.clone(), &wire)?;
            let original_bytes = original.clone();
            let original_hash = hash(&original);
            let original_len = original.len();
            let deadline = Instant::now() + Duration::from_secs(1);
            {
                let mut state = core.state.lock().unwrap();
                state.deadline = Some(deadline);
                state.armed_mutation = is_not_ready.then_some(RootReplyMutation::ProfileTwo);
                state.target_attempts = 1;
            }
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
            trailers.insert(
                "x-component-trailer",
                http::HeaderValue::from_static("untouched"),
            );
            let expected_trailers = trailers.clone();
            let upstream_headers = Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("content-length", original_len)
                .header("x-component-header", "untouched")
                .body(())?;
            let expected_headers = upstream_headers.headers().clone();

            let (up_client_io, up_server_io) = tokio::io::duplex(65536);
            let upstream = tokio::spawn(async move {
                let mut connection = server::handshake::<_>(up_server_io).await?;
                let (_request, mut response) = connection
                    .accept()
                    .await
                    .context("upstream component request absent")??;
                let mut send = response.send_response(upstream_headers, false)?;
                send.send_data(original, false)?;
                send.send_trailers(trailers)?;
                drop(send);
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let (mut up_sender, up_driver) = client::handshake::<_>(up_client_io).await?;
            let up_driver = tokio::spawn(up_driver);
            let (up_reply, up_send) = up_sender.send_request(
                Request::builder().uri("http://localhost/root").body(())?,
                true,
            )?;
            let up_reply = up_reply.await?;

            let (down_client_io, down_server_io) = tokio::io::duplex(65536);
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let downstream = tokio::spawn(async move {
                let mut connection = server::handshake::<_>(down_server_io).await?;
                let (_request, response) = connection
                    .accept()
                    .await
                    .context("downstream component request absent")??;
                response_tx
                    .send(response)
                    .map_err(|_| anyhow::anyhow!("component response owner gone"))?;
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let (mut down_sender, down_driver) = client::handshake::<_>(down_client_io).await?;
            let down_driver = tokio::spawn(down_driver);
            let (down_reply, down_send) = down_sender.send_request(
                Request::builder().uri("http://localhost/root").body(())?,
                true,
            )?;
            let response = response_rx.await?;
            inject_reply(up_reply, response, expected, deadline, core.clone()).await?;
            let response = down_reply.await?;
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(response.headers(), &expected_headers);
            let mut body = response.into_body();
            let (actual, actual_trailers) =
                read_bounded(&mut body, RESPONSE_BYTES, core.clone()).await?;
            assert_eq!(actual, original_bytes);
            assert_eq!(actual.len(), original_len);
            assert_eq!(hash(&actual), original_hash);
            assert_eq!(actual_trailers.as_ref(), Some(&expected_trailers));
            let observation = RootReplyFaultControl { core: core.clone() }.observation();
            assert_eq!(observation.claimed, 0);
            assert_eq!(observation.emitted_messages, 0);
            assert_eq!(observation.not_ready_observed, u64::from(is_not_ready));
            assert_eq!(observation.not_ready_forwarded, u64::from(is_not_ready));
            assert!(observation.original.is_none() && observation.mutated.is_none());
            assert!(observation.failures.is_empty());
            assert_eq!(core.state.lock().unwrap().deadline, Some(deadline));
            drop(actual);
            drop(original_bytes);
            drop(body);
            drop(up_sender);
            drop(up_send);
            drop(down_sender);
            drop(down_send);
            up_driver.abort();
            down_driver.abort();
            upstream.abort();
            downstream.abort();
            let _ = up_driver.await;
            let _ = down_driver.await;
            let _ = upstream.await;
            let _ = downstream.await;
            assert_eq!(core.bytes.load(Ordering::Acquire), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context(deadline_context)?
    }

    #[tokio::test]
    async fn actual_not_ready_forwarding_preserves_wire_and_does_not_claim() -> Result<()> {
        assert_actual_transparent_response(
            |expected| {
                Ok(encode_reply(&RootResultReply {
                    root_task: expected.root_task(),
                    profile: expected.profile(),
                    kind: expected.kind(),
                    accepted_consumed: 0,
                    outcome: RootReadOutcome::NotReady,
                })?)
            },
            true,
            "component NotReady deadline",
        )
        .await
    }

    #[tokio::test]
    async fn actual_deferred_forwarding_keeps_wire_and_deadline_without_arming_or_ack() -> Result<()>
    {
        use novarocks_execution_contract::root_result::{RootReadDeferral, RootReadDeferred};
        assert_actual_transparent_response(
            |expected| {
                Ok(novarocks_task_codec::root_result::encode_response(
                    &RootReadResponse::Deferred(RootReadDeferred::for_request(
                        expected,
                        RootReadDeferral::Preparing,
                    )),
                )?)
            },
            false,
            "component Deferred deadline",
        )
        .await
    }

    #[tokio::test]
    async fn actual_refused_forwarding_keeps_wire_and_deadline_without_arming_or_ack() -> Result<()>
    {
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{FailureClass, FailureFact, FailureReason},
            root_result::RootReadRefusal,
        };
        assert_actual_transparent_response(
            |expected| {
                Ok(novarocks_task_codec::root_result::encode_response(
                    &RootReadResponse::Refused(Box::new(RootReadRefusal::for_request(
                        expected,
                        FailureFact::new(
                            FailureClass::Data,
                            FailureReason::new(
                                std::num::NonZeroU16::MAX,
                                std::num::NonZeroU16::new(1).unwrap(),
                            ),
                        ),
                        SafeDetail::truncating("component root read domain refusal"),
                    ))),
                )?)
            },
            false,
            "component Refused deadline",
        )
        .await
    }

    #[tokio::test]
    async fn actor_stop_joins_pending_stream_owners_and_returns_credit() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let upstream_address = upstream_listener.local_addr()?;
            let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
            let upstream = tokio::spawn(async move {
                let (socket, _) = upstream_listener.accept().await?;
                let mut connection = server::handshake(socket).await?;
                let (request, response) = connection
                    .accept()
                    .await
                    .context("pending component request absent")??;
                accepted_tx
                    .send(())
                    .map_err(|_| anyhow::anyhow!("component waiter gone"))?;
                let _held = (request, response);
                while connection.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let core = core();
            let mut actor = RootReplyFaultProxy::start(
                upstream_address,
                0,
                None,
                RootReplyFaultBounds::default(),
                core.verifier.clone(),
                core.normal_subject.clone(),
            )?;
            let control = actor.control();
            let socket = tokio::net::TcpStream::connect(actor.address()).await?;
            let (mut sender, driver) = client::handshake(socket).await?;
            let driver = tokio::spawn(driver);
            let (reply, mut send) = sender.send_request(
                Request::builder()
                    .uri("http://localhost/pending")
                    .body(())?,
                false,
            )?;
            send.send_data(Bytes::from_static(b"bounded-live-body"), false)?;
            accepted_rx.await?;
            // The stream cannot complete normally: both request and response
            // are held. stop must join the actual child owners before returning.
            tokio::task::spawn_blocking(move || actor.stop()).await?;
            drop(reply);
            drop(send);
            drop(sender);
            driver.abort();
            let _ = driver.await;
            upstream.abort();
            let _ = upstream.await;
            let observed = control.observation();
            assert!(observed.shutdown_joined);
            assert_eq!(observed.active_listeners, 0);
            assert_eq!(observed.active_connections, 0);
            assert_eq!(observed.active_streams, 0);
            assert_eq!(observed.connection_positions, 0);
            assert_eq!(observed.stream_positions, 0);
            assert_eq!(observed.owned_buffer_bytes, 0);
            assert!(observed.joined_children >= 2);
            assert!(observed.failures.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("component actor join deadline")?
    }
}
