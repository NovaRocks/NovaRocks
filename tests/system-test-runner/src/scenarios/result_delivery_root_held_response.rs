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

//! One original authenticated replay response, deliberately left incomplete.
//!
//! This is a runner actor, not a Root authority or a BE backing observer.
//! All asynchronous operations borrow this outer owner. The scene must retain
//! it through every failure/cancel branch and call `settle` before role teardown.

use super::{REQUEST_CAP, RESPONSE_CAP, ROOT_PATH, grpc_content_type};
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::{RecvStream, SendStream, client};
use http::{HeaderValue, Request, header};
use novarocks_cluster_harness::NativeTrustFixtureMode;
use novarocks_execution_contract::{
    TaskIdentity,
    root_result::{RootReadOutcome, RootResultRead, RootResultReply},
};
use novarocks_native_trust::NativeEndpointConnector;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as wire;
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_task_codec::root_result::decode_read;
use novarocks_transport_io::BoxedIo;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::future::poll_fn;
use std::time::{Duration, Instant};
use tokio::task::{JoinError, JoinHandle};

const S: usize = 1_048_576;
const CAPTURE_BYTES: usize = 4096;
const FRAME_BYTES: usize = 16_384;
const CAPTURE_FRAMES: u32 = 16;
// Immutable installed-root-protocol-freeze-v3 Data1 digest, not a reply-derived golden.
const DATA1_SHA: [u8; 32] = [
    0xe1, 0x77, 0x8a, 0x1a, 0x63, 0xf0, 0xde, 0xff, 0x42, 0x3d, 0x34, 0xc2, 0x67, 0xbb, 0xc2, 0xbe,
    0x60, 0xc0, 0x18, 0xad, 0xde, 0x82, 0xcf, 0x23, 0xdf, 0xcc, 0x99, 0x26, 0x58, 0x2d, 0x0b, 0x42,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum Phase {
    Prepared,
    Opening,
    Held,
    Settling,
    Settled,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum FailureClass {
    Transition,
    Clock,
    Transport,
    Framing,
    Driver,
    PriorFailure,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum DriverExit {
    NotSpawned,
    Live,
    Closed,
    AbortedAndJoined,
    ConnectionFailed,
    JoinFailed,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub(crate) struct HeldObservation {
    pub phase: Phase,
    pub first_failure: Option<FailureClass>,
    pub backend_index: usize,
    pub actual_grpc_port: u16,
    pub proven_offered_sequence: u64,
    pub received_frames: u32,
    pub received_bytes: u64,
    pub received_sha256: [u8; 32],
    pub received_digest_complete: bool,
    pub captured_prefix_bytes: usize,
    pub captured_prefix_sha256: [u8; 32],
    pub declared_message_bytes: Option<u32>,
    /// Last sample before abandoning the response; not a post-Drop assertion.
    pub withheld_stream_credit_bytes: usize,
    /// Explicit release_capacity calls only, excluding reset/Drop effects.
    pub released_stream_credit_bytes: usize,
    pub reply_fully_decoded: bool,
    /// send_reset was invoked; this does not prove the RST reached the BE.
    pub reset_requested: bool,
    pub response_abandoned: bool,
    pub abort_requested: bool,
    pub driver_exit: DriverExit,
}

/// The actual first source remains owned; fixed presentation never formats it.
pub(crate) struct HeldFailure {
    pub class: FailureClass,
    pub observation: HeldObservation,
    pub cause: anyhow::Error,
    pub additional_actual_source: Option<anyhow::Error>,
}
impl std::fmt::Debug for HeldFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldFailure")
            .field("class", &self.class)
            .field("actual_sources_retained", &true)
            .finish()
    }
}
impl std::fmt::Display for HeldFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original held root response failed; actual sources retained")
    }
}
impl std::error::Error for HeldFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// Scalar projection from the already decoded/validated original Data1.
/// It contains no Bytes/reply/guard alias and cannot select another Root.
pub(crate) struct ProvenReplayOne {
    read: RootResultRead,
}
impl ProvenReplayOne {
    pub(crate) fn from_validated_data1(
        read: &RootResultRead,
        reply: &RootResultReply,
    ) -> Result<Self> {
        ensure!(
            read.profile() == RootProfileId::V1
                && read.kind() == RootOutputKind::ClientRows
                && read.wanted().map(|n| n.get()) == Some(1)
                && read.consumed() == 0
                && read.max_wait() == Duration::from_millis(100),
            "original replay request differs"
        );
        reply.validate()?;
        ensure!(
            reply.root_task == read.root_task()
                && reply.profile == read.profile()
                && reply.kind == read.kind()
                && reply.accepted_consumed == 0,
            "original decoded Data1 identity/frontier differs"
        );
        let RootReadOutcome::Data(data) = &reply.outcome else {
            anyhow::bail!("original reply was not Data1");
        };
        ensure!(
            data.sequence().get() == 1
                && data.body().len() == S
                && data.end_after_data().is_none()
                && <[u8; 32]>::from(Sha256::digest(data.body())) == DATA1_SHA,
            "original Data1 differs from immutable S+8 input"
        );
        Ok(Self { read: read.clone() })
    }
    /// Check only the real typed closed reply; UnknownRoot is not this proof.
    pub(crate) fn require_closed_ack(
        &self,
        actual_read: &RootResultRead,
        reply: &RootResultReply,
    ) -> Result<()> {
        ensure!(
            actual_read.root_task() == self.read.root_task()
                && actual_read.profile() == self.read.profile()
                && actual_read.kind() == self.read.kind()
                && actual_read.wanted().is_none()
                && actual_read.consumed() == 1
                && actual_read.max_wait() == self.read.max_wait(),
            "late ACK request differs from the original proven sequence"
        );
        reply.validate()?;
        ensure!(
            reply.root_task == self.read.root_task()
                && reply.profile == self.read.profile()
                && reply.kind == self.read.kind()
                && reply.accepted_consumed == 0
                && matches!(reply.outcome, RootReadOutcome::AwaitTerminalControl),
            "late ACK did not preserve the original sealed frontier"
        );
        Ok(())
    }
    pub(crate) fn root_task(&self) -> TaskIdentity {
        self.read.root_task()
    }
}

struct Capture {
    prefix: [u8; CAPTURE_BYTES],
    filled: usize,
    frames: u32,
    received: u64,
    hash: Sha256,
    digest_complete: bool,
    declared: Option<u32>,
}
impl Capture {
    fn new() -> Self {
        Self {
            prefix: [0; CAPTURE_BYTES],
            filled: 0,
            frames: 0,
            received: 0,
            hash: Sha256::new(),
            digest_complete: true,
            declared: None,
        }
    }
    fn push(&mut self, data: &[u8]) -> Result<()> {
        // Record actual returned bytes before clock/framing rejection. An
        // oversized third-party frame gets only a finite prefix/hash sample.
        self.received = self
            .received
            .checked_add(u64::try_from(data.len())?)
            .context("held byte count overflow")?;
        self.frames = self
            .frames
            .checked_add(1)
            .context("held frame count overflow")?;
        let copied = (CAPTURE_BYTES - self.filled).min(data.len());
        self.prefix[self.filled..self.filled + copied].copy_from_slice(&data[..copied]);
        self.filled += copied;
        self.hash.update(&data[..data.len().min(FRAME_BYTES)]);
        if data.len() > FRAME_BYTES {
            self.digest_complete = false;
        }
        ensure!(
            !data.is_empty() && data.len() <= FRAME_BYTES && self.frames <= CAPTURE_FRAMES,
            "held capture DATA frame violates its fixed bound"
        );
        ensure!(
            self.received <= RESPONSE_CAP as u64,
            "held response exceeds original wire bound"
        );
        if self.filled >= 5 && self.declared.is_none() {
            ensure!(self.prefix[0] == 0, "held gRPC response uses compression");
            let length = u32::from_be_bytes(self.prefix[1..5].try_into()?);
            ensure!(
                (S as u32..=(RESPONSE_CAP - 5) as u32).contains(&length),
                "held response is not an incomplete S-byte replay candidate"
            );
            self.declared = Some(length);
        }
        if let Some(length) = self.declared {
            ensure!(
                self.received < u64::from(length) + 5,
                "held response was already complete or has trailing bytes"
            );
        }
        Ok(())
    }
}

/// A success receipt exists only after the original JoinHandle actually returns.
/// Expected local abort still retains that actual JoinError as owned evidence.
pub(crate) struct JoinedOriginalDriver {
    pub observation: HeldObservation,
    pub actual_expected_cancellation: Option<JoinError>,
}
impl std::fmt::Debug for JoinedOriginalDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JoinedOriginalDriver { actual_join_completed: true }")
    }
}

pub(crate) struct HeldRootResponse {
    proof: ProvenReplayOne,
    deadline: Instant,
    connector: Option<NativeEndpointConnector>,
    authorization: Option<HeaderValue>,
    request_frame: Option<Bytes>,
    sender: Option<client::SendRequest<Bytes>>,
    request_stream: Option<SendStream<Bytes>>,
    response: Option<RecvStream>,
    driver: Option<JoinHandle<std::result::Result<(), h2::Error>>>,
    capture: Capture,
    phase: Phase,
    first_failure: Option<FailureClass>,
    backend_index: usize,
    actual_grpc_port: u16,
    withheld: usize,
    reset_requested: bool,
    response_abandoned: bool,
    abort_requested: bool,
    driver_exit: DriverExit,
    // A test-only hold at the original await boundary. It owns no driver/body.
    #[cfg(test)]
    settle_before_join: Option<SettleHold>,
}
#[cfg(test)]
struct SettleHold {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}
impl HeldRootResponse {
    /// Fresh selected Root identity belongs to the caller's independent observer.
    /// This adds only exact actual role/BE checks and one replay request.
    pub(crate) fn prepare(
        context: &mut ScenarioContext,
        backend: usize,
        proof: ProvenReplayOne,
        request: &wire::FetchRootResultRequest,
        original_deadline: Instant,
    ) -> Result<Self> {
        before(original_deadline)?;
        let roles = context.recheck_live_process_launch_identities()?;
        ensure!(
            roles.len() == 4 && backend < 3,
            "held actor requires original 1FE+3BE"
        );
        let backends = context
            .handle()
            .original_exact_mysql_backend_process_ids(original_deadline)?;
        ensure!(
            backends[backend] == proof.root_task().backend_process_id(),
            "held Root differs from independently actual backend process"
        );
        let actual = decode_read(request, FieldPath::root("held_original_replay_request"))?;
        ensure!(
            actual == proof.read,
            "held request differs from original validated fetch1"
        );
        let handle = context.handle();
        let mode = handle.native_trust_mode();
        let advertised = handle.native_be_endpoint(backend)?;
        ensure!(
            mode == NativeTrustFixtureMode::Plaintext
                && advertised.host().parse::<std::net::IpAddr>().is_ok(),
            "held response supports only original plaintext/IP transport"
        );
        let port = handle
            .runtime()
            .be
            .get(backend)
            .context("held backend is absent")?
            .grpc;
        // As in the existing strict root probe, retain the advertised host
        // identity but connect the exact original runtime BE listener. An
        // advertised proxy port is not an actual listener identity.
        let endpoint = novarocks_types::NativeEndpoint::from_host_port(advertised.host(), port)
            .map_err(anyhow::Error::msg)?;
        let connector = handle.native_probe_connector(endpoint, mode)?;
        let mut authorization_request = tonic::Request::new(());
        handle
            .native_probe_trust()?
            .apply_client_authorization(authorization_request.metadata_mut())
            .map_err(anyhow::Error::msg)?;
        let auth = authorization_request
            .metadata()
            .get("authorization")
            .context("held original authorization was not issued")?
            .as_bytes();
        ensure!(
            auth.len() <= REQUEST_CAP,
            "held authorization exceeds fixed request bound"
        );
        let authorization = HeaderValue::from_bytes(auth)?;
        let frame = request_frame(request)?;
        ensure!(
            context.recheck_live_process_launch_identities()? == roles,
            "original role instance changed during held preparation"
        );
        before(original_deadline)?;
        Ok(Self::new(
            proof,
            original_deadline,
            Some(connector),
            authorization,
            frame,
            backend,
            port,
        ))
    }
    fn new(
        proof: ProvenReplayOne,
        deadline: Instant,
        connector: Option<NativeEndpointConnector>,
        authorization: HeaderValue,
        frame: Bytes,
        backend_index: usize,
        actual_grpc_port: u16,
    ) -> Self {
        Self {
            proof,
            deadline,
            connector,
            authorization: Some(authorization),
            request_frame: Some(frame),
            sender: None,
            request_stream: None,
            response: None,
            driver: None,
            capture: Capture::new(),
            phase: Phase::Prepared,
            first_failure: None,
            backend_index,
            actual_grpc_port,
            withheld: 0,
            reset_requested: false,
            response_abandoned: false,
            abort_requested: false,
            driver_exit: DriverExit::NotSpawned,
            #[cfg(test)]
            settle_before_join: None,
        }
    }
    pub(crate) fn snapshot(&self) -> HeldObservation {
        HeldObservation {
            phase: self.phase,
            first_failure: self.first_failure,
            backend_index: self.backend_index,
            actual_grpc_port: self.actual_grpc_port,
            proven_offered_sequence: 1,
            received_frames: self.capture.frames,
            received_bytes: self.capture.received,
            received_sha256: self.capture.hash.clone().finalize().into(),
            received_digest_complete: self.capture.digest_complete,
            captured_prefix_bytes: self.capture.filled,
            captured_prefix_sha256: Sha256::digest(&self.capture.prefix[..self.capture.filled])
                .into(),
            declared_message_bytes: self.capture.declared,
            withheld_stream_credit_bytes: self.withheld,
            released_stream_credit_bytes: 0,
            reply_fully_decoded: false,
            reset_requested: self.reset_requested,
            response_abandoned: self.response_abandoned,
            abort_requested: self.abort_requested,
            driver_exit: self.driver_exit,
        }
    }
    pub(crate) fn require_closed_ack(
        &self,
        actual_read: &RootResultRead,
        reply: &RootResultReply,
    ) -> Result<()> {
        self.proof.require_closed_ack(actual_read, reply)
    }
    fn fail(&mut self, class: FailureClass, cause: anyhow::Error) -> HeldFailure {
        self.first_failure.get_or_insert(class);
        HeldFailure {
            class,
            observation: self.snapshot(),
            cause,
            additional_actual_source: None,
        }
    }
    /// Do not place this outer owner inside the timed/cancellable future.
    pub(crate) async fn start(&mut self) -> std::result::Result<HeldObservation, HeldFailure> {
        if self.phase != Phase::Prepared || self.first_failure.is_some() {
            return Err(self.fail(
                FailureClass::Transition,
                anyhow::anyhow!("held start repeated"),
            ));
        }
        self.phase = Phase::Opening;
        let deadline = self.deadline;
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            before(deadline)?;
            let connector = self
                .connector
                .take()
                .context("held original connector is absent")?;
            let io = connector.connect().await?;
            self.attach_and_capture(io).await
        })
        .await;
        self.finish_start(result)
    }
    fn finish_start(
        &mut self,
        result: std::result::Result<Result<()>, tokio::time::error::Elapsed>,
    ) -> std::result::Result<HeldObservation, HeldFailure> {
        match result {
            Ok(Ok(())) => {
                if let Err(error) = before(self.deadline) {
                    return Err(self.fail(FailureClass::Clock, error));
                }
                self.phase = Phase::Held;
                Ok(self.snapshot())
            }
            Ok(Err(error)) => Err(self.fail(FailureClass::Transport, error)),
            Err(error) => Err(self.fail(FailureClass::Clock, error.into())),
        }
    }
    async fn attach_and_capture(&mut self, io: BoxedIo) -> Result<()> {
        let (sender, connection) = client::handshake(io).await?;
        // No await/fallible call between spawn and installation of its original handle.
        self.driver = Some(tokio::spawn(connection));
        self.driver_exit = DriverExit::Live;
        self.sender = Some(sender);
        before(self.deadline)?;
        poll_fn(|cx| {
            self.sender
                .as_mut()
                .expect("original sender")
                .poll_ready(cx)
        })
        .await?;
        let request = Request::builder()
            .method("POST")
            .uri(ROOT_PATH)
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("te", "trailers")
            .header(
                header::AUTHORIZATION,
                self.authorization.take().context("original auth absent")?,
            )
            .body(())?;
        let (response, send) = self
            .sender
            .as_mut()
            .expect("original sender")
            .send_request(request, false)?;
        self.request_stream = Some(send);
        self.request_stream
            .as_mut()
            .expect("original request stream")
            .send_data(
                self.request_frame
                    .take()
                    .context("original request frame absent")?,
                true,
            )?;
        let response = response.await?;
        ensure!(
            response.status() == http::StatusCode::OK,
            "held root HTTP status differs"
        );
        grpc_content_type(response.headers())?;
        ensure!(
            !response.headers().contains_key("grpc-status"),
            "held DATA candidate has an initial gRPC status"
        );
        self.response = Some(response.into_body());
        while self.capture.declared.is_none() {
            let data = self
                .response
                .as_mut()
                .expect("original response")
                .data()
                .await
                .context("held response ended before its gRPC prefix")??;
            let result = self.capture.push(&data);
            // No Bytes clone/slice, no release_capacity, no remaining body read.
            drop(data);
            self.withheld = self
                .response
                .as_mut()
                .expect("original response")
                .flow_control()
                .used_capacity();
            result?;
            before(self.deadline)?;
        }
        ensure!(
            self.withheld >= usize::try_from(self.capture.received)? && self.withheld > 0,
            "held response has no actual withheld stream credit"
        );
        ensure!(
            !self
                .response
                .as_ref()
                .expect("original response")
                .is_end_stream(),
            "held response already ended"
        );
        ensure!(
            !self.driver.as_ref().expect("original driver").is_finished(),
            "original driver exited before held observation"
        );
        Ok(())
    }
    /// No read, credit release, reconnect or new Root request occurs here.
    pub(crate) async fn observe_held(
        &mut self,
    ) -> std::result::Result<HeldObservation, HeldFailure> {
        if self.phase != Phase::Held || self.first_failure.is_some() {
            return Err(self.fail(
                FailureClass::Transition,
                anyhow::anyhow!("original response is not held"),
            ));
        }
        if let Err(error) = before(self.deadline) {
            return Err(self.fail(FailureClass::Clock, error));
        }
        if self.driver.as_ref().is_some_and(JoinHandle::is_finished) {
            // Borrow the same original handle until its actual result is obtained.
            let joined = self.driver.as_mut().expect("original driver").await;
            self.driver.take();
            let cause = match joined {
                Ok(Ok(())) => {
                    self.driver_exit = DriverExit::Closed;
                    anyhow::anyhow!("original driver exited early")
                }
                Ok(Err(error)) => {
                    self.driver_exit = DriverExit::ConnectionFailed;
                    error.into()
                }
                Err(error) => {
                    self.driver_exit = DriverExit::JoinFailed;
                    error.into()
                }
            };
            return Err(self.fail(FailureClass::Driver, cause));
        }
        let response = self.response.as_mut().expect("held original response");
        if response.is_end_stream() {
            return Err(self.fail(
                FailureClass::Framing,
                anyhow::anyhow!("held original response ended"),
            ));
        }
        self.withheld = response.flow_control().used_capacity();
        if self.withheld < self.capture.received as usize {
            return Err(self.fail(
                FailureClass::Framing,
                anyhow::anyhow!("original withheld credit disappeared"),
            ));
        }
        if let Err(error) = before(self.deadline) {
            return Err(self.fail(FailureClass::Clock, error));
        }
        Ok(self.snapshot())
    }
    /// Actual cleanup, never a success inferred from Drop/reset/abort request.
    /// Abort+await is unconditional; expiry is a failure before/after cleanup,
    /// not a fresh cleanup/success clock. No timeout drops the original handle.
    pub(crate) async fn settle(
        &mut self,
    ) -> std::result::Result<JoinedOriginalDriver, HeldFailure> {
        if self.phase == Phase::Settled {
            return Err(self.fail(
                FailureClass::Transition,
                anyhow::anyhow!("original driver settled twice"),
            ));
        }
        // Only an unfinished borrowed settle leaves this phase. Re-entry
        // still joins the same handle but cannot turn cancellation into PASS.
        if self.phase == Phase::Settling {
            self.first_failure.get_or_insert(FailureClass::Transition);
        }
        let mut clock_error = before(self.deadline).err();
        // Cleanup is not a final held observation: an already exited driver
        // must stay failed even when joining its original handle returns Ok.
        if self.phase == Phase::Held {
            if self.driver.as_ref().is_none_or(JoinHandle::is_finished) {
                self.first_failure.get_or_insert(FailureClass::Driver);
            }
            if let Some(response) = self.response.as_mut() {
                self.withheld = response.flow_control().used_capacity();
                if response.is_end_stream() || self.withheld < self.capture.received as usize {
                    self.first_failure.get_or_insert(FailureClass::Framing);
                }
            } else {
                self.first_failure.get_or_insert(FailureClass::Framing);
            }
        }
        if self.phase == Phase::Opening {
            self.first_failure.get_or_insert(FailureClass::Transition);
        }
        self.phase = Phase::Settling;
        if let Some(send) = self.request_stream.as_mut() {
            send.send_reset(h2::Reason::CANCEL);
            self.reset_requested = true;
        }
        self.response_abandoned |= self.response.is_some();
        drop(self.response.take());
        drop(self.request_stream.take());
        drop(self.sender.take());
        drop(self.request_frame.take());
        drop(self.authorization.take());
        drop(self.connector.take());
        let mut actual_expected_cancellation = None;
        let mut driver_error = None;
        if let Some(driver) = self.driver.as_mut() {
            driver.abort();
            self.abort_requested = true;
            #[cfg(test)]
            if let Some(hold) = self.settle_before_join.take() {
                // The original handle remains in self.driver across this
                // cancellable hold; no wrapper task or alternate handle.
                hold.entered.notify_one();
                hold.release.notified().await;
            }
            let actual = driver.await;
            self.driver.take();
            match actual {
                Ok(Ok(())) => self.driver_exit = DriverExit::Closed,
                Ok(Err(error)) => {
                    self.driver_exit = DriverExit::ConnectionFailed;
                    driver_error = Some(error.into());
                }
                Err(error) if error.is_cancelled() => {
                    self.driver_exit = DriverExit::AbortedAndJoined;
                    actual_expected_cancellation = Some(error);
                }
                Err(error) => {
                    self.driver_exit = DriverExit::JoinFailed;
                    driver_error = Some(error.into());
                }
            }
        }
        self.phase = Phase::Settled;
        if let Err(error) = before(self.deadline) {
            clock_error.get_or_insert(error);
        }
        if let Some(error) = driver_error {
            let mut failure = self.fail(FailureClass::Driver, error);
            failure.additional_actual_source = clock_error;
            return Err(failure);
        }
        if let Some(error) = clock_error {
            let mut failure = self.fail(FailureClass::Clock, error);
            failure.additional_actual_source = actual_expected_cancellation.map(Into::into);
            return Err(failure);
        }
        if self.first_failure.is_some() || self.driver_exit == DriverExit::NotSpawned {
            let mut failure = self.fail(
                FailureClass::PriorFailure,
                anyhow::anyhow!("original held attempt failed or never spawned a driver"),
            );
            failure.additional_actual_source = actual_expected_cancellation.map(Into::into);
            return Err(failure);
        }
        Ok(JoinedOriginalDriver {
            observation: self.snapshot(),
            actual_expected_cancellation,
        })
    }
}
impl Drop for HeldRootResponse {
    fn drop(&mut self) {
        // Emergency abort only. This branch produces NO joined/success receipt.
        // The scene must explicitly settle the same outer owner before dropping it.
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}
fn before(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "original held-response deadline expired"
    );
    Ok(())
}
fn request_frame(request: &wire::FetchRootResultRequest) -> Result<Bytes> {
    let size = request.encoded_len();
    ensure!(
        size.checked_add(5).is_some_and(|n| n <= REQUEST_CAP),
        "held replay request exceeds original frame bound"
    );
    let mut frame = Vec::with_capacity(REQUEST_CAP);
    frame.push(0);
    frame.extend_from_slice(&u32::try_from(size)?.to_be_bytes());
    request.encode(&mut frame)?;
    Ok(Bytes::from(frame))
}

#[cfg(test)]
#[path = "result_delivery_root_held_response_tests.rs"]
mod tests;
