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

// Host components only. These tests do not assert a BE producer, backing
// last-alias, authenticated Native role, or a completed Native scene.

use super::*;
use novarocks_execution_contract::root_result::{RootResultData, RootResultEnd};
use novarocks_task_codec::root_result::{encode_read, encode_reply};
use std::num::NonZeroU64;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::io::DuplexStream;

fn root() -> TaskIdentity {
    let wire = super::super::parse_created_line(&format!(
        "{} execution_id=-7:23:2 stage=9 task=17 backend=019a0203-0405-7000-8000-000000000001",
        novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED
    ))
    .unwrap();
    novarocks_task_codec::identity::decode_task_identity(
        &wire,
        FieldPath::root("held_component_root"),
    )
    .unwrap()
}
fn read(wanted: Option<u64>, consumed: u64) -> RootResultRead {
    RootResultRead::try_new(
        root(),
        RootProfileId::V1,
        RootOutputKind::ClientRows,
        wanted.map(|n| NonZeroU64::new(n).unwrap()),
        consumed,
        Duration::from_millis(100),
    )
    .unwrap()
}
fn data1() -> RootResultReply {
    let mut bytes = vec![b'x'; S];
    // Literal native row prefix + MySQL string prefix from the immutable input.
    bytes[..8].copy_from_slice(&[4, 0, 16, 0, 253, 0, 0, 16]);
    RootResultReply {
        root_task: root(),
        profile: RootProfileId::V1,
        kind: RootOutputKind::ClientRows,
        accepted_consumed: 0,
        outcome: RootReadOutcome::Data(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                NonZeroU64::new(1).unwrap(),
                Bytes::from(bytes),
                None,
            )
            .unwrap(),
        ),
    }
}
fn actor(deadline: Instant) -> HeldRootResponse {
    let original = read(Some(1), 0);
    let proof = ProvenReplayOne::from_validated_data1(&original, &data1()).unwrap();
    HeldRootResponse::new(
        proof,
        deadline,
        None,
        HeaderValue::from_static("Bearer host-component"),
        request_frame(&encode_read(&original)).unwrap(),
        0,
        1,
    )
}
fn prefix(length: u32) -> [u8; 5] {
    let mut out = [0; 5];
    out[1..].copy_from_slice(&length.to_be_bytes());
    out
}

#[test]
fn literal_data1_proof_and_actual_typed_request_are_required() {
    let original = read(Some(1), 0);
    let reply = data1();
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(match &reply.outcome {
            RootReadOutcome::Data(data) => data.body(),
            _ => unreachable!(),
        })),
        DATA1_SHA
    );
    assert!(ProvenReplayOne::from_validated_data1(&original, &reply).is_ok());
    assert!(ProvenReplayOne::from_validated_data1(&read(Some(2), 0), &reply).is_err());
    let mut wrong = reply.clone();
    wrong.accepted_consumed = 1;
    assert!(ProvenReplayOne::from_validated_data1(&original, &wrong).is_err());
    let mut wrong = reply.clone();
    wrong.kind = RootOutputKind::CountOnly;
    assert!(ProvenReplayOne::from_validated_data1(&original, &wrong).is_err());
    let mut wrong = reply.clone();
    wrong.outcome = RootReadOutcome::Data(
        RootResultData::try_new(
            RootOutputKind::ClientRows,
            NonZeroU64::new(1).unwrap(),
            Bytes::from(vec![b'y'; S]),
            None,
        )
        .unwrap(),
    );
    assert!(ProvenReplayOne::from_validated_data1(&original, &wrong).is_err());
    let mut wrong = reply;
    wrong.outcome = RootReadOutcome::Data(
        RootResultData::try_new(
            RootOutputKind::ClientRows,
            NonZeroU64::new(1).unwrap(),
            match wrong.outcome {
                RootReadOutcome::Data(data) => data.body().clone(),
                _ => unreachable!(),
            },
            Some(RootResultEnd {
                sequence: NonZeroU64::new(2).unwrap(),
                output_rows: 1,
            }),
        )
        .unwrap(),
    );
    assert!(ProvenReplayOne::from_validated_data1(&original, &wrong).is_err());
}
#[test]
fn closed_ack_requires_same_original_request_and_old_accepted_frontier() {
    let proof = ProvenReplayOne::from_validated_data1(&read(Some(1), 0), &data1()).unwrap();
    let mut closed = RootResultReply {
        root_task: root(),
        profile: RootProfileId::V1,
        kind: RootOutputKind::ClientRows,
        accepted_consumed: 0,
        outcome: RootReadOutcome::AwaitTerminalControl,
    };
    assert!(proof.require_closed_ack(&read(None, 1), &closed).is_ok());
    assert!(proof.require_closed_ack(&read(None, 0), &closed).is_err());
    assert!(
        proof
            .require_closed_ack(&read(Some(1), 1), &closed)
            .is_err()
    );
    closed.accepted_consumed = 1;
    assert!(proof.require_closed_ack(&read(None, 1), &closed).is_err());
    closed.accepted_consumed = 0;
    for outcome in [
        RootReadOutcome::AckOnly,
        RootReadOutcome::NotReady,
        RootReadOutcome::Retired,
    ] {
        closed.outcome = outcome;
        assert!(proof.require_closed_ack(&read(None, 1), &closed).is_err());
    }
    closed.outcome = RootReadOutcome::AwaitTerminalControl;
    closed.root_task = TaskIdentity::new(
        root().query_execution_id(),
        root().stage_id(),
        root().task_id(),
        "019a0203-0405-7000-8000-000000000002".parse().unwrap(),
    );
    assert!(proof.require_closed_ack(&read(None, 1), &closed).is_err());
}
#[test]
fn capture_accepts_every_prefix_split_without_a_complete_reply_claim() {
    let bytes = prefix(S as u32 + 100);
    for cut in 1..5 {
        let mut capture = Capture::new();
        capture.push(&bytes[..cut]).unwrap();
        assert!(capture.declared.is_none());
        capture.push(&bytes[cut..]).unwrap();
        assert_eq!(capture.declared, Some(S as u32 + 100));
        assert_eq!(capture.received, 5);
        assert_eq!(capture.filled, 5);
        let actual: [u8; 32] = capture.hash.finalize().into();
        let expected: [u8; 32] = Sha256::digest(bytes).into();
        assert_eq!(actual, expected);
    }
}
#[test]
fn malformed_or_unbounded_capture_keeps_finite_actual_prefix_counts() {
    let mut capture = Capture::new();
    assert!(capture.push(&[]).is_err());
    assert_eq!(capture.frames, 1);
    let mut capture = Capture::new();
    let mut bytes = prefix(S as u32);
    bytes[0] = 1;
    assert!(capture.push(&bytes).is_err());
    assert_eq!(capture.received, 5);
    let mut capture = Capture::new();
    assert!(capture.push(&prefix(S as u32 - 1)).is_err());
    let mut capture = Capture::new();
    assert!(capture.push(&prefix(RESPONSE_CAP as u32)).is_err());
    let mut capture = Capture::new();
    assert!(capture.push(&vec![1; FRAME_BYTES + 1]).is_err());
    assert_eq!(capture.received, (FRAME_BYTES + 1) as u64);
    assert_eq!(capture.filled, CAPTURE_BYTES);
    assert!(!capture.digest_complete);
    let mut capture = Capture::new();
    capture.frames = CAPTURE_FRAMES;
    assert!(capture.push(&prefix(S as u32)).is_err());
    assert_eq!(capture.frames, CAPTURE_FRAMES + 1);
    let mut capture = Capture::new();
    capture.received = u64::MAX;
    assert!(capture.push(&[0]).is_err());
    assert_eq!(capture.filled, 0);
}
#[test]
fn capture_rejects_complete_or_trailing_message_before_success() {
    let mut capture = Capture::new();
    capture.push(&prefix(S as u32)).unwrap();
    capture.received = S as u64 + 4;
    assert!(capture.push(&[0]).is_err());
    assert_eq!(capture.received, S as u64 + 5);
    let mut capture = Capture::new();
    capture.push(&prefix(S as u32)).unwrap();
    capture.received = S as u64 + 5;
    assert!(capture.push(&[0]).is_err());
}

struct OriginalFrame {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}
impl AsRef<[u8]> for OriginalFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for OriginalFrame {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
fn encoded_original_frame(dropped: Arc<AtomicBool>) -> Bytes {
    // Production codec, no hand-written successful reply protobuf.
    let message = encode_reply(&data1()).unwrap();
    assert!(message.encoded_len() + 5 <= RESPONSE_CAP);
    let mut bytes = Vec::with_capacity(RESPONSE_CAP);
    bytes.extend_from_slice(&prefix(message.encoded_len() as u32));
    message.encode(&mut bytes).unwrap();
    Bytes::from_owner(OriginalFrame { bytes, dropped })
}
async fn real_h2_server(
    io: DuplexStream,
    original_frame: Bytes,
    requests: Arc<AtomicUsize>,
) -> Result<()> {
    let mut connection = h2::server::handshake(io).await?;
    let (request, mut respond) = connection
        .accept()
        .await
        .context("host H2 request absent")??;
    requests.fetch_add(1, Ordering::SeqCst);
    ensure!(
        request.uri().path() == ROOT_PATH && request.method() == "POST",
        "unexpected host request"
    );
    // Keep the original small request's RecvStream while driving this connection.
    let _original_request_body = request.into_body();
    let response = http::Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "application/grpc")
        .body(())?;
    let mut send = respond.send_response(response, false)?;
    send.send_data(original_frame, false)?;
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    send.send_trailers(trailers)?;
    if let Some(next) = connection.accept().await {
        let _ = next?;
        requests.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("actor issued another host H2 request");
    }
    Ok(())
}
async fn begin_with_original_io(
    actor: &mut HeldRootResponse,
    io: DuplexStream,
) -> std::result::Result<HeldObservation, HeldFailure> {
    actor.phase = Phase::Opening;
    let result = tokio::time::timeout_at(
        tokio::time::Instant::from_std(actor.deadline),
        actor.attach_and_capture(Box::new(io)),
    )
    .await;
    actor.finish_start(result)
}
// Abort+actual await is component server cleanup, not a BE/role exit assertion.
async fn join_component_server(
    server: JoinHandle<Result<()>>,
) -> std::result::Result<Result<()>, JoinError> {
    server.abort();
    server.await
}

#[tokio::test]
async fn original_h2_recvstream_holds_credit_and_original_encoded_owner_until_actual_join() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let dropped = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(real_h2_server(
        server_io,
        encoded_original_frame(dropped.clone()),
        requests.clone(),
    ));
    let mut actor = actor(deadline);
    let held = begin_with_original_io(&mut actor, client_io).await;
    let observation = actor.observe_held().await;
    let owned_while_held = !dropped.load(Ordering::SeqCst);
    let connection_was_live = actor
        .connection_job
        .as_ref()
        .is_some_and(|handle| !handle.is_finished());
    let settled = actor.settle().await;
    let server_join = join_component_server(server).await;
    // Assertions follow the actual original client/server joins on every ordinary path.
    let held = held.unwrap();
    observation.unwrap();
    let settled = settled.unwrap();
    assert!(held.received_bytes > 0 && held.received_bytes <= FRAME_BYTES as u64);
    assert!(held.withheld_stream_credit_bytes >= held.received_bytes as usize);
    assert_eq!(held.released_stream_credit_bytes, 0);
    assert!(!held.reply_fully_decoded);
    assert!(owned_while_held && connection_was_live);
    assert!(matches!(
        settled.observation.connection_exit,
        ConnectionExit::AbortedAndJoined | ConnectionExit::Closed
    ));
    assert!(actor.connection_job.is_none());
    assert!(actor.response.is_none());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert!(dropped.load(Ordering::SeqCst));
    match server_join {
        Ok(result) => result.unwrap(),
        Err(error) => assert!(error.is_cancelled()),
    }
}
#[tokio::test]
async fn dropped_borrowed_start_keeps_the_same_original_connection_job_for_failed_actual_cleanup() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        let mut connection = h2::server::handshake(server_io).await.unwrap();
        let _request = connection.accept().await.unwrap().unwrap();
        // Headers intentionally withheld; drive the same original connection.
        while connection.accept().await.is_some() {}
    });
    let mut actor = actor(deadline);
    let timed = tokio::time::timeout(
        Duration::from_millis(30),
        begin_with_original_io(&mut actor, client_io),
    )
    .await;
    let installed = actor.connection_job.is_some();
    let cleaned = actor.settle().await;
    server.abort();
    let joined = server.await;
    assert!(timed.is_err() && installed);
    let error = cleaned.unwrap_err();
    assert_eq!(error.observation.phase, Phase::Settled);
    assert!(actor.connection_job.is_none());
    assert!(error.observation.abort_requested);
    if let Err(error) = joined {
        assert!(error.is_cancelled());
    }
}
#[tokio::test]
async fn expired_original_clock_still_joins_but_cannot_mint_success() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let dropped = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(real_h2_server(
        server_io,
        encoded_original_frame(dropped.clone()),
        requests,
    ));
    let mut actor = actor(deadline);
    let held = begin_with_original_io(&mut actor, client_io).await;
    actor.deadline = Instant::now(); // Host-only deterministic clock-expiry seam.
    let result = actor.settle().await;
    let actual_server_join = join_component_server(server).await;
    // Actual unexpected server panic/H2 failure must not be silently discarded.
    match actual_server_join {
        Ok(result) => result.unwrap(),
        Err(error) => assert!(error.is_cancelled()),
    }
    held.unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.class, FailureClass::Clock);
    assert!(actor.connection_job.is_none() && dropped.load(Ordering::SeqCst));
    assert_eq!(error.observation.phase, Phase::Settled);
}
#[tokio::test]
async fn actual_original_connection_job_panic_source_is_owned_and_not_formatted() {
    let mut actor = actor(Instant::now() + Duration::from_secs(5));
    actor.phase = Phase::Held;
    actor.connection_job = Some(tokio::spawn(async {
        panic!("host synthetic original connection job panic");
        #[allow(unreachable_code)]
        Ok(())
    }));
    actor.connection_exit = ConnectionExit::Live;
    // Observe the same original task's completed result through the actor.
    // This fault-injection task is not a BE or an H2-connection job Native claim.
    tokio::task::yield_now().await;
    let observed = actor.observe_held().await;
    let cleaned = actor.settle().await;
    let failure = observed.unwrap_err();
    assert!(
        failure
            .cause
            .downcast_ref::<JoinError>()
            .unwrap()
            .is_panic()
    );
    assert_eq!(
        failure.to_string(),
        "original held root response failed; actual sources retained"
    );
    assert!(cleaned.is_err());
    assert!(actor.connection_job.is_none());
}
#[tokio::test]
async fn already_finished_original_connection_job_is_failure_even_after_successful_actual_join() {
    let mut actor = actor(Instant::now() + Duration::from_secs(5));
    actor.phase = Phase::Held;
    actor.connection_job = Some(tokio::spawn(async { Ok(()) }));
    actor.connection_exit = ConnectionExit::Live;
    tokio::task::yield_now().await;
    let actual = actor.settle().await;
    assert!(actual.is_err());
    assert!(actor.connection_job.is_none());
    assert_eq!(
        actual.unwrap_err().observation.connection_exit,
        ConnectionExit::Closed
    );
}

#[tokio::test]
async fn deterministic_borrowed_settle_cancel_keeps_original_handle_and_retry_stays_failed() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let dropped = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(real_h2_server(
        server_io,
        encoded_original_frame(dropped.clone()),
        requests,
    ));
    let mut actor = actor(deadline);
    let held = begin_with_original_io(&mut actor, client_io).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    actor.settle_before_join = Some(SettleHold {
        entered: entered.clone(),
        release,
    });
    // No sleeps/races are used to infer where cancellation landed. The private
    // barrier can only notify after abort_requested and before the original
    // borrowed JoinHandle await; no production ownership/configuration changes.
    let mut unexpected_early_result = None;
    let cancelled_at_original_join = {
        let settling = actor.settle();
        tokio::pin!(settling);
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            tokio::select! {
                biased;
                _ = entered.notified() => true,
                result = &mut settling => { unexpected_early_result = Some(result); false }
            }
        })
        .await
        // Leaving this scope drops only the borrowed settle future.
    };
    let handle_still_owned = actor.connection_job.is_some();
    let interrupted_phase = actor.phase;
    let abort_had_been_requested = actor.abort_requested;
    let retry = actor.settle().await;
    let actual_server_join = join_component_server(server).await;
    match actual_server_join {
        Ok(result) => result.unwrap(),
        Err(error) => assert!(error.is_cancelled()),
    }
    held.unwrap();
    assert!(cancelled_at_original_join.unwrap() && unexpected_early_result.is_none());
    assert!(handle_still_owned && abort_had_been_requested);
    assert_eq!(interrupted_phase, Phase::Settling);
    let failure = retry.unwrap_err();
    assert_eq!(
        failure.observation.first_failure,
        Some(FailureClass::Transition)
    );
    assert_eq!(failure.observation.phase, Phase::Settled);
    assert!(actor.connection_job.is_none() && dropped.load(Ordering::SeqCst));
    assert!(matches!(
        failure.observation.connection_exit,
        ConnectionExit::AbortedAndJoined | ConnectionExit::Closed
    ));
}
