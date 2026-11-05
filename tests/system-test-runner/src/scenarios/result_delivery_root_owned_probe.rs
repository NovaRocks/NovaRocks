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

//! Opt-in same strict probe with original connection job retained outside timeout.
use super::*;

pub(crate) fn probe_owned(
    context: &mut ScenarioContext,
    backend: usize,
    request: &proto::FetchRootResultRequest,
    maximum_delivered_consumed: u64,
    deadline: Instant,
) -> Result<(RootResultReply, serde_json::Value)> {
    ensure!(
        maximum_delivered_consumed != u64::MAX,
        "root probe requires a finite proven consumption watermark"
    );
    let expected = decode_read(request, FieldPath::root("root_probe_request"))?;
    ensure!(
        request.consumed_sequence <= maximum_delivered_consumed,
        "root probe ACK exceeds its proven delivery watermark"
    );
    let message = request.encode_to_vec();
    let frame_len = message
        .len()
        .checked_add(5)
        .context("root request length overflow")?;
    ensure!(
        frame_len <= REQUEST_CAP,
        "root request frame exceeds its bound"
    );
    let mut frame = Vec::with_capacity(frame_len);
    frame.push(0);
    frame.extend_from_slice(&u32::try_from(message.len())?.to_be_bytes());
    frame.extend_from_slice(&message);

    let handle = context.handle();
    let mode = handle.native_trust_mode();
    let advertised = handle.native_be_endpoint(backend)?;
    ensure!(
        mode == NativeTrustFixtureMode::Plaintext
            && advertised.host().parse::<std::net::IpAddr>().is_ok(),
        "installed root probe requires plaintext and an IP endpoint"
    );
    let port = handle
        .runtime()
        .be
        .get(backend)
        .context("invalid root backend index")?
        .grpc;
    let endpoint =
        NativeEndpoint::from_host_port(advertised.host(), port).map_err(anyhow::Error::msg)?;
    let connector = handle.native_probe_connector(endpoint, mode)?;
    let mut authorization_request = tonic::Request::new(());
    handle
        .native_probe_trust()?
        .apply_client_authorization(authorization_request.metadata_mut())
        .map_err(anyhow::Error::new)?;
    let authorization = authorization_request
        .metadata()
        .get("authorization")
        .context("root probe authorization was not issued")?
        .to_str()
        .context("root probe authorization is not ASCII")?
        .to_owned();
    let remaining = context.remaining("installed root protocol probe")?;
    let deadline = deadline.min(Instant::now() + remaining);
    ensure!(
        Instant::now() < deadline,
        "root protocol probe deadline expired"
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // The actual original handle is outside the timed future on every path.
    let mut original_connection_job = None;
    let outcome = runtime.block_on(async {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            let stream = connector
                .connect()
                .await
                .map_err(anyhow::Error::new)
                .context("connect actual root BE listener")?;
            let (mut sender, connection) = client::handshake(stream)
                .await
                .context("handshake root probe HTTP/2")?;
            original_connection_job = Some(tokio::spawn(connection));
            let request = Request::builder()
                .method("POST")
                .uri(ROOT_PATH)
                .header(header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .header(header::AUTHORIZATION, authorization)
                .body(())?;
            let (response, mut send) = sender.send_request(request, false)?;
            send.send_data(Bytes::from(frame), true)?;
            drop(send);
            let response = response.await.context("receive root probe response")?;
            ensure!(
                response.status() == http::StatusCode::OK,
                "root probe HTTP status is not 200"
            );
            grpc_content_type(response.headers())?;
            let header_status = candidate_status(response.headers())?;
            let mut body = response.into_body();
            let mut bytes = Vec::with_capacity(RESPONSE_CAP);
            let mut saw_data = false;
            while let Some(chunk) = body.data().await {
                let chunk = chunk.context("read root probe DATA")?;
                saw_data = true;
                let length = bytes
                    .len()
                    .checked_add(chunk.len())
                    .context("root response length overflow")?;
                ensure!(
                    length <= RESPONSE_CAP,
                    "root response DATA exceeds its bound"
                );
                body.flow_control().release_capacity(chunk.len())?;
                bytes.extend_from_slice(&chunk);
            }
            let trailers = body.trailers().await.context("read root probe trailers")?;
            let trailer_status = trailers
                .as_ref()
                .map(candidate_status)
                .transpose()?
                .flatten();
            let status = response_status(header_status, trailer_status, &bytes, saw_data)?;
            drop(body);
            drop(sender);
            Ok::<_, anyhow::Error>((bytes, status))
        })
        .await
        .context("absolute root protocol probe deadline exceeded")?
    });
    let cleanup = runtime.block_on(settle_original_connection(&mut original_connection_job));
    let (frame, status) = assemble_probe_outcome(outcome, cleanup, deadline)?;
    ensure!(
        status == ProbeStatus::Success,
        "original root transport did not succeed"
    );
    let message = single_message(&frame)?;
    let wire =
        proto::FetchRootResultResponse::decode(message).context("decode root response protobuf")?;
    let response = decode_response(
        wire,
        &expected,
        maximum_delivered_consumed,
        FieldPath::root("root_probe_reply"),
    )?;
    let RootReadResponse::Reply(reply) = response else {
        anyhow::bail!("independently selected original root supplied no ACK receipt: {response:?}");
    };
    let (outcome, sequence, body_bytes, body_sha256) = match &reply.outcome {
        RootReadOutcome::Data(data) => (
            "data",
            Some(data.sequence().get()),
            data.body().len(),
            Some(format!("{:x}", Sha256::digest(data.body()))),
        ),
        RootReadOutcome::End(end) => ("end", Some(end.sequence.get()), 0, None),
        RootReadOutcome::AckOnly => ("ack_only", None, 0, None),
        RootReadOutcome::NotReady => ("not_ready", None, 0, None),
        RootReadOutcome::Retired => ("retired", None, 0, None),
        RootReadOutcome::AwaitTerminalControl => ("await_terminal_control", None, 0, None),
    };
    let end = match &reply.outcome {
        RootReadOutcome::Data(data) => data.end_after_data(),
        RootReadOutcome::End(end) => Some(*end),
        _ => None,
    };
    let observation = serde_json::json!({
        "backend_index": backend, "actual_grpc_port": port,
        "response_frame_bytes": frame.len(), "body_bytes": body_bytes,
        "body_sha256": body_sha256, "outcome": outcome, "sequence": sequence,
        "accepted_consumed_sequence": reply.accepted_consumed,
        "end": end.map(|end| serde_json::json!({"sequence": end.sequence.get(), "output_rows": end.output_rows})),
        "grpc_status": 0, "original_driver_actual_join":true, "identity": identity_observation(reply.root_task),
        "request_identity": identity_observation(expected.root_task())
    });
    ensure!(
        Instant::now() < deadline,
        "root protocol probe decoding exceeded its deadline"
    );
    Ok((reply, observation))
}

// Private assembly only: preserve the original three-slot order and check
// the same absolute clock after the actual original connection job has been joined.
fn assemble_probe_outcome<T>(
    outcome: Result<T>,
    cleanup: Result<()>,
    deadline: Instant,
) -> Result<T> {
    let late = (Instant::now() >= deadline)
        .then(|| anyhow::anyhow!("original probe actual settlement was late"));
    let mut errors = [None, cleanup.err(), late];
    let result = match outcome {
        Ok(value) => Some(value),
        Err(error) => {
            errors[0] = Some(error);
            None
        }
    };
    if errors.iter().any(Option::is_some) {
        return Err(OwnedProbeFailure { errors }.into());
    }
    Ok(result.expect("validated original probe result"))
}

struct OwnedProbeFailure {
    errors: [Option<anyhow::Error>; 3],
}
impl std::fmt::Debug for OwnedProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedProbeFailure")
            .field("failed_slots", &self.errors.each_ref().map(Option::is_some))
            .finish()
    }
}
impl std::fmt::Display for OwnedProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original root probe failed; original sources retained")
    }
}
impl std::error::Error for OwnedProbeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.errors
            .iter()
            .flatten()
            .next()
            .map(|error| error.as_ref())
    }
}

async fn settle_original_connection(
    original: &mut Option<tokio::task::JoinHandle<std::result::Result<(), h2::Error>>>,
) -> Result<()> {
    if let Some(handle) = original.as_mut() {
        handle.abort();
        let joined = handle.await;
        original.take(); // actual result was obtained before releasing the original position
        match joined {
            Ok(Ok(())) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(anyhow::Error::new(error)),
            Err(error) => Err(anyhow::Error::new(error)),
        }
    } else {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn abort_and_join_retains_original_handle_until_actual_exit() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut original = Some(tokio::spawn(async move {
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            Ok::<(), h2::Error>(())
        }));
        started_rx.await.unwrap();
        settle_original_connection(&mut original).await.unwrap();
        assert!(original.is_none());
    }
    #[tokio::test]
    async fn original_panic_is_retained_instead_of_discarded_as_abort() {
        let mut original = Some(tokio::spawn(async {
            panic!("original connection job panic");
            #[allow(unreachable_code)]
            Ok::<(), h2::Error>(())
        }));
        // Waiting on is_finished does not consume its actual JoinError.
        let observed_exit = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !original.as_ref().unwrap().is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let result = settle_original_connection(&mut original).await;
        assert!(observed_exit.is_ok());
        let error = result.unwrap_err();
        assert!(
            error
                .downcast_ref::<tokio::task::JoinError>()
                .unwrap()
                .is_panic()
        );
        assert!(original.is_none());
    }
}

#[cfg(test)]
mod extra_tests {
    use super::*;
    use std::error::Error as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    struct ActualFutureExit(Arc<AtomicBool>);
    impl Drop for ActualFutureExit {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    fn fixed_failure(error: anyhow::Error) -> OwnedProbeFailure {
        match error.downcast::<OwnedProbeFailure>() {
            Ok(failure) => failure,
            Err(_) => panic!("original error assembly must retain its fixed failure type"),
        }
    }

    #[tokio::test]
    async fn timed_wait_retains_the_same_original_handle_until_actual_abort_and_join() {
        // A no-I/O original-handle lifecycle fixture, not an h2/Native result.
        let exited = Arc::new(AtomicBool::new(false));
        let guard = ActualFutureExit(Arc::clone(&exited));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut original = Some(tokio::spawn(async move {
            let _guard = guard;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
            Ok::<(), h2::Error>(())
        }));
        let original_id = original.as_ref().unwrap().id();
        let deadline = Instant::now() + Duration::from_millis(50);
        let started = tokio::time::timeout_at(deadline.into(), started_rx).await;
        // Borrowing timeout cannot consume or detach the original handle.
        let wait = tokio::time::timeout_at(deadline.into(), original.as_mut().unwrap()).await;
        let retained_id = original.as_ref().map(tokio::task::JoinHandle::id);
        let not_exited_before_cleanup = !exited.load(Ordering::SeqCst);
        let cleanup = settle_original_connection(&mut original).await;
        let no_original_after_join = original.is_none();
        let actual_exit_after_join = exited.load(Ordering::SeqCst);
        let primary = match wait {
            Err(elapsed) => anyhow::Error::new(elapsed),
            Ok(_) => anyhow::anyhow!("pending original unexpectedly completed"),
        };
        let primary_address = primary
            .downcast_ref::<tokio::time::error::Elapsed>()
            .map(|source| source as *const tokio::time::error::Elapsed);
        let assembled = assemble_probe_outcome::<()>(Err(primary), cleanup, deadline);
        // All assertions occur after the same actual handle has been joined.
        assert!(matches!(started, Ok(Ok(()))));
        assert_eq!(retained_id, Some(original_id));
        assert!(not_exited_before_cleanup);
        assert!(no_original_after_join && actual_exit_after_join);
        let failure = fixed_failure(assembled.unwrap_err());
        assert_eq!(
            failure.errors.each_ref().map(Option::is_some),
            [true, false, true]
        );
        let actual_elapsed = failure.errors[0]
            .as_ref()
            .unwrap()
            .downcast_ref::<tokio::time::error::Elapsed>()
            .unwrap();
        assert_eq!(Some(actual_elapsed as *const _), primary_address);
    }

    struct FormatCanary(Arc<AtomicUsize>);
    impl std::fmt::Debug for FormatCanary {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("actual source Debug must not be used by fixed formatter")
        }
    }
    impl std::fmt::Display for FormatCanary {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("actual source Display must not be used by fixed formatter")
        }
    }
    impl std::error::Error for FormatCanary {}

    #[tokio::test]
    async fn actual_tcp_h2_connection_error_and_primary_sources_survive_fixed_assembly() {
        // Only the original h2 Connection future is spawned. The peer socket
        // remains owned by this parent; no wrapping or peer connection job exists.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut original = None;
        let operation = tokio::time::timeout_at(deadline.into(), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let (socket, accepted) =
                tokio::try_join!(tokio::net::TcpStream::connect(address), listener.accept())?;
            let (mut peer, _) = accepted;
            let (sender, connection) = h2::client::handshake(socket).await?;
            original = Some(tokio::spawn(connection));
            // Actual bytes: non-ACK SETTINGS with one byte (not a 6B entry).
            // This is a library protocol error, never Error::from(Reason).
            peer.write_all(&[0, 0, 1, 4, 0, 0, 0, 0, 0, 0]).await?;
            while !original.as_ref().unwrap().is_finished() {
                tokio::task::yield_now().await;
            }
            drop(sender);
            drop(peer);
            Ok::<(), anyhow::Error>(())
        })
        .await;
        // A timeout/read/write failure still aborts and awaits the SAME handle.
        let cleanup = settle_original_connection(&mut original).await;
        let actually_joined = original.is_none();
        // Cleanup precedes every assertion and actual-source extraction.
        assert!(
            operation.is_ok_and(|value| value.is_ok()),
            "actual h2 error fixture did not finish"
        );
        assert!(actually_joined);
        let cleanup = match cleanup {
            Err(error) => error,
            Ok(()) => panic!("actual malformed frame must produce an h2 connection error"),
        };
        let original_h2 = cleanup
            .downcast_ref::<h2::Error>()
            .expect("actual h2 connection error");
        let h2_address = original_h2 as *const h2::Error;
        assert!(!original_h2.is_io());
        assert!(original_h2.is_go_away() && original_h2.is_library());
        assert_eq!(original_h2.reason(), Some(h2::Reason::PROTOCOL_ERROR));

        // Original primary IO object and its original custom source are created
        // outside h2; locked h2 may stringify its own IO causes (see notes).
        let format_calls = Arc::new(AtomicUsize::new(0));
        let primary = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            FormatCanary(Arc::clone(&format_calls)),
        ));
        let primary_address =
            primary.downcast_ref::<std::io::Error>().unwrap() as *const std::io::Error;
        let failure = fixed_failure(
            assemble_probe_outcome::<()>(Err(primary), Err(cleanup), deadline).unwrap_err(),
        );
        assert_eq!(
            failure.errors.each_ref().map(Option::is_some),
            [true, true, false]
        );
        let retained_primary = failure.errors[0]
            .as_ref()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(retained_primary as *const _, primary_address);
        let retained_canary = retained_primary
            .get_ref()
            .unwrap()
            .downcast_ref::<FormatCanary>()
            .unwrap();
        assert!(Arc::ptr_eq(&retained_canary.0, &format_calls));
        assert_eq!(
            failure.errors[1]
                .as_ref()
                .unwrap()
                .downcast_ref::<h2::Error>()
                .unwrap() as *const _,
            h2_address
        );
        assert_eq!(
            failure
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap() as *const _,
            primary_address
        );
        let debug = format!("{failure:#?}");
        let display = format!("{failure}");
        assert!(debug.len() < 256 && display.len() < 128);
        let outer = anyhow::Error::new(failure);
        assert_eq!(
            outer.to_string(),
            "original root probe failed; original sources retained"
        );
        let moved = fixed_failure(outer);
        assert_eq!(
            moved.errors[1]
                .as_ref()
                .unwrap()
                .downcast_ref::<h2::Error>()
                .unwrap() as *const _,
            h2_address
        );
        assert_eq!(format_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn actual_successful_original_exit_after_deadline_cannot_turn_late_settlement_into_pass()
    {
        // A no-I/O lifecycle fixture deliberately returns Ok after the original
        // clock. Completing and joining is necessary, but cannot renew it.
        let exited = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_driver = Arc::clone(&completed);
        let guard = ActualFutureExit(Arc::clone(&exited));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut original = Some(tokio::spawn(async move {
            let _guard = guard;
            let _ = release_rx.await;
            completed_in_driver.store(true, Ordering::SeqCst);
            Ok::<(), h2::Error>(())
        }));
        let original_id = original.as_ref().unwrap().id();
        let deadline = Instant::now() + Duration::from_millis(20);
        let elapsed = tokio::time::timeout_at(deadline.into(), std::future::pending::<()>()).await;
        let released = release_tx.send(()).is_ok();
        // This separate component observation watchdog is never an acceptance
        // deadline: the assembly below uses the already expired original clock.
        let observed_completion = tokio::time::timeout(Duration::from_secs(1), async {
            while !original.as_ref().unwrap().is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let retained_id = original.as_ref().map(tokio::task::JoinHandle::id);
        let cleanup = settle_original_connection(&mut original).await;
        let failure = assemble_probe_outcome(Ok(7_u8), cleanup, deadline);
        // Even an observation timeout/panic path is settled before assertions.
        assert!(elapsed.is_err() && released && observed_completion.is_ok());
        assert_eq!(retained_id, Some(original_id));
        assert!(original.is_none());
        assert!(completed.load(Ordering::SeqCst) && exited.load(Ordering::SeqCst));
        let failure = fixed_failure(failure.unwrap_err());
        assert_eq!(
            failure.errors.each_ref().map(Option::is_some),
            [false, false, true]
        );
        assert_eq!(
            failure.errors[2].as_ref().unwrap().to_string(),
            "original probe actual settlement was late"
        );
    }

    #[test]
    fn settled_on_time_success_remains_success_and_cleanup_only_error_remains_primary_source() {
        assert_eq!(
            assemble_probe_outcome(Ok(11), Ok(()), Instant::now() + Duration::from_secs(1))
                .unwrap(),
            11
        );
        let format_calls = Arc::new(AtomicUsize::new(0));
        let cleanup = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            FormatCanary(Arc::clone(&format_calls)),
        ));
        let cleanup_address =
            cleanup.downcast_ref::<std::io::Error>().unwrap() as *const std::io::Error;
        let failure = fixed_failure(
            assemble_probe_outcome(
                Ok(11),
                Err(cleanup),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap_err(),
        );
        assert_eq!(
            failure.errors.each_ref().map(Option::is_some),
            [false, true, false]
        );
        assert_eq!(
            failure
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap() as *const _,
            cleanup_address
        );
        assert!(format!("{failure:?}").len() < 256);
        assert!(format!("{failure}").len() < 128);
        assert_eq!(format_calls.load(Ordering::SeqCst), 0);
    }
}
