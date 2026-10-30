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

use super::*;
use crate::native_transport_admission::{AdmissionDimensions, FrontendOutgoingSnapshot};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot};
use tower::ServiceExt;

const FIXTURE_DEADLINE: Duration = Duration::from_secs(8);

type FixtureResult<T> = Result<T, String>;
type Frames = mpsc::Sender<Result<Bytes, std::io::Error>>;
type HeadReply = oneshot::Sender<hyper::http::Response<PeerBody>>;

fn small_frontend() -> NativeTransportAdmission {
    NativeTransportAdmission::with_parts(
        TransportRole::Frontend,
        AdmissionDimensions {
            data_positions: 2,
            control_positions: 1,
            data_handshakes: 1,
            control_handshakes: 1,
        },
        None,
    )
    .unwrap()
}

async fn bounded<F: Future>(future: F) -> FixtureResult<F::Output> {
    tokio::time::timeout(FIXTURE_DEADLINE, future)
        .await
        .map_err(|_| "outgoing fixture watchdog expired".to_owned())
}

fn request() -> hyper::http::Request<BoxBody> {
    hyper::http::Request::builder()
        .method("POST")
        .uri("/novarocks.NovaRocksGrpc/FetchTaskResult")
        .body(tonic::body::empty_body())
        .unwrap()
}

struct PeerBody(mpsc::Receiver<Result<Bytes, std::io::Error>>);

impl HttpBody for PeerBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|item| item.map(|item| item.map(Frame::data)))
    }
}

// Exactly one listener, one accepted TCP connection, and one original task.
// No detached per-connection task or unbounded JoinSet exists in this fixture.
struct OriginalH2Peer {
    address: SocketAddr,
    heads: mpsc::Receiver<HeadReply>,
    task: tokio::task::JoinHandle<FixtureResult<()>>,
}

impl OriginalH2Peer {
    async fn start() -> FixtureResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "peer bind failed".to_owned())?;
        let address = listener
            .local_addr()
            .map_err(|_| "peer address failed".to_owned())?;
        let (heads_tx, heads) = mpsc::channel(2);
        let task = tokio::spawn(async move {
            let (io, _) = listener
                .accept()
                .await
                .map_err(|_| "peer accept failed".to_owned())?;
            drop(listener);
            let service = hyper::service::service_fn(
                move |_request: hyper::http::Request<hyper::body::Incoming>| {
                    let heads_tx = heads_tx.clone();
                    async move {
                        let (reply, response) = oneshot::channel();
                        heads_tx.send(reply).await.map_err(|_| {
                            std::io::Error::from(std::io::ErrorKind::ConnectionAborted)
                        })?;
                        response.await.map_err(|_| {
                            std::io::Error::from(std::io::ErrorKind::ConnectionAborted)
                        })
                    }
                },
            );
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                .await
                .map_err(|_| "peer connection failed".to_owned())
        });
        Ok(Self {
            address,
            heads,
            task,
        })
    }

    async fn channel(
        &self,
        admission: &NativeTransportAdmission,
    ) -> FixtureResult<NativeLaneChannel> {
        let native_endpoint = novarocks_types::NativeEndpoint::from_socket_addr(self.address);
        let connector = frontend_lane_connector(
            novarocks_native_trust::NativeEndpointConnector::plaintext(native_endpoint),
            admission.clone(),
            FrontendNativeLane::ResultData,
        );
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
            .map_err(|_| "endpoint construction failed".to_owned())?;
        let channel =
            bounded(configure_native_endpoint(endpoint).connect_with_connector(connector))
                .await?
                .map_err(|_| "actual admitted H2 connect failed".to_owned())?;
        Ok(NativeLaneChannel::new(
            channel,
            NativeLane::ResultData,
            Some(admission),
        ))
    }

    async fn stop(self) -> FixtureResult<()> {
        let task = self.task;
        task.abort();
        // This fixture task performs only asynchronous H2 IO. Cleanup must
        // actually join it even after a scenario watchdog failure. The outer
        // test process watchdog is a failure boundary, never an exit receipt.
        match task.await {
            Ok(_) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(_) => Err("original peer task panicked".to_owned()),
        }
    }
}

async fn headers(
    lane: &mut NativeLaneChannel,
    peer: &mut OriginalH2Peer,
) -> FixtureResult<(hyper::http::Response<BoxBody>, Frames)> {
    bounded(lane.ready())
        .await?
        .map_err(|_| "lane ready failed".to_owned())?;
    let response = lane.call(request());
    let serve = async {
        let reply = peer
            .heads
            .recv()
            .await
            .ok_or_else(|| "peer head missing".to_owned())?;
        let (frames, receiver) = mpsc::channel(1);
        if reply
            .send(hyper::http::Response::new(PeerBody(receiver)))
            .is_err()
        {
            return Err("original response waiter exited".to_owned());
        }
        Ok(frames)
    };
    let (response, frames) = bounded(async { tokio::join!(response, serve) }).await?;
    Ok((
        response.map_err(|_| "original response failed".to_owned())?,
        frames?,
    ))
}

// Uses the same Core notification and original public IO positions. It never
// treats this IO-only predicate as a response-body exit or a library-task join.
async fn actual_io_exit(admission: &NativeTransportAdmission) -> FixtureResult<()> {
    let deadline = tokio::time::Instant::now() + FIXTURE_DEADLINE;
    loop {
        let changed = admission.frontend_outgoing_notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let snapshot = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        if snapshot.data_connections == 0
            && snapshot.control_connections == 0
            && snapshot.data_handshakes == 0
            && snapshot.control_handshakes == 0
        {
            return Ok(());
        }
        tokio::time::timeout_at(deadline, changed)
            .await
            .map_err(|_| "actual public IO exit not observed".to_owned())?;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outgoing_close_keeps_actual_io_until_drop_and_does_not_close_membership() {
    let admission = small_frontend();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let native_endpoint =
        novarocks_types::NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let connector = frontend_lane_connector(
        novarocks_native_trust::NativeEndpointConnector::plaintext(native_endpoint),
        admission.clone(),
        FrontendNativeLane::ResultData,
    );
    let uri: hyper::http::Uri = "http://original-outgoing.test".parse().unwrap();
    let io = bounded(connector.clone().oneshot(uri.clone()))
        .await
        .unwrap()
        .unwrap();
    let (peer, _) = bounded(listener.accept()).await.unwrap().unwrap();
    let membership = admission.try_accept(TransportClass::Membership).unwrap();
    admission.close_frontend_outgoing().unwrap();
    let held = admission.frontend_outgoing_snapshot().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(30);
    let timed_out = admission
        .wait_frontend_outgoing_until(deadline)
        .await
        .err()
        .map(|e| e.kind());
    let refused = bounded(connector.oneshot(uri))
        .await
        .unwrap()
        .err()
        .map(|e| e.kind());
    let no_new_socket = listener
        .poll_accept(&mut Context::from_waker(std::task::Waker::noop()))
        .is_pending();
    // Actual IO ownership is destroyed; there is no count write or test ack.
    drop(io);
    drop(peer);
    let late_same_deadline = admission
        .wait_frontend_outgoing_until(deadline)
        .await
        .err()
        .map(|e| e.kind());
    let retry = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    let still_held_membership = admission.available_positions(TransportClass::Membership)
        == admission.positions(TransportClass::Membership) - 1;
    let membership_after_close = admission.try_accept(TransportClass::Membership).is_ok();
    drop(membership);
    drop(listener);
    assert_eq!(held.data_connections, 1);
    assert_eq!(held.data_handshakes, 0);
    assert_eq!(held.requests, [0; 4]);
    assert_eq!(timed_out, Some(std::io::ErrorKind::TimedOut));
    assert_eq!(late_same_deadline, Some(std::io::ErrorKind::TimedOut));
    assert!(retry.is_ok());
    assert!(refused.is_some() && no_new_socket);
    assert!(still_held_membership && membership_after_close);
}

#[tokio::test(flavor = "current_thread")]
async fn an_already_expired_original_deadline_never_returns_empty_success() {
    let admission = small_frontend();
    admission.close_frontend_outgoing().unwrap();
    let original_deadline = tokio::time::Instant::now();
    let empty = admission.frontend_outgoing_snapshot().unwrap().is_drained();
    let result = admission
        .wait_frontend_outgoing_until(original_deadline)
        .await;
    assert!(empty);
    assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test(flavor = "current_thread")]
async fn closed_poll_ready_destroys_the_original_hidden_acquire_qualification() {
    // Unit test of the real PollSemaphore reservation state. No Native IO claim.
    let admission = small_frontend();
    let lazy = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut lane = NativeLaneChannel::new(lazy, NativeLane::ResultData, Some(&admission));
    let count = lane.stream_limit();
    let held = lane
        .gate
        .semaphore
        .clone()
        .acquire_many_owned(u32::try_from(count).unwrap())
        .await
        .unwrap();
    let was_pending = lane
        .poll_ready(&mut Context::from_waker(std::task::Waker::noop()))
        .is_pending();
    drop(held); // Assigns one qualification to the pending original acquire future.
    let hidden = lane.available_streams();
    admission.close_frontend_outgoing().unwrap();
    let rejected = matches!(
        lane.poll_ready(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Ready(Err(_))
    );
    let released = lane.available_streams();
    drop(lane);
    assert!(was_pending && rejected);
    assert_eq!(hidden, count - 1);
    assert_eq!(released, count);
}

#[tokio::test(flavor = "current_thread")]
async fn original_hidden_readiness_qualification_exits_on_channel_drop_without_repoll() {
    // Unit-only: the real PollSemaphore and original gate, with no Native IO.
    let admission = small_frontend();
    let lazy = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut lane = NativeLaneChannel::new(lazy, NativeLane::ResultData, Some(&admission));
    let count = lane.stream_limit();
    // Retain the SAME semaphore only to inspect its actual returned positions.
    let original_streams = Arc::clone(&lane.gate.semaphore);
    let held = Arc::clone(&original_streams)
        .acquire_many_owned(u32::try_from(count).unwrap())
        .await
        .unwrap();
    let was_pending = lane
        .poll_ready(&mut Context::from_waker(std::task::Waker::noop()))
        .is_pending();
    drop(held); // Assigns one qualification inside this original acquire future.
    let assigned = original_streams.available_permits();
    admission.close_frontend_outgoing().unwrap();
    // Do not poll_ready or call again: cancellation must drop this very holder.
    let after_close = original_streams.available_permits();
    let public_exits = admission.frontend_outgoing_snapshot().unwrap();
    drop(lane);
    let after_original_drop = original_streams.available_permits();

    assert!(was_pending);
    assert_eq!(assigned, count - 1);
    assert_eq!(after_close, count - 1);
    assert!(public_exits.is_drained()); // Scoped IO/handshake/dispatched requests only.
    assert_eq!(after_original_drop, count);
}

#[tokio::test(flavor = "current_thread")]
async fn escaped_ready_channel_cannot_dispatch_after_the_original_role_close() {
    let admission = small_frontend();
    let mut peer = OriginalH2Peer::start().await.unwrap();
    let facts: FixtureResult<(bool, bool, FrontendOutgoingSnapshot)> = async {
        let mut lane = peer.channel(&admission).await?;
        bounded(lane.ready())
            .await?
            .map_err(|_| "original poll_ready failed".to_owned())?;
        admission
            .close_frontend_outgoing()
            .map_err(|_| "close failed".to_owned())?;
        let rejected = bounded(lane.call(request())).await?.is_err();
        let no_request = matches!(peer.heads.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        let snapshot = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        drop(lane);
        Ok((rejected, no_request, snapshot))
    }
    .await;
    let original_peer_join = peer.stop().await;
    let drained = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    assert!(original_peer_join.is_ok());
    assert!(drained.is_ok());
    let (rejected, no_request, snapshot) = facts.unwrap();
    assert!(rejected && no_request);
    assert_eq!(snapshot.requests, [0; 4]);
}

#[tokio::test(flavor = "current_thread")]
async fn real_headers_and_io_exit_do_not_retire_a_held_response_body() {
    let admission = small_frontend();
    let mut peer = Some(OriginalH2Peer::start().await.unwrap());
    let body_dropped = Arc::new(AtomicBool::new(false));
    let facts: FixtureResult<(
        FrontendOutgoingSnapshot,
        FrontendOutgoingSnapshot,
        FrontendOutgoingSnapshot,
    )> = async {
        let mut lane = peer.as_ref().unwrap().channel(&admission).await?;
        let (response, frames) = headers(&mut lane, peer.as_mut().unwrap()).await?;
        admission
            .close_frontend_outgoing()
            .map_err(|_| "close failed".to_owned())?;
        let at_headers = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        let body = response.into_body();
        drop(lane);
        // Keep the actual client body unpolled while the actual peer task and
        // its socket exit. Thus a queued body reset is not mistaken for EOF.
        let joined = peer.take().unwrap().stop().await;
        if joined.is_err() {
            return Err("original peer join failed".to_owned());
        }
        actual_io_exit(&admission).await?;
        let io_only = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        drop(frames);
        drop(body);
        body_dropped.store(true, Ordering::Release);
        let last = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        Ok((at_headers, io_only, last))
    }
    .await;
    // Preserve and actually join the original peer on setup/error exits too.
    let rescue_join = match peer.take() {
        Some(peer) => peer.stop().await,
        None => Ok(()),
    };
    // No assertion is made until the original body/IO ownership is gone.
    let drained = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    let (at_headers, io_only, last) = facts.unwrap();
    assert!(rescue_join.is_ok() && drained.is_ok() && body_dropped.load(Ordering::Acquire));
    assert_eq!(at_headers.requests, [1, 0, 0, 0]);
    assert_eq!(io_only.data_connections, 0);
    assert_eq!(io_only.requests, [1, 0, 0, 0]);
    assert!(last.is_drained());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_body_eof_and_reset_each_retire_the_original_call_before_io_exit() {
    for reset in [false, true] {
        let admission = small_frontend();
        let mut peer = OriginalH2Peer::start().await.unwrap();
        let facts: FixtureResult<(FrontendOutgoingSnapshot, FrontendOutgoingSnapshot)> = async {
            let mut lane = peer.channel(&admission).await?;
            let (response, frames) = headers(&mut lane, &mut peer).await?;
            admission
                .close_frontend_outgoing()
                .map_err(|_| "close failed".to_owned())?;
            let at_headers = admission
                .frontend_outgoing_snapshot()
                .map_err(|_| "snapshot failed".to_owned())?;
            let mut body = response.into_body();
            if reset {
                frames
                    .send(Err(std::io::ErrorKind::ConnectionReset.into()))
                    .await
                    .map_err(|_| "actual peer body was gone".to_owned())?;
            }
            drop(frames);
            let mut ended = false;
            for _ in 0..4 {
                match bounded(std::future::poll_fn(|cx| {
                    Pin::new(&mut body).poll_frame(cx)
                }))
                .await?
                {
                    None => {
                        ended = true;
                        break;
                    }
                    Some(Err(_)) if reset => {
                        ended = true;
                        break;
                    }
                    Some(Err(_)) => return Err("unexpected actual EOF error".to_owned()),
                    Some(Ok(frame)) if frame.data_ref().is_some_and(Bytes::is_empty) => {}
                    Some(Ok(_)) => return Err("unexpected actual peer frame".to_owned()),
                }
            }
            if !ended {
                return Err("actual body public exit missing".to_owned());
            }
            let at_body_exit = admission
                .frontend_outgoing_snapshot()
                .map_err(|_| "snapshot failed".to_owned())?;
            drop(body);
            drop(lane);
            Ok((at_headers, at_body_exit))
        }
        .await;
        let joined = peer.stop().await;
        let drained = admission
            .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
            .await;
        assert!(joined.is_ok() && drained.is_ok());
        let (at_headers, at_body_exit) = facts.unwrap();
        assert_eq!(at_headers.requests, [1, 0, 0, 0]);
        assert_eq!(at_body_exit.requests, [0; 4]);
        assert_eq!(at_body_exit.data_connections, 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_entered_original_response_future_survives_close_until_actual_waiter_drop() {
    let admission = small_frontend();
    let mut peer = OriginalH2Peer::start().await.unwrap();
    let mut original: Option<
        tokio::task::JoinHandle<Result<hyper::http::Response<BoxBody>, StdError>>,
    > = None;
    let mut reply: Option<HeadReply> = None;
    let facts: FixtureResult<(FrontendOutgoingSnapshot, FrontendOutgoingSnapshot)> = async {
        let mut lane = peer.channel(&admission).await?;
        bounded(lane.ready())
            .await?
            .map_err(|_| "original ready failed".to_owned())?;
        original = Some(tokio::spawn(lane.call(request())));
        reply = Some(
            bounded(peer.heads.recv())
                .await?
                .ok_or_else(|| "original request absent".to_owned())?,
        );
        admission
            .close_frontend_outgoing()
            .map_err(|_| "close failed".to_owned())?;
        let entered = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        original.as_ref().unwrap().abort();
        let joined = bounded(original.as_mut().unwrap()).await?;
        original.take();
        if !joined.is_err_and(|error| error.is_cancelled()) {
            return Err("original waiter did not cancel".to_owned());
        }
        let exited = admission
            .frontend_outgoing_snapshot()
            .map_err(|_| "snapshot failed".to_owned())?;
        drop(lane);
        Ok((entered, exited))
    }
    .await;
    // Rescue any still-owned original waiter before peer shutdown and assertions.
    let rescue_join: FixtureResult<bool> = if let Some(task) = original.take() {
        task.abort();
        let joined = task.await;
        Ok(joined.is_ok() || joined.is_err_and(|error| error.is_cancelled()))
    } else {
        Ok(true)
    };
    drop(reply.take());
    let joined = peer.stop().await;
    let drained = admission
        .wait_frontend_outgoing_until(tokio::time::Instant::now() + FIXTURE_DEADLINE)
        .await;
    assert!(rescue_join == Ok(true) && joined.is_ok() && drained.is_ok());
    let (entered, exited) = facts.unwrap();
    assert_eq!(entered.requests, [1, 0, 0, 0]);
    assert_eq!(exited.requests, [0; 4]);
}

use std::sync::Mutex;

// Component-only public lazy-connector mechanism. This is not the FE eager
// factory, a Native query, a byte coefficient, or an internal Buffer getter.
#[derive(Clone, Copy, Debug)]
struct PublicQueueFacts {
    ready_calls: usize,
    retained_calls: usize,
    ninth_pending: bool,
    original_connector_first_pending: bool,
    original_connector_attempts: usize,
    connector_released: bool,
    before_original_deadline: bool,
    native_available: usize,
    outgoing: FrontendOutgoingSnapshot,
    completed_responses: usize,
    after_ninth_drop_available: usize,
}
fn public_queue_fill_source(facts: &PublicQueueFacts) -> Result<(), StdError> {
    if facts.ready_calls != 8
        || facts.retained_calls != 8
        || !facts.ninth_pending
        || !facts.original_connector_first_pending
        || facts.original_connector_attempts != 1
        || facts.connector_released
        || !facts.before_original_deadline
        || facts.native_available != 119
        || facts.outgoing.closed
        || facts.outgoing.requests != [8, 0, 0, 0]
        || facts.outgoing.data_connections != 1
        || facts.outgoing.data_handshakes != 1
        || facts.outgoing.control_connections != 0
        || facts.outgoing.control_handshakes != 0
    {
        return Err("public queue fill source bracket incomplete".into());
    }
    Ok(())
}

struct QueueConnectorSignals {
    release: Mutex<Option<oneshot::Receiver<()>>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    attempts: std::sync::atomic::AtomicUsize,
    first_pending: AtomicBool,
    released: AtomicBool,
}

struct QueuePollPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for QueuePollPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The actual payload remains owned; no arbitrary formatter is invoked.
        let _ = &self.0;
        f.write_str("OriginalQueueComponentPollPanic")
    }
}
impl std::fmt::Display for QueuePollPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original queue component future panicked")
    }
}
impl std::error::Error for QueuePollPanic {}
async fn queue_catch_poll<F: Future>(future: F) -> Result<F::Output, StdError> {
    let mut original = Box::pin(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| original.as_mut().poll(cx)))
        {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(
                Box::new(QueuePollPanic(Mutex::new(payload))) as StdError
            )),
        }
    })
    .await
}

// Exactly one original peer task. It owns accept + H2 polling, never spawns a
// child stream/driver task, and detects any ninth application request.
async fn queue_original_peer(
    listener: tokio::net::TcpListener,
    mut stop: oneshot::Receiver<()>,
) -> Result<usize, StdError> {
    let accepted = tokio::select! {
        accepted = listener.accept() => Some(accepted?),
        _ = &mut stop => None,
    };
    drop(listener);
    let Some((io, _)) = accepted else {
        return Ok(0);
    };
    let mut h2 = h2::server::Builder::new().handshake::<_, Bytes>(io).await?;
    let mut received = 0;
    let mut seen = [false; 8];
    loop {
        tokio::select! {
            _ = &mut stop => {
                h2.graceful_shutdown();
                std::future::poll_fn(|cx| h2.poll_closed(cx)).await?;
                return Ok(received);
            }
            accepted = h2.accept() => {
                let Some(accepted) = accepted else { return Ok(received) };
                let (request, mut response) = accepted?;
                let index: usize = request.headers().get("x-component-queue-index")
                    .ok_or("queue request lacks component index")?.to_str()?.parse()?;
                if index >= 8 || seen[index] || !request.body().is_end_stream() {
                    return Err("duplicate/extra/body-bearing queue component request".into());
                }
                seen[index] = true;
                received += 1;
                let headers = hyper::http::Response::builder().status(200)
                    .header("content-type", "application/grpc").body(())?;
                let mut body = response.send_response(headers, false)?;
                body.send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false)?;
                let mut trailers = hyper::http::HeaderMap::new();
                trailers.insert("grpc-status", hyper::http::HeaderValue::from_static("0"));
                body.send_trailers(trailers)?;
            }
        }
    }
}
async fn queue_body_public_eof(
    mut response: hyper::http::Response<BoxBody>,
) -> Result<(), StdError> {
    if response.status() != 200
        || response.headers().get("content-type")
            != Some(&hyper::http::HeaderValue::from_static("application/grpc"))
    {
        return Err("queue component response headers differ".into());
    }
    let mut bytes = [0u8; 5];
    let mut used = 0;
    let mut trailers = 0;
    let mut ended = false;
    // One fixed five-byte DATA message, one trailer, public EOF. No arbitrary body retention.
    for _ in 0..4 {
        let frame = std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await;
        let Some(frame) = frame else {
            ended = true;
            break;
        };
        let frame = frame?;
        if let Some(data) = frame.data_ref() {
            let next = used + data.len();
            if next > bytes.len() {
                return Err("queue component response DATA exceeds literal".into());
            }
            bytes[used..next].copy_from_slice(data);
            used = next;
        } else if let Some(fields) = frame.trailers_ref() {
            if fields.len() != 1
                || fields.get("grpc-status") != Some(&hyper::http::HeaderValue::from_static("0"))
            {
                return Err("queue component response trailers differ".into());
            }
            trailers += 1;
        } else {
            return Err("unknown queue component response frame".into());
        }
    }
    if !ended || used != 5 || bytes != [0; 5] || trailers != 1 {
        return Err("queue component response lacks complete literal DATA/trailers/EOF".into());
    }
    Ok(())
}
struct QueueRun {
    result: Result<PublicQueueFacts, StdError>,
    cleanup: [Option<StdError>; 4],
    peer_requests: Option<usize>,
    original_peer_joined: bool,
    public_io_and_calls_drained: bool,
}
fn queue_error_class(error: &StdError) -> &'static str {
    if error.is::<tokio::time::error::Elapsed>() {
        "original-deadline"
    } else if error.is::<h2::Error>() {
        "original-h2"
    } else if error.is::<std::io::Error>() {
        "original-io"
    } else if error.is::<tokio::task::JoinError>() {
        "original-peer-join"
    } else {
        "retained-original-other"
    }
}
async fn public_queue_recipe(ready_only: bool) -> Result<QueueRun, StdError> {
    let geometry =
        novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1;
    if geometry.transport_streams_per_connection != 128
        || geometry.transport_tonic_pending_per_connection != 8
        || geometry.transport_connect_deadline_ms != 2000
    {
        return Err("queue recipe frozen public geometry differs".into());
    }
    // One absolute clock, established before bind/Channel/firstpoll. Endpoint's
    // original 2s timeout remains; T is conservatively earlier, never reset.
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(geometry.transport_connect_deadline_ms);
    let admission = NativeTransportAdmission::frontend(None)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stop_rx) = oneshot::channel();
    let mut peer = tokio::task::JoinSet::new();
    let (release, release_rx) = oneshot::channel();
    let (entered_tx, mut entered) = oneshot::channel();
    let signals = Arc::new(QueueConnectorSignals {
        release: Mutex::new(Some(release_rx)),
        entered: Mutex::new(Some(entered_tx)),
        attempts: std::sync::atomic::AtomicUsize::new(0),
        first_pending: AtomicBool::new(false),
        released: AtomicBool::new(false),
    });
    let native = novarocks_native_trust::NativeEndpointConnector::plaintext(
        novarocks_types::NativeEndpoint::from_socket_addr(address),
    );
    let captured_admission = admission.clone();
    let captured_signals = signals.clone();
    let connector = tower::service_fn(move |_uri: hyper::http::Uri| {
        let native = native.clone();
        let admission = captured_admission.clone();
        let signals = captured_signals.clone();
        async move {
            // This is the actual connector firstpoll and original public dial
            // guard. The test pause owns that guard until actual IO is returned.
            if signals.attempts.fetch_add(1, Ordering::AcqRel) != 0 {
                return Err::<hyper_util::rt::TokioIo<novarocks_transport_io::BoxedIo>, StdError>(
                    "queue recipe attempted a second connector".into(),
                );
            }
            let dial = admission.try_dial_lane(FrontendNativeLane::ResultData)?;
            let mut released = signals
                .release
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
                .ok_or("queue connector lost original release receiver")?;
            std::future::poll_fn(|cx| match Pin::new(&mut released).poll(cx) {
                Poll::Pending => {
                    signals.first_pending.store(true, Ordering::Release);
                    if let Some(entered) = signals
                        .entered
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .take()
                        && entered.send(()).is_err()
                    {
                        return Poll::Ready(Err("queue connector original observer exited".into()));
                    }
                    Poll::Pending
                }
                Poll::Ready(Ok(())) => Poll::Ready(Ok::<(), StdError>(())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(Box::new(error) as StdError)),
            })
            .await?;
            let io = native.connect().await?;
            let io: novarocks_transport_io::BoxedIo = Box::new(dial.established_io(io)?);
            Ok(hyper_util::rt::TokioIo::new(io))
        }
    });
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?;
    let channel = configure_native_endpoint(endpoint).connect_with_connector_lazy(connector);
    let mut original = Some(NativeLaneChannel::new(
        channel,
        NativeLane::ResultData,
        Some(&admission),
    ));
    type OriginalCall = <NativeLaneChannel as Service<hyper::http::Request<BoxBody>>>::Future;
    let mut calls: [Option<OriginalCall>; 8] = std::array::from_fn(|_| None);
    let mut ready_clones: [Option<NativeLaneChannel>; 8] = std::array::from_fn(|_| None);
    let mut ninth = None;
    let mut release = Some(release);
    // All fallible setup completed before the original test-owned task exists.
    // After spawn, every operation failure reaches the same explicit join path.
    peer.spawn(queue_original_peer(listener, stop_rx));
    let operation = async {
        let lane = original.as_mut().ok_or("original queue lane missing")?;
        if lane.stream_limit() != 128 || lane.available_streams() != 128 {
            return Err("original queue lane initial public stream geometry differs".into());
        }
        let mut ready_calls = 0;
        for index in 0..8 {
            if ready_only {
                let mut clone = lane.clone();
                clone.ready().await?;
                ready_clones[index] = Some(clone);
            } else {
                lane.ready().await?;
                let mut offered = request();
                offered.headers_mut().insert(
                    "x-component-queue-index",
                    hyper::http::HeaderValue::from_str(&index.to_string())?,
                );
                calls[index] = Some(lane.call(offered));
                ready_calls += 1;
                if index == 0 {
                    (&mut entered).await?;
                }
            }
        }
        ninth = Some(lane.clone());
        let ninth_lane = ninth.as_mut().ok_or("ninth queue lane missing")?;
        let ninth_pending = std::future::poll_fn(|cx| {
            Poll::Ready(matches!(ninth_lane.poll_ready(cx), Poll::Pending))
        })
        .await;
        let mut facts = PublicQueueFacts {
            ready_calls,
            retained_calls: calls.iter().filter(|item| item.is_some()).count(),
            ninth_pending,
            original_connector_first_pending: signals.first_pending.load(Ordering::Acquire),
            original_connector_attempts: signals.attempts.load(Ordering::Acquire),
            connector_released: signals.released.load(Ordering::Acquire),
            before_original_deadline: tokio::time::Instant::now() < deadline,
            native_available: lane.available_streams(),
            outgoing: admission.frontend_outgoing_snapshot()?,
            completed_responses: 0,
            after_ninth_drop_available: 0,
        };
        if !ready_only {
            public_queue_fill_source(&facts)?;
            signals.released.store(true, Ordering::Release);
            release
                .take()
                .ok_or("original connector release sender missing")?
                .send(())
                .map_err(|_| "original connector exited before release")?;
            for call in &mut calls {
                let response = call
                    .take()
                    .ok_or("original retained queue call missing")?
                    .await?;
                queue_body_public_eof(response).await?;
                facts.completed_responses += 1;
            }
            ninth
                .as_mut()
                .ok_or("original ninth waiter missing")?
                .ready()
                .await?;
            drop(ninth.take());
            facts.after_ninth_drop_available = lane.available_streams();
            if facts.completed_responses != 8
                || facts.after_ninth_drop_available != 128
                || admission.frontend_outgoing_snapshot()?.requests != [0; 4]
            {
                return Err("original queue response/body/ninth qualification exits differ".into());
            }
        }
        Ok::<_, StdError>(facts)
    };
    // The future borrows the parent's original futures/IO. Timeout or panic
    // drops that borrow, not the parent's actual peer owner or stored handles.
    let result = match tokio::time::timeout_at(deadline, queue_catch_poll(operation)).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error),
        Err(error) => Err(Box::new(error) as StdError),
    };
    for call in &mut calls {
        drop(call.take());
    }
    for clone in &mut ready_clones {
        drop(clone.take());
    }
    drop(ninth.take());
    drop(release.take());
    let mut cleanup: [Option<StdError>; 4] = std::array::from_fn(|_| None);
    if let Err(error) = admission.close_frontend_outgoing() {
        cleanup[0] = Some(Box::new(error));
    }
    let _ = stop.send(());
    let mut peer_requests = None;
    // The same original JoinSet is borrowed by the timeout. No handle is
    // consumed by a watchdog; after timeout, abort_all then actual join_next.
    match tokio::time::timeout_at(deadline, peer.join_next()).await {
        Ok(Some(Ok(Ok(count)))) => peer_requests = Some(count),
        Ok(Some(Ok(Err(error)))) => cleanup[1] = Some(error),
        Ok(Some(Err(error))) => cleanup[1] = Some(Box::new(error)),
        Ok(None) => cleanup[1] = Some("original peer JoinSet unexpectedly empty".into()),
        Err(error) => {
            cleanup[1] = Some(Box::new(error));
            peer.abort_all();
        }
    }
    while let Some(joined) = peer.join_next().await {
        match joined {
            Ok(Ok(count)) => peer_requests = Some(count),
            Ok(Err(error)) => cleanup[2] = Some(error),
            Err(error) if error.is_cancelled() => {}
            Err(error) => cleanup[2] = Some(Box::new(error)),
        }
    }
    let original_peer_joined = peer.is_empty();
    // Keep the original Channel alive until its peer's normal close has been
    // polled and joined. Dropping the client first races the peer's GOAWAY write.
    drop(original.take());
    let public_io_and_calls_drained = match admission.wait_frontend_outgoing_until(deadline).await {
        Ok(()) => true,
        Err(error) => {
            cleanup[3] = Some(Box::new(error));
            false
        }
    };
    // No late settlement is accepted. The original deadline is never renewed,
    // even though mandatory abort/join cleanup may finish after it.
    if tokio::time::Instant::now() >= deadline && cleanup[3].is_none() {
        cleanup[3] = Some("queue component settled after original deadline".into());
    }
    Ok(QueueRun {
        result,
        cleanup,
        peer_requests,
        original_peer_joined,
        public_io_and_calls_drained,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn eight_actual_calls_fill_public_buffer_at_original_connector_pending_with_native_headroom()
{
    let run = public_queue_recipe(false).await;
    // Every assertion follows the actual original peer join and public exit.
    let run = match run {
        Ok(run) => run,
        Err(_) => panic!("queue component setup/owner construction failed"),
    };
    assert!(
        run.cleanup.iter().all(Option::is_none),
        "queue component original cleanup failed; operation_ok={} cleanup={:?} peer_requests={:?} joined={} drained={}",
        run.result.is_ok(),
        run.cleanup
            .each_ref()
            .map(|error| error.as_ref().map(queue_error_class)),
        run.peer_requests,
        run.original_peer_joined,
        run.public_io_and_calls_drained,
    );
    assert!(run.original_peer_joined && run.public_io_and_calls_drained);
    let facts = match run.result {
        Ok(facts) => facts,
        Err(_) => panic!("queue component original operation failed"),
    };
    assert!(public_queue_fill_source(&facts).is_ok());
    assert_eq!(facts.completed_responses, 8);
    assert_eq!(facts.after_ninth_drop_available, 128);
    assert_eq!(run.peer_requests, Some(8));
}

#[tokio::test(flavor = "current_thread")]
async fn eight_actual_ready_only_reservations_do_not_prove_public_queue_messages() {
    let run = public_queue_recipe(true).await;
    let run = match run {
        Ok(run) => run,
        Err(_) => panic!("ready-only component setup/owner construction failed"),
    };
    assert!(
        run.cleanup.iter().all(Option::is_none),
        "ready-only original cleanup failed"
    );
    assert!(run.original_peer_joined && run.public_io_and_calls_drained);
    let facts = match run.result {
        Ok(facts) => facts,
        Err(_) => panic!("ready-only original operation failed"),
    };
    assert!(facts.ninth_pending);
    assert_eq!(facts.native_available, 119);
    assert_eq!(facts.ready_calls, 0);
    assert_eq!(facts.retained_calls, 0);
    assert_eq!(facts.outgoing.requests, [0; 4]);
    assert_eq!(facts.original_connector_attempts, 0);
    assert!(!facts.original_connector_first_pending);
    assert!(public_queue_fill_source(&facts).is_err());
    assert_eq!(run.peer_requests, Some(0));
}

// One real public readiness poll. Pending does not retain a local waiter; the
// original service's bounded recheck is responsible for the next attempt.
async fn reserve_once(selected: &SelectedNativeCall) -> Poll<Result<ReservedNativeCall, StdError>> {
    std::future::poll_fn(|cx| Poll::Ready(selected.try_reserve(cx))).await
}

fn lazy_selected_lane(streams: usize) -> NativeLaneChannel {
    let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
    NativeLaneChannel::with_streams(
        configure_native_endpoint(endpoint).connect_lazy(),
        NativeLane::Observation,
        streams,
        None,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn selected_call_refusal_keeps_no_partial_stream_or_buffer_position() {
    let lane = lazy_selected_lane(16);
    let selected = lane.select_local_call();
    let mut reserved = Vec::with_capacity(8);
    // These are eight actual Tower Buffer permits, with no request dispatched.
    for _ in 0..8 {
        match reserve_once(&selected).await {
            Poll::Ready(Ok(call)) => reserved.push(call),
            other => panic!("original local reservation unexpectedly refused: {other:?}"),
        }
    }
    assert_eq!(lane.available_streams(), 8);
    assert!(matches!(reserve_once(&selected).await, Poll::Pending));
    assert_eq!(lane.available_streams(), 8, "Buffer refusal kept a stream");
    // Real refusal must return its assigned queue waiter too, so this exact
    // selected connection can immediately use an actual returned qualification.
    drop(reserved.pop());
    assert_eq!(lane.available_streams(), 9);
    let next = match reserve_once(&selected).await {
        Poll::Ready(Ok(call)) => call,
        other => panic!("returned original Buffer position was not usable: {other:?}"),
    };
    assert_eq!(lane.available_streams(), 8);
    drop(next);
    drop(reserved);
    assert_eq!(lane.available_streams(), 16);
}

#[tokio::test(flavor = "current_thread")]
async fn selected_call_cannot_borrow_a_different_connection_gate() {
    let a = lazy_selected_lane(1);
    let b = lazy_selected_lane(1);
    let selected_a = a.select_local_call();
    let selected_b = b.select_local_call();
    let held_a = match reserve_once(&selected_a).await {
        Poll::Ready(Ok(call)) => call,
        other => panic!("first original stream unexpectedly refused: {other:?}"),
    };
    assert!(matches!(reserve_once(&selected_a).await, Poll::Pending));
    assert_eq!(a.available_streams(), 0);
    let held_b = match reserve_once(&selected_b).await {
        Poll::Ready(Ok(call)) => call,
        other => panic!("independent original stream unexpectedly refused: {other:?}"),
    };
    assert_eq!(b.available_streams(), 0);
    drop(held_b);
    assert_eq!(a.available_streams(), 0);
    assert_eq!(b.available_streams(), 1);
    drop(held_a);
    assert_eq!(a.available_streams(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn selected_call_moves_ready_channel_into_generated_grpc_and_body_exit() {
    let admission = small_frontend();
    let mut peer = OriginalH2Peer::start().await.unwrap();
    let mut original = None;
    // Original peer/join remains outside this cancellable operation. All
    // scenario errors reach the same actual stop and IO-exit observation.
    let primary = bounded(async {
        original = Some(peer.channel(&admission).await?);
        let lane = original.as_ref().ok_or("original channel missing")?;
        let selected = lane.select_local_call();
        let reserved = match reserve_once(&selected).await {
            Poll::Ready(Ok(call)) => call,
            Poll::Ready(Err(error)) => return Err(error.to_string()),
            Poll::Pending => return Err("connected original lane refused local positions".into()),
        };
        if lane.available_streams() != lane.stream_limit() - 1 {
            return Err("reservation did not hold the original stream".into());
        }
        let interceptor = |request: tonic::Request<()>| Ok(request);
        let mut grpc =
            crate::generated::nova_rocks_grpc_client::NovaRocksGrpcClient::with_interceptor(
                reserved,
                interceptor,
            );
        let call = grpc.subscribe_task_status(tonic::Request::new(
            novarocks_proto_models::novarocks::SubscribeTaskStatusRequest::default(),
        ));
        let serve = async {
            let reply = peer
                .heads
                .recv()
                .await
                .ok_or("original peer did not receive request")?;
            let (frames, receiver) = mpsc::channel(1);
            let response = hyper::http::Response::builder()
                .header("content-type", "application/grpc")
                .body(PeerBody(receiver))
                .map_err(|error| error.to_string())?;
            reply
                .send(response)
                .map_err(|_| "original response waiter exited")?;
            Ok::<_, String>(frames)
        };
        let (response, frames) = tokio::join!(call, serve);
        let response = response.map_err(|error| error.to_string())?;
        let frames = frames?;
        if lane.available_streams() != lane.stream_limit() - 1 {
            return Err("headers prematurely returned original stream position".into());
        }
        drop(response);
        drop(frames);
        if lane.available_streams() != lane.stream_limit() {
            return Err("generated response Drop kept original stream position".into());
        }
        // The generated Grpc cannot consume a second local qualification from
        // this one-shot carrier. Its normal ready adapter returns an error.
        if grpc
            .subscribe_task_status(tonic::Request::new(
                novarocks_proto_models::novarocks::SubscribeTaskStatusRequest::default(),
            ))
            .await
            .is_ok()
        {
            return Err("one reservation dispatched more than one request".into());
        }
        Ok::<_, String>(())
    })
    .await;
    drop(original);
    let cleanup = peer.stop().await;
    let io_exit = actual_io_exit(&admission).await;
    assert!(
        primary.is_ok(),
        "scenario watchdog: {primary:?}; cleanup: {cleanup:?}; IO: {io_exit:?}"
    );
    assert!(
        primary.as_ref().is_ok_and(Result::is_ok),
        "scenario: {primary:?}; cleanup: {cleanup:?}; IO: {io_exit:?}"
    );
    assert!(cleanup.is_ok(), "actual peer cleanup: {cleanup:?}");
    assert!(io_exit.is_ok(), "actual IO exit: {io_exit:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn selected_call_role_close_refuses_an_already_ready_carrier() {
    let admission = small_frontend();
    let mut lane = lazy_selected_lane(1);
    lane.outgoing_admission = Some(admission.clone());
    let selected = lane.select_local_call();
    let mut held = match reserve_once(&selected).await {
        Poll::Ready(Ok(call)) => call,
        other => panic!("initial qualification unexpectedly refused: {other:?}"),
    };
    admission.close_frontend_outgoing().unwrap();
    assert!(matches!(
        std::future::poll_fn(|cx| Poll::Ready(held.poll_ready(cx))).await,
        Poll::Ready(Err(_)),
    ));
    assert!(matches!(reserve_once(&selected).await, Poll::Ready(Err(_))));
    drop(held);
    assert_eq!(lane.available_streams(), 1);
    assert_eq!(
        admission.frontend_outgoing_snapshot().unwrap().requests,
        [0; 4]
    );
}
