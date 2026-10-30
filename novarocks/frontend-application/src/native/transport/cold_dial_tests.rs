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

//! Actual FA cold-source, local refusal, cache fence and H2 bootstrap cases.
//! No Joint grant, source-exit receipt or complete Native profile is asserted.

use super::*;
use crate::native::data_runtime::NativeDialObservationForTest;
use futures::{FutureExt, StreamExt};
use novarocks_native_adapter::FrontendNativeTransport;
use novarocks_native_adapter::native_transport_admission::TransportClass;
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_proto_codec::native_rpc::FrontendNativeLane;
use novarocks_secret::SecretValue;
use std::any::Any;
use std::convert::Infallible;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tonic::codegen::{Service, http};
use tower::ServiceExt;

type TestError = Box<dyn std::error::Error + Send + Sync>;

// Same unstarted/empty-owner pattern as eager_tls_reconnect_tests. No Apply
// job/subscription is submitted and neither original reaper is started.
fn original_runtime() -> FrontendDataRuntime {
    let trust = NativeTrust::new(
        DeploymentId::parse("cold-dial-fa-test").unwrap(),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef")).unwrap(),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").unwrap(),
        NativeTransportMode::Disabled,
    );
    FrontendDataRuntime::new_with_native_trust(
        tokio::runtime::Handle::current(),
        Arc::new(trust),
        FrontendNativeTransport::plaintext(),
        novarocks_task_codec::TransportBudget::DEFAULT,
        8,
    )
    .unwrap()
}

fn local_case(
    domain: NativeEndpointDomain,
) -> (
    FrontendDataRuntime,
    Client,
    std::net::TcpListener,
    BackendProcessId,
) {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let runtime = original_runtime();
    let peer = BackendProcessId::new_v7();
    let client = Client::for_endpoint(
        NativeEndpoint::from_socket_addr(socket.local_addr().unwrap()),
        domain,
        peer,
        runtime.clone(),
    );
    (runtime, client, socket, peer)
}

fn observed(source: &Option<NativeChannelSource>) -> NativeDialObservationForTest {
    let Some(NativeChannelSource::Cold(selected)) = source else {
        panic!("the original test source must be a cold election");
    };
    selected.reservation.observe_for_test()
}

fn original_source(
    source: &Option<NativeChannelSource>,
    observer: &NativeDialObservationForTest,
    leader: bool,
) {
    let Some(NativeChannelSource::Cold(selected)) = source else {
        panic!("the original cold source was replaced or removed");
    };
    assert!(
        observer.matches(&selected.reservation),
        "original slot/generation changed"
    );
    assert_eq!(selected.reservation.is_leader(), leader);
}

fn no_io(socket: &std::net::TcpListener) {
    match socket.accept() {
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
        Ok((stream, _)) => {
            drop(stream);
            panic!("a nonholding cold-source transition started IO");
        }
    }
}

#[tokio::test]
async fn cold_source_follower_keeps_the_exact_generation_without_reelection() {
    let (runtime, client, socket, _) = local_case(NativeEndpointDomain::BackendControl);
    let method = NativeRpcMethod::Heartbeat;
    let mut leader = Some(client.prepare_channel_source(method).unwrap());
    let identity = observed(&leader);
    // The real LifecycleControl lane has one original cache slot. This is
    // existing geometry, not a global cap=1 policy for another lane.
    let mut follower = Some(client.prepare_channel_source(method).unwrap());
    original_source(&leader, &identity, true);
    original_source(&follower, &identity, false);
    let mut receiver = None;
    for _ in 0..2 {
        assert_eq!(
            NativeChannelSource::try_reserve_in(&mut follower, &mut receiver).unwrap(),
            NativeDialAttempt::Pending
        );
        assert!(receiver.is_none());
        original_source(&follower, &identity, false);
    }
    assert_eq!(runtime.cached_connections(&identity.slot.key), 0);
    no_io(&socket);
    drop(leader.take());
    assert!(matches!(
        NativeChannelSource::try_reserve_in(&mut follower, &mut receiver),
        Err(ChannelAcquisitionError::RetryableNetwork(_))
    ));
    original_source(&follower, &identity, false);
    let mut fresh = Some(client.prepare_channel_source(method).unwrap());
    let fresh_identity = observed(&fresh);
    let Some(NativeChannelSource::Cold(selected)) = fresh.as_ref() else {
        unreachable!()
    };
    assert!(
        !identity.matches(&selected.reservation),
        "cancelled leader was reelected in place"
    );
    assert!(selected.reservation.is_leader());
    // An old follower Drop cannot delete the separately elected replacement.
    drop(follower.take());
    original_source(&fresh, &fresh_identity, true);
    assert_eq!(
        NativeChannelSource::try_reserve_in(&mut fresh, &mut receiver).unwrap(),
        NativeDialAttempt::Installed
    );
    assert!(matches!(receiver, Some(NativeDialHandoff::Reserved(_))));
    no_io(&socket);
    drop(receiver.take());
    runtime.close_native_outgoing().unwrap();
    assert!(
        runtime
            .transport_admission()
            .frontend_outgoing_snapshot()
            .unwrap()
            .is_drained()
    );
}

#[tokio::test]
async fn cold_source_lane_refusal_preserves_source_and_takes_no_physical_holder() {
    let (runtime, client, socket, _) = local_case(NativeEndpointDomain::BackendControl);
    let mut source = Some(
        client
            .prepare_channel_source(NativeRpcMethod::Heartbeat)
            .unwrap(),
    );
    let identity = observed(&source);
    let held = runtime
        .try_dial_lane(FrontendNativeLane::LifecycleControl)
        .unwrap();
    let before = runtime
        .transport_admission()
        .frontend_outgoing_snapshot()
        .unwrap();
    let mut target = None;
    assert_eq!(
        NativeChannelSource::try_reserve_in(&mut source, &mut target).unwrap(),
        NativeDialAttempt::Pending
    );
    original_source(&source, &identity, true);
    assert!(target.is_none());
    assert_eq!(
        runtime
            .transport_admission()
            .frontend_outgoing_snapshot()
            .unwrap(),
        before
    );
    no_io(&socket);
    drop(held);
    assert_eq!(
        NativeChannelSource::try_reserve_in(&mut source, &mut target).unwrap(),
        NativeDialAttempt::Installed
    );
    assert!(source.is_none());
    let held = runtime
        .transport_admission()
        .frontend_outgoing_snapshot()
        .unwrap();
    assert_eq!((held.control_connections, held.control_handshakes), (1, 1));
    no_io(&socket);
    drop(target.take());
    let returned = runtime
        .transport_admission()
        .frontend_outgoing_snapshot()
        .unwrap();
    assert_eq!(
        (returned.control_connections, returned.control_handshakes),
        (0, 0)
    );
    assert!(
        runtime
            .try_dial_lane(FrontendNativeLane::LifecycleControl)
            .is_some()
    );
    runtime.close_native_outgoing().unwrap();
}

#[tokio::test]
async fn cold_source_actual_handshake_refusal_returns_partial_physical_and_lane() {
    let (runtime, client, socket, _) = local_case(NativeEndpointDomain::BackendData);
    let admission = runtime.transport_admission();
    let count = admission.handshake_positions(TransportClass::Data);
    let mut holders = Vec::with_capacity(count);
    for _ in 0..count {
        holders.push(
            admission
                .try_reserve_frontend_dial(FrontendNativeLane::Observation)
                .unwrap(),
        );
    }
    let mut source = Some(
        client
            .prepare_channel_source(NativeRpcMethod::SubscribeTaskStatus)
            .unwrap(),
    );
    let identity = observed(&source);
    let before = admission.frontend_outgoing_snapshot().unwrap();
    let refused = admission.refused_connections(TransportClass::Data);
    assert_eq!(
        (before.data_connections, before.data_handshakes),
        (count, count)
    );
    let mut target = None;
    assert_eq!(
        NativeChannelSource::try_reserve_in(&mut source, &mut target).unwrap(),
        NativeDialAttempt::Pending
    );
    original_source(&source, &identity, true);
    assert!(target.is_none());
    assert_eq!(
        admission.frontend_outgoing_snapshot().unwrap(),
        before,
        "the failed handshake kept its partial physical position"
    );
    assert_eq!(
        admission.refused_connections(TransportClass::Data),
        refused + 1
    );
    assert!(
        runtime
            .try_dial_lane(FrontendNativeLane::Observation)
            .is_some(),
        "failed physical handoff kept the original lane"
    );
    no_io(&socket);
    drop(holders);
    assert_eq!(
        NativeChannelSource::try_reserve_in(&mut source, &mut target).unwrap(),
        NativeDialAttempt::Installed
    );
    assert!(source.is_none());
    no_io(&socket);
    drop(target.take());
    let after = admission.frontend_outgoing_snapshot().unwrap();
    assert_eq!((after.data_connections, after.data_handshakes), (0, 0));
    runtime.close_native_outgoing().unwrap();
}

#[tokio::test]
async fn cold_source_occupied_receiver_changes_neither_original_value() {
    let (runtime, client, socket, _) = local_case(NativeEndpointDomain::BackendControl);
    let mut first = Some(
        client
            .prepare_channel_source(NativeRpcMethod::Heartbeat)
            .unwrap(),
    );
    let identity = observed(&first);
    let mut target = None;
    assert_eq!(
        NativeChannelSource::try_reserve_in(&mut first, &mut target).unwrap(),
        NativeDialAttempt::Installed
    );
    let mut follower = Some(
        client
            .prepare_channel_source(NativeRpcMethod::Heartbeat)
            .unwrap(),
    );
    original_source(&follower, &identity, false);
    let before = runtime
        .transport_admission()
        .frontend_outgoing_snapshot()
        .unwrap();
    assert!(matches!(
        NativeChannelSource::try_reserve_in(&mut follower, &mut target),
        Err(ChannelAcquisitionError::Fatal(_))
    ));
    original_source(&follower, &identity, false);
    let Some(NativeDialHandoff::Reserved(reserved)) = target.as_ref() else {
        panic!("the occupied original receiver was overwritten");
    };
    assert!(identity.matches(reserved.reservation.as_ref().unwrap()));
    assert_eq!(
        runtime
            .transport_admission()
            .frontend_outgoing_snapshot()
            .unwrap(),
        before
    );
    no_io(&socket);
    drop(target.take());
    drop(follower.take());
    runtime.close_native_outgoing().unwrap();
}

#[tokio::test]
async fn cold_source_close_or_retirement_before_reservation_cannot_start_io() {
    for close in [false, true] {
        let (runtime, client, socket, peer) = local_case(NativeEndpointDomain::BackendControl);
        let mut source = Some(
            client
                .prepare_channel_source(NativeRpcMethod::Heartbeat)
                .unwrap(),
        );
        let identity = observed(&source);
        if close {
            runtime.close_native_outgoing().unwrap();
        } else {
            runtime.invalidate_peer(peer);
        }
        let mut target = None;
        assert!(matches!(
            NativeChannelSource::try_reserve_in(&mut source, &mut target),
            Err(ChannelAcquisitionError::RetryableNetwork(_))
        ));
        original_source(&source, &identity, true);
        assert!(target.is_none());
        no_io(&socket);
        drop(source.take());
        let snapshot = runtime
            .transport_admission()
            .frontend_outgoing_snapshot()
            .unwrap();
        assert_eq!(
            (snapshot.control_connections, snapshot.control_handshakes),
            (0, 0)
        );
        runtime.close_native_outgoing().unwrap();
    }
}

#[tokio::test]
async fn cold_source_close_or_retirement_after_handoff_refuses_before_first_io() {
    for close in [false, true] {
        let (runtime, client, socket, peer) = local_case(NativeEndpointDomain::BackendControl);
        let mut source = Some(
            client
                .prepare_channel_source(NativeRpcMethod::Heartbeat)
                .unwrap(),
        );
        let mut handoff = None;
        assert_eq!(
            NativeChannelSource::try_reserve_in(&mut source, &mut handoff).unwrap(),
            NativeDialAttempt::Installed
        );
        assert!(source.is_none());
        if close {
            runtime.close_native_outgoing().unwrap();
        } else {
            runtime.invalidate_peer(peer);
        }
        let Some(NativeDialHandoff::Reserved(reserved)) = handoff.take() else {
            unreachable!()
        };
        let mut ready = None;
        assert!(matches!(
            reserved.connect_in(&mut ready).await,
            Err(ChannelAcquisitionError::RetryableNetwork(_))
        ));
        assert!(ready.is_none());
        no_io(&socket);
        let after = runtime
            .transport_admission()
            .frontend_outgoing_snapshot()
            .unwrap();
        assert_eq!(
            (after.control_connections, after.control_handshakes),
            (0, 0)
        );
        assert!(
            runtime
                .try_dial_lane(FrontendNativeLane::LifecycleControl)
                .is_some()
        );
        runtime.close_native_outgoing().unwrap();
    }
}

#[derive(Clone)]
struct H2Probe(Arc<AtomicUsize>);
impl tonic::server::NamedService for H2Probe {
    const NAME: &'static str = "cold.dial.Probe";
}
impl Service<http::Request<tonic::body::BoxBody>> for H2Probe {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: http::Request<tonic::body::BoxBody>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        let mut response = http::Response::new(tonic::body::empty_body());
        response
            .headers_mut()
            .insert("x-original-cold-peer", http::HeaderValue::from_static("h2"));
        std::future::ready(Ok(response))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cold_handoff_bootstraps_endpoint_h2_and_reuses_exact_ready_generation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let runtime = original_runtime();
    let client = Client::for_endpoint(
        endpoint.clone(),
        NativeEndpointDomain::BackendControl,
        BackendProcessId::new_v7(),
        runtime.clone(),
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let probe = H2Probe(Arc::clone(&requests));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    // One accepted socket, then no further accepts and no EOF until shutdown.
    // Reuse the existing Tonic component peer/one-original-handle pattern; no
    // new runtime, unbounded peer task set or detached test task is created.
    let incoming =
        futures::stream::once(async move { listener.accept().await.map(|(socket, _)| socket) })
            .chain(futures::stream::pending::<
                Result<tokio::net::TcpStream, std::io::Error>,
            >());
    let mut peer = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(probe)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = stopped.await;
            })
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    // Stable source/receiver and actual original peer handle outlive the
    // cancellable operation. Its original panic/error is saved before cleanup.
    let mut source = None;
    let mut handoff = None;
    let mut acquired = None;
    let operation = async {
        source = Some(client.prepare_channel_source(NativeRpcMethod::Heartbeat)?);
        let identity = observed(&source);
        if NativeChannelSource::try_reserve_in(&mut source, &mut handoff)?
            != NativeDialAttempt::Installed
        {
            return Err("empty original cold lane unexpectedly refused".into());
        }
        let Some(NativeDialHandoff::Reserved(reserved)) = handoff.take() else {
            return Err("new original source did not produce a cold holder".into());
        };
        reserved.connect_in(&mut acquired).await?;
        let original = acquired
            .as_ref()
            .ok_or("original published channel missing")?;
        if original.slot() != &identity.slot {
            return Err("Endpoint publication changed the original selected slot".into());
        }
        let cached = runtime
            .cached_channel(original.slot())
            .ok_or("same published cache row missing")?;
        if !original.same_generation_for_test(&cached) {
            return Err("Endpoint publication changed original generation".into());
        }
        let mut channel = original.channel().clone();
        let request = http::Request::builder()
            .method("POST")
            .uri("/cold.dial.Probe/Ping")
            .body(tonic::body::empty_body())?;
        let response = channel.ready().await?.call(request).await?;
        if response.version() != http::Version::HTTP_2
            || response.headers().get("x-original-cold-peer")
                != Some(&http::HeaderValue::from_static("h2"))
            || requests.load(Ordering::SeqCst) != 1
        {
            return Err("actual Endpoint did not complete the original H2 round trip".into());
        }
        drop(response);
        drop(channel);
        // prepare consumes the actual ready cache row (Control's original one
        // slot); it must not open another connection or generation.
        let mut ready_source = Some(client.prepare_channel_source(NativeRpcMethod::Heartbeat)?);
        let mut ready_handoff = None;
        if NativeChannelSource::try_reserve_in(&mut ready_source, &mut ready_handoff)?
            != NativeDialAttempt::Installed
        {
            return Err("original ready cache handoff refused".into());
        }
        let Some(NativeDialHandoff::Ready(ready)) = ready_handoff.take() else {
            return Err("ready generation was needlessly redialed".into());
        };
        if !original.same_generation_for_test(&ready) {
            return Err("ready handoff changed generation".into());
        }
        drop(ready);
        // connect_in also rejects an occupied receiver before any IO and
        // cannot overwrite the actual original published channel.
        let other = Client::for_endpoint(
            endpoint.clone(),
            NativeEndpointDomain::BackendControl,
            BackendProcessId::new_v7(),
            runtime.clone(),
        );
        let mut other_source = Some(other.prepare_channel_source(NativeRpcMethod::Heartbeat)?);
        let mut other_handoff = None;
        if NativeChannelSource::try_reserve_in(&mut other_source, &mut other_handoff)?
            != NativeDialAttempt::Installed
        {
            return Err("independent local bootstrap holder refused".into());
        }
        let Some(NativeDialHandoff::Reserved(reserved)) = other_handoff.take() else {
            return Err("independent cold holder missing".into());
        };
        if !matches!(
            reserved.connect_in(&mut acquired).await,
            Err(ChannelAcquisitionError::Fatal(_))
        ) {
            return Err("occupied actual channel receiver was accepted".into());
        }
        if !cached
            .same_generation_for_test(acquired.as_ref().ok_or("original receiver was erased")?)
        {
            return Err("occupied receiver was overwritten".into());
        }
        let state = runtime.transport_admission().frontend_outgoing_snapshot()?;
        if state.control_connections != 1
            || state.control_handshakes != 0
            || requests.load(Ordering::SeqCst) != 1
        {
            return Err("occupied receiver refusal retained partial holder or dispatched".into());
        }
        Ok::<_, TestError>(())
    };
    let primary: Result<Result<(), TestError>, Box<dyn Any + Send>> =
        match tokio::time::timeout_at(deadline, AssertUnwindSafe(operation).catch_unwind()).await {
            Ok(primary) => primary,
            Err(error) => Ok(Err(Box::new(error))),
        };
    drop(handoff.take());
    drop(source.take());
    drop(acquired.take());
    let close = runtime.close_native_outgoing();
    let stop_sent = stop.send(());
    // Borrow the same original JoinHandle into the one absolute deadline.
    // Timeout keeps it here; abort is followed by actual join, never detach.
    let (peer_exit, cleanup_timeout) = match tokio::time::timeout_at(deadline, &mut peer).await {
        Ok(exit) => (exit, None),
        Err(timeout) => {
            peer.abort();
            (peer.await, Some(timeout))
        }
    };
    let public_exit = runtime
        .drain_native_outgoing_until(deadline.into_std())
        .await;
    // Keep the exact primary, all cleanup causes and original empty role
    // owner together before inspecting success. A new assertion panic would
    // otherwise unwind and ordinarily drop those opaque original causes.
    struct ColdH2Failure {
        primary: Result<Result<(), TestError>, Box<dyn Any + Send>>,
        close: Result<(), String>,
        stop_sent: Result<(), ()>,
        peer_exit: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>,
        cleanup_timeout: Option<tokio::time::error::Elapsed>,
        public_exit: Result<(), String>,
        // In particular a failed public drain must not drop the original
        // runtime merely because this test is reporting another failure.
        _runtime: FrontendDataRuntime,
    }
    let failure = ColdH2Failure {
        primary,
        close,
        stop_sent,
        peer_exit,
        cleanup_timeout,
        public_exit,
        _runtime: runtime,
    };
    // Borrow every result. Keep the same four success predicates and never
    // format or destructure away an arbitrary panic payload or JoinError.
    let operation_ok = matches!(&failure.primary, Ok(Ok(())));
    let close_stop_ok = failure.close.is_ok() && failure.stop_sent.is_ok();
    let peer_ok = failure.cleanup_timeout.is_none() && matches!(&failure.peer_exit, Ok(Ok(())));
    let public_exit_ok = failure.public_exit.is_ok();
    if operation_ok && close_stop_ok && peer_ok && public_exit_ok {
        return;
    }
    // The Rust test harness receives this whole concrete custody packet.
    // No replacement assert unwinds across the original opaque causes here.
    std::panic::panic_any(failure);
}
