// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The actual post-connect publication owns IO through refusal and unwind.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::task::{Context, Poll};
use std::time::Duration;

use novarocks_types::{BackendProcessId, NativeEndpoint};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

use super::*;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("dial exit fixture must finish")
}

async fn sockets(listener: &TcpListener) -> (TcpStream, TcpStream) {
    let (client, (peer, _)) = bounded(async {
        tokio::try_join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        )
    })
    .await
    .unwrap();
    (client, peer)
}

#[derive(Default)]
struct ExitFacts {
    used_positions: AtomicUsize,
    connecting_still_held: AtomicBool,
    socket_destroyed: AtomicBool,
}

struct ObservedSocket {
    socket: Option<TcpStream>,
    admission: NativeTransportAdmission,
    key: Option<InlineNativeChannelIdentity>,
    facts: Arc<ExitFacts>,
}

impl Drop for ObservedSocket {
    fn drop(&mut self) {
        // Record rather than assert here: an observer may already be unwinding.
        self.facts.used_positions.store(
            self.admission.positions(TransportClass::Data)
                - self.admission.available_positions(TransportClass::Data),
            Ordering::SeqCst,
        );
        if let Some(key) = self.key {
            match self.admission.core.connection_keys.claim(key) {
                Err(error) => self
                    .facts
                    .connecting_still_held
                    .store(error.kind() == io::ErrorKind::WouldBlock, Ordering::SeqCst),
                Ok(token) => {
                    // Keep the failing counterexample itself free of leaked rows.
                    let _ = self.admission.core.connection_keys.retire(token);
                    let _ = self.admission.core.connection_keys.exit(token);
                }
            }
        }
        drop(self.socket.take());
        self.facts.socket_destroyed.store(true, Ordering::SeqCst);
    }
}

impl AsyncRead for ObservedSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ObservedSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn full_live_lane_refusal_destroys_actual_io_before_returning_original_positions() {
    let admission = NativeTransportAdmission::new().unwrap();
    let listener = bounded(TcpListener::bind("127.0.0.1:0")).await.unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let key = InlineNativeChannelIdentity::from_parts(
        Some(BackendProcessId::new_v7()),
        &endpoint,
        NativeRpcMethod::TransmitRuntimeFilterEnvelope,
    )
    .unwrap();
    let first = admission.try_dial(TransportClass::Data, Some(key)).unwrap();
    let (socket, mut first_peer) = sockets(&listener).await;
    let first = first.established_io(Box::new(socket)).unwrap();
    assert_eq!(
        admission.available_handshakes(TransportClass::Data),
        admission.handshake_positions(TransportClass::Data)
    );
    let second = admission.try_dial(TransportClass::Data, Some(key)).unwrap();
    let (socket, mut second_peer) = sockets(&listener).await;
    let facts = Arc::new(ExitFacts::default());
    let error = second
        .established_io(Box::new(ObservedSocket {
            socket: Some(socket),
            admission: admission.clone(),
            key: Some(key),
            facts: Arc::clone(&facts),
        }))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(facts.used_positions.load(Ordering::SeqCst), 2);
    assert!(facts.connecting_still_held.load(Ordering::SeqCst));
    assert!(facts.socket_destroyed.load(Ordering::SeqCst));
    assert_eq!(bounded(second_peer.read(&mut [0])).await.unwrap(), 0);
    assert_eq!(
        admission.available_positions(TransportClass::Data),
        admission.positions(TransportClass::Data) - 1
    );
    drop(first);
    assert_eq!(bounded(first_peer.read(&mut [0])).await.unwrap(), 0);
    assert_eq!(
        admission.available_positions(TransportClass::Data),
        admission.positions(TransportClass::Data)
    );
}

struct PanicObserver {
    handshake: bool,
    armed: AtomicBool,
}
impl NativeTransportObserver for PanicObserver {
    fn positions(&self, _: TransportClass, kind: PositionKind, used: usize, _: usize) {
        if self.handshake
            && kind == PositionKind::Handshake
            && used == 0
            && self.armed.swap(false, Ordering::SeqCst)
        {
            panic_any(93_u8);
        }
    }
    fn lane_connections(&self, _: NativeLane, delta: i64) {
        if !self.handshake && delta == 1 && self.armed.swap(false, Ordering::SeqCst) {
            panic_any(93_u8);
        }
    }
}

#[tokio::test]
async fn observer_unwind_destroys_actual_io_before_returning_original_positions() {
    for handshake in [true, false] {
        let observer = Arc::new(PanicObserver {
            handshake,
            armed: AtomicBool::new(false),
        });
        let admission = NativeTransportAdmission::frontend(Some(observer.clone())).unwrap();
        let listener = bounded(TcpListener::bind("127.0.0.1:0")).await.unwrap();
        let dial = admission
            .try_dial_lane(FrontendNativeLane::ResultData)
            .unwrap();
        let (socket, mut peer) = sockets(&listener).await;
        let facts = Arc::new(ExitFacts::default());
        observer.armed.store(true, Ordering::SeqCst);
        let result = catch_unwind(AssertUnwindSafe(|| {
            dial.established_io(Box::new(ObservedSocket {
                socket: Some(socket),
                admission: admission.clone(),
                key: None,
                facts: Arc::clone(&facts),
            }))
        }));
        let cause = result.expect_err("original observer must unwind");
        assert_eq!(cause.downcast_ref::<u8>(), Some(&93));
        assert_eq!(facts.used_positions.load(Ordering::SeqCst), 1);
        assert!(facts.socket_destroyed.load(Ordering::SeqCst));
        assert_eq!(bounded(peer.read(&mut [0])).await.unwrap(), 0);
        assert_eq!(
            admission.available_positions(TransportClass::Data),
            admission.positions(TransportClass::Data)
        );
        assert_eq!(
            admission.available_handshakes(TransportClass::Data),
            admission.handshake_positions(TransportClass::Data)
        );
    }
}

fn single_frontend_dial_position() -> NativeTransportAdmission {
    NativeTransportAdmission::with_parts(
        TransportRole::Frontend,
        AdmissionDimensions {
            data_positions: 1,
            control_positions: 1,
            data_handshakes: 1,
            control_handshakes: 1,
        },
        None,
    )
    .unwrap()
}

#[test]
fn reserved_frontend_dial_drop_returns_the_original_pair() {
    let admission = single_frontend_dial_position();
    let held = admission
        .try_reserve_frontend_dial(FrontendNativeLane::Observation)
        .unwrap();
    assert_eq!(admission.available_positions(TransportClass::Data), 0);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 0);
    assert!(
        admission
            .try_reserve_frontend_dial(FrontendNativeLane::ResultData)
            .is_err()
    );
    drop(held);
    assert_eq!(admission.available_positions(TransportClass::Data), 1);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
}

#[tokio::test]
async fn reserved_connector_consumes_first_pair_once_then_reconnects_with_original_author() {
    use tower::Service;
    let admission = single_frontend_dial_position();
    let listener = bounded(TcpListener::bind("127.0.0.1:0")).await.unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let held = admission
        .try_reserve_frontend_dial(FrontendNativeLane::Observation)
        .unwrap();
    let mut connector = held.into_connector(
        novarocks_native_trust::NativeEndpointConnector::plaintext(endpoint),
    );
    let uri: hyper::http::Uri = "http://original-cold-dial.test".parse().unwrap();
    // The one original physical position is already held. A second acquire
    // here would fail: real first IO success is the no-double-acquire witness.
    let (first, first_peer) =
        bounded(async { tokio::try_join!(connector.call(uri.clone()), listener.accept()) })
            .await
            .unwrap();
    let mut first_peer = first_peer.0;
    assert_eq!(admission.available_positions(TransportClass::Data), 0);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
    // A reconnect cannot reuse the first qualification while its IO lives.
    let refused = match bounded(connector.call(uri.clone())).await {
        Err(error) => error,
        Ok(io) => {
            drop(io);
            panic!("reconnect borrowed an occupied original position");
        }
    };
    assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
    drop(first);
    assert_eq!(bounded(first_peer.read(&mut [0])).await.unwrap(), 0);
    assert_eq!(admission.available_positions(TransportClass::Data), 1);
    // The same nonClone service now takes a fresh pair from the original
    // author; no new pool/registry or fallback connector exists.
    let (second, second_peer) =
        bounded(async { tokio::try_join!(connector.call(uri), listener.accept()) })
            .await
            .unwrap();
    let mut second_peer = second_peer.0;
    assert_eq!(admission.available_positions(TransportClass::Data), 0);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
    drop(second);
    assert_eq!(bounded(second_peer.read(&mut [0])).await.unwrap(), 0);
    assert_eq!(admission.available_positions(TransportClass::Data), 1);
}

#[tokio::test]
async fn reserved_connector_observes_close_before_starting_first_io() {
    use tower::Service;
    let admission = single_frontend_dial_position();
    let held = admission
        .try_reserve_frontend_dial(FrontendNativeLane::Observation)
        .unwrap();
    admission.close_frontend_outgoing().unwrap();
    let mut connector =
        held.into_connector(novarocks_native_trust::NativeEndpointConnector::plaintext(
            NativeEndpoint::from_socket_addr("127.0.0.1:9".parse().unwrap()),
        ));
    let uri: hyper::http::Uri = "http://original-closed-dial.test".parse().unwrap();
    let error = match bounded(connector.call(uri)).await {
        Err(error) => error,
        Ok(io) => {
            drop(io);
            panic!("closed frontend started new IO");
        }
    };
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    assert_eq!(admission.available_positions(TransportClass::Data), 1);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
    assert!(
        admission
            .try_reserve_frontend_dial(FrontendNativeLane::Observation)
            .is_err()
    );
}
