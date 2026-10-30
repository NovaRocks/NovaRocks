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

//! Component-only original FE eager factory and cached TLS reconnect bracket.
//! This does not measure queue occupancy or allocator coefficients.
use super::*;
use futures::FutureExt;
use novarocks_native_adapter::FrontendNativeTransport;
use novarocks_native_trust::{
    AutomaticTlsMaterial, DeploymentId, NativeCallerSubject, NativeIncomingAdapter,
    NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

type E = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Bracket {
    same_cached_generation: bool,
    cold_io: usize,
    exited_old_io: usize,
    hello: [u8; 6],
    peer_writes_on_reconnect: usize,
    reconnect_positions: usize,
    reconnect_handshakes: usize,
    original_call_pending: bool,
    original_lane_remaining: usize,
    original_requests: [usize; 4],
}

impl Bracket {
    fn check(&self) -> Result<(), &'static str> {
        if !self.same_cached_generation || self.cold_io != 1 || self.exited_old_io != 0 {
            return Err("original eager cache/IO identity differs");
        }
        let record_length = u16::from_be_bytes([self.hello[3], self.hello[4]]);
        if self.hello[0] != 22
            || self.hello[1] != 3
            || !matches!(self.hello[2], 1 | 3)
            || record_length < 4
            || self.hello[5] != 1
            || self.peer_writes_on_reconnect != 0
        {
            return Err("actual withheld ClientHello bracket differs");
        }
        // Physical positions include connecting sockets; handshakes are a
        // subset. The withheld TLS handshake retains both original positions.
        if self.reconnect_positions != 1
            || self.reconnect_handshakes != 1
            || !self.original_call_pending
            || self.original_lane_remaining != 127
            || self.original_requests != [0, 1, 0, 0]
        {
            return Err("original TLS pending/public qualification bracket differs");
        }
        Ok(())
    }
}

#[derive(Debug)]
struct BracketFailure {
    reason: &'static str,
    actual: Bracket,
}

impl std::fmt::Display for BracketFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason)
    }
}
impl std::error::Error for BracketFailure {}

async fn pending<F: Future>(mut future: Pin<&mut F>) -> bool {
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

// The peer never consumes the prefix; it may later feed this same socket into
// the public TLS acceptor. Only six fixed bytes are retained; no TLS/body log.
async fn client_hello_prefix(socket: &tokio::net::TcpStream) -> Result<[u8; 6], E> {
    let mut prefix = [0; 6];
    loop {
        match socket.peek(&mut prefix).await? {
            0 => return Err("original TLS peer closed before ClientHello prefix".into()),
            6 => return Ok(prefix),
            _ => {}
        }
        tokio::task::yield_now().await;
    }
}

struct Outcome {
    operation: Result<Result<Bracket, E>, Box<dyn Any + Send>>,
    close: Result<(), String>,
    peer: Vec<Result<Result<(), E>, tokio::task::JoinError>>,
    wait_timeout: Option<tokio::time::error::Elapsed>,
    public_exit: Result<(), String>,
    late: bool,
}

// This fixed peer has one original handle. Its output vector contains at most
// that one join result; no output, raw TLS, or arbitrary cause is formatted.
async fn original_eager_tls_reconnect(retire_before_bracket: bool) -> Result<Outcome, E> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = NativeEndpoint::from_host_port("127.0.0.1", listener.local_addr()?.port())?;
    let trust = NativeTrust::new(
        DeploymentId::parse("eager-tls-component")?,
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))?,
        NativeCallerSubject::parse("fe@127.0.0.1:19040")?,
        NativeTransportMode::Automatic,
    );
    let material = AutomaticTlsMaterial::for_endpoint(trust.clone(), endpoint.clone())?;
    let incoming = NativeIncomingAdapter::automatic(&material);
    // Original process owner is unstarted/empty: this test submits no Apply
    // owner job and no covered subscription. No reaper exit is fabricated.
    let runtime = FrontendDataRuntime::new_with_native_trust(
        tokio::runtime::Handle::current(),
        Arc::new(trust),
        FrontendNativeTransport::automatic(material),
        novarocks_task_codec::TransportBudget::DEFAULT,
        novarocks_workload_control::WorkloadConfig::default().scope_records_limit,
    )?;
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                .transport_connect_deadline_ms,
        );
    let (close_first, closed_first) = oneshot::channel();
    let (send_hello, mut recv_hello) = oneshot::channel();
    let (stop, stopped) = oneshot::channel();
    let mut peer = JoinSet::new();
    peer.spawn(async move {
        let work = async {
            let (first_tcp, _) = listener.accept().await?;
            let first_tls = incoming.accept(first_tcp).await?;
            closed_first.await?;
            drop(first_tls);
            let (second_tcp, _) = listener.accept().await?;
            let hello = client_hello_prefix(&second_tcp).await?;
            send_hello
                .send(hello)
                .map_err(|_| "original hello receiver closed")?;
            // Zero bytes are written on this connection. No server TLS future
            // is started, so ServerHello cannot be sent before parent stop.
            stopped.await?;
            drop(second_tcp);
            Ok::<_, E>(())
        };
        match tokio::time::timeout_at(deadline, work).await {
            Ok(result) => result,
            Err(error) => Err(Box::new(error) as E),
        }
    });
    let mut acquired: Option<CachedNativeChannel> = None;
    let mut grpc: Option<AuthenticatedNovaRocksGrpcClient> = None;
    let operation = async {
        let client = Client::for_endpoint(
            endpoint,
            NativeEndpointDomain::BackendData,
            BackendProcessId::new_v7(),
            runtime.clone(),
        );
        let (original_grpc, original_cache) = client
            .grpc_with_channel_identity(NativeRpcMethod::PruneCatalogs)
            .await
            .map_err(|original| FactoryCause { original })?;
        acquired = Some(original_cache);
        grpc = Some(original_grpc);
        let original = acquired.as_ref().ok_or("original cached channel missing")?;
        let cold = runtime.transport_admission().frontend_outgoing_snapshot()?;
        close_first
            .send(())
            .map_err(|_| "original first TLS peer exited early")?;
        let old_exit = loop {
            let state = runtime.transport_admission().frontend_outgoing_snapshot()?;
            if state.data_connections == 0 && state.data_handshakes == 0 {
                break state;
            }
            tokio::task::yield_now().await;
        };
        // Generated client uses the original auth interceptor. The peer does
        // not reach application decode: this is transport-only component input.
        let request = PruneCatalogsRequest::new(std::iter::empty())?;
        let mut call = Box::pin(
            grpc.as_mut()
                .ok_or("original grpc missing")?
                .prune_catalogs(Request::new(request.as_proto().clone())),
        );
        if !pending(call.as_mut()).await {
            return Err("original call completed before TLS hold".into());
        }
        let hello = loop {
            match recv_hello.try_recv() {
                Ok(hello) => break hello,
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err("original hello sender exited".into());
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
            if !pending(call.as_mut()).await {
                return Err("original call exited while TLS held".into());
            }
            tokio::task::yield_now().await;
        };
        let state = runtime.transport_admission().frontend_outgoing_snapshot()?;
        if retire_before_bracket && !runtime.invalidate_channel_if_current(original) {
            return Err("original current cache invalidation failed".into());
        }
        let cached = runtime
            .cached_channel(original.slot())
            .ok_or(CacheRetired)?;
        let bracket = Bracket {
            same_cached_generation: original.same_generation_for_test(&cached),
            cold_io: cold.data_connections,
            exited_old_io: old_exit.data_connections,
            hello,
            peer_writes_on_reconnect: 0,
            reconnect_positions: state.data_connections,
            reconnect_handshakes: state.data_handshakes,
            original_call_pending: pending(call.as_mut()).await,
            original_lane_remaining: original.channel().available_streams(),
            original_requests: state.requests,
        };
        bracket.check().map_err(|reason| BracketFailure {
            reason,
            actual: bracket,
        })?;
        // Actual original caller future Drop; not a completed RPC/healthy reply.
        drop(call);
        Ok::<_, E>(bracket)
    };
    let operation =
        match tokio::time::timeout_at(deadline, AssertUnwindSafe(operation).catch_unwind()).await {
            Ok(result) => result,
            Err(error) => Ok(Err(Box::new(error) as E)),
        };
    // Parent retains original runtime, caller clients, sockets and JoinSet even
    // if operation timeout/panic drops its borrowed future.
    drop(grpc.take());
    drop(acquired.take());
    let close = runtime.close_native_outgoing();
    let _ = stop.send(());
    let mut joined = Vec::with_capacity(1);
    let wait_timeout = match tokio::time::timeout_at(deadline, peer.join_next()).await {
        Ok(Some(result)) => {
            joined.push(result);
            None
        }
        Ok(None) => None,
        Err(error) => {
            peer.abort_all();
            Some(error)
        }
    };
    while let Some(result) = peer.join_next().await {
        joined.push(result);
    }
    let public_exit = runtime
        .drain_native_outgoing_until(deadline.into_std())
        .await;
    Ok(Outcome {
        operation,
        close,
        peer: joined,
        wait_timeout,
        public_exit,
        late: tokio::time::Instant::now() >= deadline,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_fe_eager_cache_reconnects_same_slot_into_withheld_tls() {
    let outcome = original_eager_tls_reconnect(false)
        .await
        .expect("bounded component construction");
    // All assertions follow actual original peer join and public drain; errors
    // and the original panic object remain owned, with only finite presentation.
    if let Ok(Err(error)) = &outcome.operation
        && let Some(failure) = error.downcast_ref::<BracketFailure>()
    {
        // Static reason and fixed public counters only; never format an
        // arbitrary original transport/TLS/provider cause.
        eprintln!(
            "bracket refusal: {} actual={:?}",
            failure.reason, failure.actual
        );
    }
    assert!(
        matches!(&outcome.operation, Ok(Ok(_))),
        "original operation failed/panicked"
    );
    assert!(outcome.close.is_ok(), "original cache close failed");
    assert!(
        outcome.wait_timeout.is_none()
            && outcome.peer.len() == 1
            && matches!(&outcome.peer[0], Ok(Ok(()))),
        "original peer did not exit successfully"
    );
    assert!(
        outcome.public_exit.is_ok() && !outcome.late,
        "original public exits late/failed"
    );
}

struct FactoryCause {
    // Original existing acquisition enum is retained, not formatted or remapped.
    #[allow(dead_code)]
    original: ChannelAcquisitionError,
}
impl std::fmt::Debug for FactoryCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FactoryCause(original retained)")
    }
}
impl std::fmt::Display for FactoryCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original eager FE channel acquisition failed")
    }
}
impl std::error::Error for FactoryCause {}

#[derive(Debug)]
struct CacheRetired;
impl std::fmt::Display for CacheRetired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original cache retired before measurement bracket")
    }
}
impl std::error::Error for CacheRetired {}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_current_cache_retirement_cannot_pass_same_slot_bracket() {
    let outcome = original_eager_tls_reconnect(true)
        .await
        .expect("bounded component construction");
    let typed_retirement =
        matches!(&outcome.operation, Ok(Err(error)) if error.is::<CacheRetired>());
    assert!(
        outcome.close.is_ok()
            && outcome.wait_timeout.is_none()
            && outcome.peer.len() == 1
            && matches!(&outcome.peer[0], Ok(Ok(())))
            && outcome.public_exit.is_ok()
            && !outcome.late,
        "original negative fixture cleanup failed"
    );
    assert!(
        typed_retirement,
        "actual cache retirement was not the original refusal"
    );
}

#[test]
fn reconnect_bracket_requires_original_cache_and_actual_tls_pending() {
    let valid = Bracket {
        same_cached_generation: true,
        cold_io: 1,
        exited_old_io: 0,
        hello: [22, 3, 1, 0, 8, 1],
        peer_writes_on_reconnect: 0,
        reconnect_positions: 1,
        reconnect_handshakes: 1,
        original_call_pending: true,
        original_lane_remaining: 127,
        original_requests: [0, 1, 0, 0],
    };
    assert!(valid.check().is_ok());
    for bad in [
        Bracket {
            same_cached_generation: false,
            ..valid
        },
        Bracket {
            exited_old_io: 1,
            ..valid
        },
        Bracket {
            peer_writes_on_reconnect: 1,
            ..valid
        },
        Bracket {
            hello: [23, 3, 3, 0, 8, 1],
            ..valid
        },
        Bracket {
            reconnect_positions: 0,
            ..valid
        },
        Bracket {
            reconnect_handshakes: 0,
            ..valid
        },
        Bracket {
            original_call_pending: false,
            ..valid
        },
        Bracket {
            original_requests: [0; 4],
            ..valid
        },
    ] {
        assert!(bad.check().is_err());
    }
}
