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

//! Native outbound RPC transport over the Server-resolved backend capability.

use std::io;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use novarocks_native_trust::{NativeClientAuthInterceptor, NativeTrust};
use novarocks_proto_codec::native_rpc::{NativeRpcMethod, NativeTrafficClass};
use novarocks_proto_models::{filter, novarocks as proto};
use novarocks_types::{BackendProcessId, NativeEndpoint};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::service::interceptor::InterceptedService;

use crate::BackendDataRuntime;
use crate::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
use crate::native_lane::{NativeDial, NativeLane, NativeLaneChannel, admitted_connector_service};
use crate::native_transport_admission::TransportClass;

const GRPC_MAX_MESSAGE_BYTES: usize =
    novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;

#[cfg(test)]
#[path = "native_peer_key_tests.rs"]
mod peer_key_tests;

#[cfg(test)]
#[path = "native_peer_key_capacity_tests.rs"]
mod peer_key_capacity_tests;

type AuthenticatedNovaRocksGrpcClient =
    NovaRocksGrpcClient<InterceptedService<NativeLaneChannel, NativeClientAuthInterceptor>>;

pub struct NativeRpcClient {
    runtime: BackendDataRuntime,
    endpoint: NativeEndpoint,
    backend_process_id: Option<BackendProcessId>,
}

/// Classification is made at the local dispatch boundary, never by parsing
/// an error string. Receiver-authored refusals travel in the typed response.
#[derive(Clone, Debug)]
pub(crate) enum ExchangeRpcFailure {
    NotProcessed(Box<str>),
    Unknown(Box<str>),
}

fn exchange_rpc_status(status: tonic::Status) -> ExchangeRpcFailure {
    let detail = format!("exchange RPC returned {:?}", status.code()).into_boxed_str();
    match status.code() {
        // These unit methods carry every application refusal in their DTO.
        // These statuses therefore come from pre-execution admission/auth.
        tonic::Code::ResourceExhausted
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied => ExchangeRpcFailure::NotProcessed(detail),
        _ => ExchangeRpcFailure::Unknown(detail),
    }
}

/// BE-origin methods use this role-local cache. Exchange unit methods share a
/// physical connection owner; manifest routing still names the exact RPC.
/// Membership has no BE peer identity.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct NativeChannelKey {
    backend_process_id: Option<BackendProcessId>,
    endpoint: NativeEndpoint,
    method: NativeRpcMethod,
}

impl NativeChannelKey {
    pub(crate) fn inline_identity(
        &self,
    ) -> io::Result<crate::native_channel_identity::InlineNativeChannelIdentity> {
        crate::native_channel_identity::InlineNativeChannelIdentity::from_parts(
            self.backend_process_id,
            &self.endpoint,
            self.method,
        )
    }
    fn new(
        backend_process_id: Option<BackendProcessId>,
        endpoint: NativeEndpoint,
        method: NativeRpcMethod,
    ) -> Result<Self, String> {
        match (method.contract().traffic, backend_process_id) {
            (NativeTrafficClass::Exchange | NativeTrafficClass::RuntimeFilter, Some(_))
            | (NativeTrafficClass::Membership, None) => Ok(Self {
                backend_process_id,
                endpoint,
                method: crate::native_channel_identity::connection_owner_method(method),
            }),
            _ => Err("Native outbound method requires its exact frozen peer domain".to_owned()),
        }
    }

    #[cfg(test)]
    pub(crate) fn membership(endpoint: NativeEndpoint) -> Self {
        Self::new(None, endpoint, NativeRpcMethod::AnnounceBackend)
            .expect("the Native manifest assigns announcement to membership")
    }
}

impl NativeRpcClient {
    pub(crate) async fn deliver_exchange_unit_async(
        &self,
        value: proto::DeliverExchangeUnitRequest,
        timeout: Duration,
        stop: CancellationToken,
    ) -> Result<proto::DeliverExchangeUnitResponse, ExchangeRpcFailure> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                ExchangeRpcFailure::NotProcessed("exchange deadline cannot be represented".into())
            })?;
        let mut client = tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(ExchangeRpcFailure::NotProcessed("exchange stopped before dispatch".into())),
            result = self.make_deadline_async_client("exchange unit", NativeRpcMethod::DeliverExchangeUnit, deadline) => {
                result.map_err(|error| ExchangeRpcFailure::NotProcessed(error.into_boxed_str()))?
            }
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ExchangeRpcFailure::NotProcessed(
                "exchange deadline elapsed before dispatch".into(),
            ));
        }
        let mut request = Request::new(value);
        request.set_timeout(remaining);
        // This future owns this local request. Cancellation drops it in this
        // scope; no detached subrequest outlives the outer activity owner.
        tokio::select! {
            biased;
            _ = stop.cancelled() => Err(ExchangeRpcFailure::Unknown("exchange stopped after dispatch".into())),
            result = tokio::time::timeout_at(deadline, client.deliver_exchange_unit(request)) => {
                result.map_err(|_| ExchangeRpcFailure::Unknown("exchange unit transmission deadline elapsed".into()))?
                    .map(|response| response.into_inner()).map_err(exchange_rpc_status)
            }
        }
    }

    pub(crate) async fn register_exchange_demand_async(
        &self,
        value: proto::RegisterExchangeDemandRequest,
        timeout: Duration,
        stop: CancellationToken,
    ) -> Result<proto::RegisterExchangeDemandResponse, ExchangeRpcFailure> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                ExchangeRpcFailure::NotProcessed("exchange deadline cannot be represented".into())
            })?;
        let mut client = tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(ExchangeRpcFailure::NotProcessed("exchange demand stopped before dispatch".into())),
            result = self.make_deadline_async_client("exchange demand", NativeRpcMethod::RegisterExchangeDemand, deadline) => {
                result.map_err(|error| ExchangeRpcFailure::NotProcessed(error.into_boxed_str()))?
            }
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ExchangeRpcFailure::NotProcessed(
                "exchange deadline elapsed before dispatch".into(),
            ));
        }
        let mut request = Request::new(value);
        request.set_timeout(remaining);
        tokio::select! {
            biased;
            _ = stop.cancelled() => Err(ExchangeRpcFailure::Unknown("exchange demand stopped after dispatch".into())),
            result = tokio::time::timeout_at(deadline, client.register_exchange_demand(request)) => {
                result.map_err(|_| ExchangeRpcFailure::Unknown("exchange demand transmission deadline elapsed".into()))?
                    .map(|response| response.into_inner()).map_err(exchange_rpc_status)
            }
        }
    }

    pub(crate) async fn subscribe_exchange_grants_async(
        &self,
        value: proto::SubscribeExchangeGrantsRequest,
        timeout: Duration,
        stop: CancellationToken,
    ) -> Result<tonic::Streaming<proto::ExchangeGrantBatch>, tonic::Status> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                tonic::Status::invalid_argument(
                    "exchange subscription deadline cannot be represented",
                )
            })?;
        let mut client = tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(tonic::Status::cancelled("exchange subscription stopped before dispatch")),
            result = self.make_deadline_async_client("exchange grant subscription", NativeRpcMethod::SubscribeExchangeGrants, deadline) => {
                result.map_err(tonic::Status::unavailable)?
            }
        };
        // The deadline bounds establishment only. Sending grpc-timeout would
        // incorrectly turn an otherwise healthy observation stream into an
        // input-idle timer after its headers had already arrived.
        tokio::select! {
            biased;
            _ = stop.cancelled() => Err(tonic::Status::cancelled("exchange subscription stopped during establishment")),
            result = tokio::time::timeout_at(deadline, client.subscribe_exchange_grants(Request::new(value))) => {
                result.map_err(|_| tonic::Status::deadline_exceeded("exchange subscription establishment deadline elapsed"))?
                    .map(|response| response.into_inner())
            }
        }
    }

    pub fn new_membership_endpoint(runtime: BackendDataRuntime, endpoint: NativeEndpoint) -> Self {
        Self {
            runtime,
            endpoint,
            backend_process_id: None,
        }
    }

    pub fn new_backend_endpoint(
        runtime: BackendDataRuntime,
        endpoint: NativeEndpoint,
        backend_process_id: BackendProcessId,
    ) -> Self {
        Self {
            runtime,
            endpoint,
            backend_process_id: Some(backend_process_id),
        }
    }

    async fn make_deadline_async_client(
        &self,
        operation: &str,
        method: NativeRpcMethod,
        deadline_at: tokio::time::Instant,
    ) -> Result<AuthenticatedNovaRocksGrpcClient, String> {
        let key = NativeChannelKey::new(self.backend_process_id, self.endpoint.clone(), method)?;
        tokio::time::timeout_at(deadline_at, get_or_create_channel(&self.runtime, key))
            .await
            .map_err(|_| format!("{operation} deadline exceeded during channel acquisition"))?
            .map(|channel| client_from_channel(channel, self.runtime.native_trust().as_ref()))
            .map_err(|error| format!("{operation} channel acquisition failed: {error}"))
    }

    pub async fn transmit_runtime_filter_envelope_async(
        &self,
        request: filter::RuntimeFilterEnvelope,
        deadline: Duration,
    ) -> Result<filter::RuntimeFilterEnvelopeResponse, String> {
        let deadline_at = tokio::time::Instant::now() + deadline;
        let mut client = self
            .make_deadline_async_client(
                "runtime filter envelope",
                NativeRpcMethod::TransmitRuntimeFilterEnvelope,
                deadline_at,
            )
            .await?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(
                "runtime filter envelope deadline exceeded before unary RPC submission".to_string(),
            );
        }
        let mut request = Request::new(request);
        request.set_timeout(remaining);
        tokio::time::timeout_at(
            deadline_at,
            client.transmit_runtime_filter_envelope(request),
        )
        .await
        .map_err(|_| "runtime filter envelope deadline exceeded during unary RPC".to_string())?
        .map(|response| response.into_inner())
        .map_err(|error| format!("transmit_runtime_filter_envelope rpc failed: {error}"))
    }

    pub fn blocking_announce_backend_with_timeout(
        &self,
        request: proto::AnnounceBackendRequest,
        timeout: Duration,
    ) -> Result<proto::AnnounceBackendResponse, String> {
        self.runtime.block_on(async {
            let deadline_at = tokio::time::Instant::now() + timeout;
            let mut client = self
                .make_deadline_async_client(
                    "announce_backend",
                    NativeRpcMethod::AnnounceBackend,
                    deadline_at,
                )
                .await?;
            let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(
                    "announce_backend deadline exceeded before unary RPC submission".to_string(),
                );
            }
            let mut request = Request::new(request);
            request.set_timeout(remaining);
            let response = tokio::time::timeout_at(deadline_at, client.announce_backend(request))
                .await
                .map_err(|_| "announce_backend deadline exceeded during unary RPC".to_string())?
                .map_err(|error| format!("announce_backend rpc failed: {error}"))?
                .into_inner();
            Ok(response)
        })
    }

    pub fn exchange_unary(
        &self,
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        sender_count: u32,
        sender_id: i32,
        be_number: i32,
        eos: bool,
        sequence: i64,
        payload: Vec<u8>,
        timeout: Duration,
        stop: CancellationToken,
    ) -> Result<Option<proto::ExchangeNormalClosed>, String> {
        let request = proto::ExchangeRequest {
            stream: Some(novarocks_task_codec::exchange_flow_codec::encode_stream(
                stream,
            )),
            finst_id_hi: stream.destination_fragment_instance_id().high(),
            finst_id_lo: stream.destination_fragment_instance_id().low(),
            node_id: stream.destination_node().get(),
            source_finst_id_hi: stream.source().fragment_instance_id().high(),
            source_finst_id_lo: stream.source().fragment_instance_id().low(),
            sender_ordinal: stream.source().sender_ordinal(),
            sender_count,
            sender_id,
            be_number,
            eos,
            sequence,
            payload,
        };
        self.runtime.block_on(async {
            tokio::select! {
                biased;
                _ = stop.cancelled() => Err("exchange send cancelled by task stop".to_string()),
                result = async {
            let deadline_at = tokio::time::Instant::now() + timeout;
            let mut client = self
                .make_deadline_async_client("exchange", NativeRpcMethod::ExchangeUnary, deadline_at)
                .await?;
            let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err("exchange deadline exceeded before unary RPC submission".to_string());
            }
            let mut request = Request::new(request);
            request.set_timeout(remaining);
            let response = client
                .exchange_unary(request);
            let response = tokio::time::timeout_at(deadline_at, response)
                .await
                .map_err(|_| "exchange deadline exceeded during unary RPC".to_string())?
                .map_err(|error| format!("exchange rpc failed: {error}"))?
                .into_inner();
            if response.ack_sequence != sequence {
                return Err("exchange rpc returned a different request sequence".to_string());
            }
            let status = response.status.as_ref().ok_or_else(||
                "exchange rpc omitted its status".to_string()
            )?;
            if status.code != 0 {
                return Err(if status.message.is_empty() {
                    format!("exchange rpc returned status_code={}", status.code)
                } else {
                    format!("exchange rpc failed: {}", status.message)
                });
            }
            Ok(response.normal_closed)
                } => result,
            }
        })
    }
}

fn channel_endpoint(
    endpoint: &NativeEndpoint,
) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
    format!("http://{endpoint}").parse()
}

/// Endpoint settings for an outgoing Native connection. An admitted runtime
/// applies the frozen public HTTP/2 limits; the counts and per-item sizes they
/// set bound what Hyper/H2 allocate, and the transport measurement gate covers
/// the rest (spec v6 §5.9).
pub(crate) fn native_endpoint(
    runtime: &BackendDataRuntime,
    endpoint: &NativeEndpoint,
) -> Result<tonic::transport::Endpoint, String> {
    let endpoint =
        channel_endpoint(endpoint).map_err(|error| format!("invalid endpoint: {error}"))?;
    if runtime.transport_admission().is_some() {
        Ok(crate::native_lane::configure_native_endpoint(endpoint))
    } else {
        Ok(endpoint
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .connect_timeout(Duration::from_secs(10))
            .http2_adaptive_window(true)
            .initial_stream_window_size(Some(32 * 1024 * 1024))
            .initial_connection_window_size(Some(128 * 1024 * 1024)))
    }
}

/// The transport connector, preceded by this runtime's dial admission. The
/// admission is taken again on every attempt, including Tonic's internal
/// reconnect; a refused attempt opens no socket, and the physical position of
/// an established connection follows its IO until the IO is dropped.
pub(crate) fn admitted_connector(
    runtime: &BackendDataRuntime,
    endpoint: &NativeEndpoint,
    class: TransportClass,
    key: Option<crate::native_channel_identity::InlineNativeChannelIdentity>,
) -> Result<
    impl tower::Service<
        tonic::codegen::http::Uri,
        Response = TokioIo<novarocks_transport_io::BoxedIo>,
        Error = io::Error,
        Future = impl Send,
    > + Clone
    + Send
    + 'static,
    String,
> {
    let connector = runtime.native_transport().connector_for(endpoint.clone())?;
    Ok(admitted_connector_service(
        connector,
        runtime.transport_admission().cloned(),
        NativeDial::Backend(class, key),
    ))
}

fn client_from_channel(
    channel: NativeLaneChannel,
    trust: &NativeTrust,
) -> AuthenticatedNovaRocksGrpcClient {
    NovaRocksGrpcClient::with_interceptor(channel, trust.client_interceptor())
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
}

async fn get_or_create_channel(
    runtime: &BackendDataRuntime,
    key: NativeChannelKey,
) -> Result<NativeLaneChannel, String> {
    let identity = key
        .inline_identity()
        .map_err(|error| format!("Native channel identity refused: {error}"))?;
    let leader = match runtime
        .channels()
        .acquire(identity)
        .await
        .map_err(|error| format!("Native channel election refused: {error}"))?
    {
        crate::native_channel_cache::Election::Ready(channel) => return Ok(channel),
        crate::native_channel_cache::Election::Leader(leader) => leader,
    };
    let connector =
        admitted_connector(runtime, &key.endpoint, TransportClass::Data, Some(identity))?;
    let channel = native_endpoint(runtime, &key.endpoint)?
        .timeout(Duration::from_secs(600))
        .connect_with_connector(connector)
        .await
        .map_err(|error| format!("connect exchange endpoint failed: {error}"))?;
    let lane = NativeLane::of(key.method.contract().traffic)
        .ok_or_else(|| "retired Native method has no lane".to_owned())?;
    // One Channel is one connection; its stream positions live with it.
    let channel = NativeLaneChannel::new(channel, lane, runtime.transport_admission());
    leader
        .publish(channel.clone())
        .map_err(|error| format!("Native channel publication refused: {error}"))?;
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    use tokio_util::sync::CancellationToken;

    use super::{NativeRpcClient, channel_endpoint};

    fn send_to_test_endpoint(
        port: u16,
        timeout: Duration,
        stop: CancellationToken,
    ) -> Result<(), String> {
        let client = NativeRpcClient::new_backend_endpoint(
            crate::backend_test_support::test_backend_data_runtime(),
            novarocks_types::NativeEndpoint::from_host_port("127.0.0.1", port).unwrap(),
            novarocks_types::BackendProcessId::new_v7(),
        );
        client
            .exchange_unary(
                crate::exchange_transmitter::tests::frame_and_proof()
                    .0
                    .stream
                    .unwrap(),
                2,
                0,
                0,
                false,
                0,
                Vec::new(),
                timeout,
                stop,
            )
            .map(|_| ())
    }

    #[test]
    fn task_stop_interrupts_exchange_before_channel_acquisition() {
        let stop = CancellationToken::new();
        stop.cancel();
        let started = Instant::now();
        let error = send_to_test_endpoint(1, Duration::from_secs(10), stop)
            .expect_err("cancelled task cannot start a send");
        assert!(error.contains("cancelled by task stop"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn task_stop_interrupts_a_stalled_exchange_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind blackhole endpoint");
        listener
            .set_nonblocking(true)
            .expect("set blackhole nonblocking");
        let port = listener.local_addr().expect("blackhole address").port();
        let stop = CancellationToken::new();
        let worker_stop = stop.clone();
        let send =
            thread::spawn(move || send_to_test_endpoint(port, Duration::from_secs(3), worker_stop));
        let accept_deadline = Instant::now() + Duration::from_secs(1);
        let accepted = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < accept_deadline, "exchange did not connect");
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("blackhole accept failed: {error}"),
            }
        };
        let started = Instant::now();
        stop.cancel();
        let error = send
            .join()
            .expect("exchange worker did not panic")
            .expect_err("stalled exchange should stop");
        drop(accepted);
        assert!(error.contains("cancelled by task stop"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn exchange_deadline_covers_a_stalled_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind blackhole endpoint");
        let port = listener.local_addr().expect("blackhole address").port();
        let started = Instant::now();
        let error =
            send_to_test_endpoint(port, Duration::from_millis(30), CancellationToken::new())
                .expect_err("blackhole exchange must time out");
        assert!(
            error.contains("deadline exceeded") || error.contains("Timeout expired"),
            "{error}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn channel_endpoint_formats_ipv4_and_ipv6_hosts() {
        assert_eq!(
            channel_endpoint(&"127.0.0.1:9070".parse().expect("IPv4 endpoint"))
                .expect("IPv4 endpoint")
                .uri()
                .to_string(),
            "http://127.0.0.1:9070/"
        );
        assert_eq!(
            channel_endpoint(&"[::1]:9070".parse().expect("IPv6 endpoint"))
                .expect("IPv6 endpoint")
                .uri()
                .to_string(),
            "http://[::1]:9070/"
        );
    }
}

#[cfg(test)]
mod exchange_unit_call_tests {
    use super::*;

    #[test]
    fn admission_statuses_are_not_processed_and_ambiguous_transport_is_unknown() {
        for code in [
            tonic::Code::ResourceExhausted,
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
        ] {
            assert!(matches!(
                exchange_rpc_status(tonic::Status::new(code, "bounded test")),
                ExchangeRpcFailure::NotProcessed(_)
            ));
        }
        for code in [
            tonic::Code::DeadlineExceeded,
            tonic::Code::Unavailable,
            tonic::Code::Internal,
            tonic::Code::DataLoss,
        ] {
            assert!(matches!(
                exchange_rpc_status(tonic::Status::new(code, "bounded test")),
                ExchangeRpcFailure::Unknown(_)
            ));
        }
    }

    #[tokio::test]
    async fn connection_refusal_before_dispatch_is_not_processed() {
        let endpoint = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            novarocks_types::NativeEndpoint::from_socket_addr(listener.local_addr().unwrap())
        };
        let client = NativeRpcClient::new_backend_endpoint(
            crate::backend_test_support::test_backend_data_runtime(),
            endpoint,
            BackendProcessId::new_v7(),
        );
        let result = client
            .deliver_exchange_unit_async(
                proto::DeliverExchangeUnitRequest::default(),
                Duration::from_millis(500),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(result, Err(ExchangeRpcFailure::NotProcessed(_))));
    }
}
