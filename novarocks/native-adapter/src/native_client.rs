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
use novarocks_proto_models::{filter, novarocks as proto};
use novarocks_types::NativeEndpoint;
use novarocks_types::identity::UniqueId;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tower::service_fn;

use crate::BackendDataRuntime;
use crate::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;

const GRPC_MAX_MESSAGE_BYTES: usize =
    novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;

type AuthenticatedNovaRocksGrpcClient =
    NovaRocksGrpcClient<InterceptedService<Channel, NativeClientAuthInterceptor>>;

pub struct NativeRpcClient {
    runtime: BackendDataRuntime,
    endpoint: NativeEndpoint,
}

impl NativeRpcClient {
    pub fn new_native_endpoint(runtime: BackendDataRuntime, endpoint: NativeEndpoint) -> Self {
        Self { runtime, endpoint }
    }

    pub fn new_host_port(
        runtime: BackendDataRuntime,
        host: String,
        port: u16,
    ) -> Result<Self, String> {
        let endpoint = NativeEndpoint::from_host_port(&host, port)
            .map_err(|error| format!("invalid BE endpoint: {error}"))?;
        channel_endpoint(&endpoint)
            .map_err(|error| format!("invalid BE endpoint {endpoint}: {error}"))?;
        Ok(Self { runtime, endpoint })
    }

    async fn make_deadline_async_client(
        &self,
        operation: &str,
        deadline_at: tokio::time::Instant,
    ) -> Result<AuthenticatedNovaRocksGrpcClient, String> {
        tokio::time::timeout_at(
            deadline_at,
            get_or_create_channel(&self.runtime, self.endpoint.clone()),
        )
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
            .make_deadline_async_client("runtime filter envelope", deadline_at)
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
                .make_deadline_async_client("announce_backend", deadline_at)
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

    #[expect(
        clippy::too_many_arguments,
        reason = "The frozen native boundary keeps independently validated inputs explicit."
    )]
    pub fn exchange_unary(
        &self,
        finst_id: UniqueId,
        node_id: i32,
        source_finst_id: UniqueId,
        sender_ordinal: u32,
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
            finst_id_hi: finst_id.high(),
            finst_id_lo: finst_id.low(),
            node_id,
            source_finst_id_hi: source_finst_id.high(),
            source_finst_id_lo: source_finst_id.low(),
            sender_ordinal,
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
                .make_deadline_async_client("exchange", deadline_at)
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

fn client_from_channel(channel: Channel, trust: &NativeTrust) -> AuthenticatedNovaRocksGrpcClient {
    NovaRocksGrpcClient::with_interceptor(channel, trust.client_interceptor())
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
}

async fn get_or_create_channel(
    runtime: &BackendDataRuntime,
    endpoint: NativeEndpoint,
) -> Result<Channel, String> {
    if let Some(channel) = runtime
        .channels()
        .lock()
        .expect("native channel cache lock")
        .get(&endpoint)
        .cloned()
    {
        return Ok(channel);
    }
    let connector = runtime.native_transport().connector_for(endpoint.clone())?;
    let connector = service_fn(move |_| {
        let connector = connector.clone();
        async move {
            connector
                .connect()
                .await
                .map(TokioIo::new)
                .map_err(|failure| {
                    io::Error::other(format!("native transport connector failed: {failure}"))
                })
        }
    });
    let channel = channel_endpoint(&endpoint)
        .map_err(|error| format!("invalid endpoint: {error}"))?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .initial_stream_window_size(Some(32 * 1024 * 1024))
        .initial_connection_window_size(Some(128 * 1024 * 1024))
        .connect_with_connector(connector)
        .await
        .map_err(|error| format!("connect exchange endpoint failed: {error}"))?;
    runtime
        .channels()
        .lock()
        .expect("native channel cache lock")
        .insert(endpoint, channel.clone());
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    use novarocks_types::UniqueId;
    use tokio_util::sync::CancellationToken;

    use super::{NativeRpcClient, channel_endpoint};

    fn send_to_test_endpoint(
        port: u16,
        timeout: Duration,
        stop: CancellationToken,
    ) -> Result<(), String> {
        let client = NativeRpcClient::new_host_port(
            crate::backend_test_support::test_backend_data_runtime(),
            "127.0.0.1".to_string(),
            port,
        )
        .expect("legal endpoint");
        client
            .exchange_unary(
                UniqueId::new(1, 2),
                3,
                UniqueId::new(4, 5),
                0,
                1,
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
