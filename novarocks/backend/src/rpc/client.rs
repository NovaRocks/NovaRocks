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

//! Backend-owned outbound RPC transport.
//!
//! This is deliberately role-private: it provides only BE-to-BE data-plane
//! calls and the BE-to-FE membership announce.  It shares no transport facade
//! with Frontend or Core.

use std::io;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_native_trust::{NativeClientAuthInterceptor, NativeTrust};
use novarocks_proto_codec::membership::BackendAnnounceResult;
use novarocks_proto_models::{filter, novarocks as proto};
use novarocks_types::NativeEndpoint;
use novarocks_types::identity::UniqueId;
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tower::service_fn;

use super::runtime::BackendDataRuntime;
use super::transport::nova_rocks_grpc_client::NovaRocksGrpcClient;

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

type AuthenticatedNovaRocksGrpcClient =
    NovaRocksGrpcClient<InterceptedService<Channel, NativeClientAuthInterceptor>>;

pub(crate) struct BackendRpcClient {
    runtime: BackendDataRuntime,
    endpoint: NativeEndpoint,
}

impl BackendRpcClient {
    pub(crate) fn new_native_endpoint(
        runtime: BackendDataRuntime,
        endpoint: NativeEndpoint,
    ) -> Self {
        Self { runtime, endpoint }
    }

    pub(crate) fn new_runtime_endpoint(
        runtime: BackendDataRuntime,
        endpoint: &RuntimeEndpoint,
    ) -> Result<Self, String> {
        Ok(Self {
            runtime,
            endpoint: endpoint.native_endpoint().clone(),
        })
    }

    pub(crate) fn new_host_port(
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

    fn make_client(&self) -> Result<AuthenticatedNovaRocksGrpcClient, String> {
        let endpoint = self.endpoint.clone();
        let runtime = self.runtime.clone();
        let channel_runtime = runtime.clone();
        let channel = runtime
            .clone()
            .block_on(async move { get_or_create_channel(&channel_runtime, endpoint).await })?;
        Ok(client_from_channel(
            channel,
            runtime.native_trust().as_ref(),
        ))
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

    pub(crate) async fn transmit_runtime_filter_envelope_async(
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

    pub(crate) fn blocking_announce_backend_with_timeout(
        &self,
        request: proto::AnnounceBackendRequest,
        timeout: Duration,
    ) -> Result<BackendAnnounceResult, String> {
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
            BackendAnnounceResult::from_proto(response)
                .map_err(|error| format!("announce_backend response invalid: {error}"))
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The frozen native boundary keeps independently validated inputs explicit."
    )]
    pub(crate) fn exchange_unary(
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
    ) -> Result<(), String> {
        let mut client = self.make_client()?;
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
        self.runtime.block_on(async move {
            let response = client
                .exchange_unary(request)
                .await
                .map_err(|error| format!("exchange rpc failed: {error}"))?
                .into_inner();
            if let Some(status) = response.status.as_ref()
                && status.code != 0
            {
                return Err(if status.message.is_empty() {
                    format!("exchange rpc returned status_code={}", status.code)
                } else {
                    format!("exchange rpc failed: {}", status.message)
                });
            }
            Ok(())
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
    use super::channel_endpoint;

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
