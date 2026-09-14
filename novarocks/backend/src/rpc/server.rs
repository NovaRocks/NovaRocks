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

//! Production Backend gRPC service and its domain handlers.
//!
//! The generic generated listener, authenticated transport, and its lifecycle
//! are owned by `novarocks-native-adapter`; this role owns its service facts.

use std::sync::Arc;

use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use tokio_stream::wrappers::ReceiverStream;

use novarocks_native_adapter::{
    backend_heartbeat::BackendHeartbeatResponder,
    catalog_prune_rpc::{CatalogReachabilityAuthority, handle_prune_catalogs},
    exchange_data_plane::{NativeExchangeDataPlane, TaskInboundCapabilitiesRouteAuthority},
    generated::nova_rocks_grpc_server::NovaRocksGrpc,
    runtime_filter_rpc::{BackendRuntimeFilterEnvelopeIngress, handle_runtime_filter_envelope},
    task_protocol::{TaskExecutionIngress, TaskStatusEventStream},
};
use novarocks_worker::TaskInboundCapabilities;

/// Backend-owned production Tonic service. Domain owners contribute the narrow
/// ingress ports while this service composes their role-local wire adapters.
#[derive(Clone)]
pub(crate) struct BackendRpcService {
    task_execution_ingress: Arc<dyn TaskExecutionIngress>,
    catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
    heartbeat: BackendHeartbeatResponder,
    exchange_data_plane: NativeExchangeDataPlane,
    runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
}

impl BackendRpcService {
    pub(crate) fn new(
        task_execution_ingress: Arc<dyn TaskExecutionIngress>,
        catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
        runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        task_inbound_capabilities: Arc<TaskInboundCapabilities>,
        heartbeat: BackendHeartbeatResponder,
    ) -> Self {
        Self {
            task_execution_ingress,
            catalog_reachability,
            heartbeat,
            exchange_data_plane: NativeExchangeDataPlane::new(
                exchange_receiver_port,
                vec![Arc::new(TaskInboundCapabilitiesRouteAuthority::new(
                    task_inbound_capabilities,
                ))],
            ),
            runtime_filter_ingress,
        }
    }
}

fn retired_fetch_result_status() -> tonic::Status {
    tonic::Status::unimplemented("FetchResult is retired; use FetchTaskResult")
}

#[tonic::async_trait]
impl NovaRocksGrpc for BackendRpcService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;
    type SubscribeTaskStatusStream = TaskStatusEventStream;

    async fn announce_backend(
        &self,
        _request: tonic::Request<proto::AnnounceBackendRequest>,
    ) -> Result<tonic::Response<proto::AnnounceBackendResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "backend announce is accepted only by the frontend native ingress",
        ))
    }

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let kernel = self.exchange_data_plane.clone();
        tokio::spawn(async move {
            loop {
                let request = match inbound.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange recv failed: {error}"
                            ))))
                            .await;
                        break;
                    }
                };
                let kernel = kernel.clone();
                let response =
                    match tokio::task::spawn_blocking(move || kernel.transmit(request)).await {
                        Ok(response) => response,
                        Err(error) => {
                            let _ = tx
                                .send(Err(tonic::Status::internal(format!(
                                    "exchange handler panicked: {error}"
                                ))))
                                .await;
                            break;
                        }
                    };
                let failed = response
                    .status
                    .as_ref()
                    .is_some_and(|status| status.code != 0);
                if tx.send(Ok(response)).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        let kernel = self.exchange_data_plane.clone();
        let response = tokio::task::spawn_blocking(move || kernel.transmit(request.into_inner()))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("exchange_unary handler panicked: {error}"))
            })?;
        Ok(tonic::Response::new(response))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.runtime_filter_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_runtime_filter_envelope(ingress, request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!(
                "transmit_runtime_filter_envelope handler panicked: {error}"
            ))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_result(
        &self,
        _request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        Err(retired_fetch_result_status())
    }

    async fn prune_catalogs(
        &self,
        request: tonic::Request<catalog::PruneCatalogsRequest>,
    ) -> Result<tonic::Response<catalog::PruneCatalogsResponse>, tonic::Status> {
        let authority = Arc::clone(&self.catalog_reachability);
        let raw = request.into_inner();
        let response =
            tokio::task::spawn_blocking(move || handle_prune_catalogs(authority.as_ref(), raw))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!("prune_catalogs handler panicked: {error}"))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        self.heartbeat
            .respond(request.into_inner())
            .map(tonic::Response::new)
    }

    async fn apply_task_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response = tokio::task::spawn_blocking(move || {
            ingress.apply_task_operations(request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("apply_task_operations handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn subscribe_task_status(
        &self,
        request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        // Opening a subscription only registers a cursor, so it does not need
        // the blocking pool the mutation path uses.
        let stream = self
            .task_execution_ingress
            .subscribe_task_status(request.into_inner())?;
        Ok(tonic::Response::new(stream))
    }

    async fn fetch_task_dynamic_filters(
        &self,
        request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response = tokio::task::spawn_blocking(move || {
            ingress.fetch_task_dynamic_filters(request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!(
                "fetch_task_dynamic_filters handler panicked: {error}"
            ))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn get_final_task_info(
        &self,
        request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response =
            tokio::task::spawn_blocking(move || ingress.get_final_task_info(request.into_inner()))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!(
                        "get_final_task_info handler panicked: {error}"
                    ))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_task_result(
        &self,
        request: tonic::Request<proto::FetchTaskResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        let response = self
            .task_execution_ingress
            .fetch_task_result(request.into_inner())
            .await?;
        Ok(tonic::Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::retired_fetch_result_status;

    #[test]
    fn legacy_fragment_result_fetch_is_rejected_before_buffer_lookup() {
        let status = retired_fetch_result_status();
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert_eq!(
            status.message(),
            "FetchResult is retired; use FetchTaskResult"
        );
    }
}
