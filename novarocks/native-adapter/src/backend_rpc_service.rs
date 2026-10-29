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

//! Native Backend gRPC service and its domain handlers.
//!
//! The generic generated listener, authenticated transport, and its lifecycle
//! are owned by `novarocks-native-adapter`; this role owns its service facts.

use std::sync::Arc;
use std::time::Instant;

use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use tokio_stream::wrappers::ReceiverStream;

use crate::native_ingress::NativeIngressOwnership;
use crate::{
    backend_heartbeat::BackendHeartbeatResponder,
    catalog_prune_rpc::{CatalogReachabilityAuthority, handle_prune_catalogs},
    exchange_data_plane::{NativeExchangeDataPlane, TaskInboundCapabilitiesRouteAuthority},
    generated::nova_rocks_grpc_server::NovaRocksGrpc,
    runtime_filter_rpc::{BackendRuntimeFilterEnvelopeIngress, handle_runtime_filter_envelope},
    task_protocol::{TaskExecutionIngress, TaskIngressTiming, TaskStatusEventStream},
};
use novarocks_worker::TaskInboundCapabilities;

/// Backend-owned production Tonic service. Domain owners contribute the narrow
/// ingress ports while this service composes their role-local wire adapters.
#[derive(Clone)]
pub struct BackendRpcService {
    task_execution_ingress: Arc<dyn TaskExecutionIngress>,
    catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
    heartbeat: BackendHeartbeatResponder,
    exchange_data_plane: NativeExchangeDataPlane,
    runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
}

impl BackendRpcService {
    pub fn new(
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
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
        }
    }
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        self.test_controls = Some(controls);
        self
    }
}

fn ingress_ownership<T>(request: &tonic::Request<T>) -> Option<Arc<NativeIngressOwnership>> {
    request
        .extensions()
        .get::<Arc<NativeIngressOwnership>>()
        .cloned()
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
                let queued_at = Instant::now();
                let response = match tokio::task::spawn_blocking(move || {
                    crate::backend_metrics::native_blocking_queue_wait(
                        "exchange_stream",
                        queued_at.elapsed(),
                    );
                    kernel.transmit(request)
                })
                .await
                {
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
        let ownership = ingress_ownership(&request);
        let raw = request.into_inner();
        let queued_at = Instant::now();
        let response = tokio::task::spawn_blocking(move || {
            crate::backend_metrics::native_blocking_queue_wait(
                "exchange_unary",
                queued_at.elapsed(),
            );
            let _ownership = ownership;
            kernel.transmit(raw)
        })
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
        let ownership = ingress_ownership(&request);
        let raw = request.into_inner();
        let queued_at = Instant::now();
        let response = tokio::task::spawn_blocking(move || {
            crate::backend_metrics::native_blocking_queue_wait(
                "runtime_filter",
                queued_at.elapsed(),
            );
            let _ownership = ownership;
            handle_runtime_filter_envelope(ingress, raw)
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
        let ownership = ingress_ownership(&request);
        let raw = request.into_inner();
        let queued_at = Instant::now();
        let response = tokio::task::spawn_blocking(move || {
            crate::backend_metrics::native_blocking_queue_wait(
                "prune_catalogs",
                queued_at.elapsed(),
            );
            let _ownership = ownership;
            handle_prune_catalogs(authority.as_ref(), raw)
        })
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
        let heartbeat = self.heartbeat.clone();
        let ownership = ingress_ownership(&request);
        let raw = request.into_inner();
        let queued_at = Instant::now();
        let response = tokio::task::spawn_blocking(move || {
            crate::backend_metrics::native_blocking_queue_wait("heartbeat", queued_at.elapsed());
            let _ownership = ownership;
            heartbeat.respond(raw)
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("heartbeat handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn apply_task_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let ownership = ingress_ownership(&request).ok_or_else(|| {
            tonic::Status::internal("task operation request is missing native ingress ownership")
        })?;
        #[cfg(novarocks_dev_controls)]
        let hold_token = if let Some(controls) = self.test_controls.as_ref() {
            crate::task_protocol_fault::claim_native_ingress_hold(controls, &request)?
        } else {
            None
        };
        let timing = TaskIngressTiming::new(ownership.arrival(), ownership.deadline());
        let raw = request.into_inner();
        #[cfg(novarocks_dev_controls)]
        let registry_hold_token = if let Some(controls) = self.test_controls.as_ref() {
            crate::task_protocol_fault::claim_native_registry_hold(controls, &raw)?
        } else {
            None
        };
        #[cfg(novarocks_dev_controls)]
        if hold_token.is_some() || registry_hold_token.is_some() {
            return Err(tonic::Status::unimplemented(
                "synchronous Native ingress/registry lock holds are retired; async owner hold injection is not yet connected",
            ));
        }
        // This exact admitted Native owner remains live while the original
        // Worker request is awaited. No blocking executor owns a second call.
        let _ownership = ownership;
        let response = ingress.apply_task_operations_at(raw, timing).await?;
        Ok(tonic::Response::new(response))
    }

    async fn apply_task_control_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskControlOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let ownership = ingress_ownership(&request).ok_or_else(|| {
            tonic::Status::internal("task control request is missing native ingress ownership")
        })?;
        let timing = TaskIngressTiming::new(ownership.arrival(), ownership.deadline());
        let raw = request.into_inner();
        let _ownership = ownership;
        let response = ingress
            .apply_task_control_operations_at(raw, timing)
            .await?;
        Ok(tonic::Response::new(response))
    }

    async fn subscribe_task_status(
        &self,
        request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let ownership = ingress_ownership(&request).ok_or_else(|| {
            tonic::Status::internal(
                "status subscription is missing original native ingress ownership",
            )
        })?;
        let deadline = ownership.deadline();
        let raw = request.into_inner();
        let stream = ingress.subscribe_task_status(raw, deadline, ownership)?;
        Ok(tonic::Response::new(stream))
    }

    async fn fetch_task_dynamic_filters(
        &self,
        request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        let ownership = ingress_ownership(&request).ok_or_else(|| {
            tonic::Status::internal(
                "fetch_task_dynamic_filters request is missing original native ingress ownership",
            )
        })?;
        let deadline = ownership.deadline();
        let raw = request.into_inner();
        let _ownership = ownership;
        let response = self
            .task_execution_ingress
            .fetch_task_dynamic_filters(raw, deadline)
            .await?;
        Ok(tonic::Response::new(response))
    }

    async fn get_final_task_info(
        &self,
        request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        let ownership = ingress_ownership(&request).ok_or_else(|| {
            tonic::Status::internal(
                "get_final_task_info request is missing original native ingress ownership",
            )
        })?;
        let deadline = ownership.deadline();
        let raw = request.into_inner();
        let _ownership = ownership;
        let response = self
            .task_execution_ingress
            .get_final_task_info(raw, deadline)
            .await?;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_task_result(
        &self,
        request: tonic::Request<proto::FetchTaskResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        let ownership = ingress_ownership(&request);
        let response = self
            .task_execution_ingress
            .fetch_task_result_with_ownership(request.into_inner(), ownership)
            .await?;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_root_result(
        &self,
        _request: tonic::Request<proto::FetchRootResultRequest>,
    ) -> Result<tonic::Response<proto::FetchRootResultResponse>, tonic::Status> {
        // The bounded root reader serves this method ahead of the generated
        // service, keeping each response body's owner until it exits.
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "FetchRootResult is served by the bounded root reader route",
        ))
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
