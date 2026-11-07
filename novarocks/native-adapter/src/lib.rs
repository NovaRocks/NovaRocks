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

#![cfg_attr(
    test,
    expect(
        clippy::disallowed_methods,
        reason = "Unit tests exercise thread/task mechanics under their original test owners; the production library is linted separately; see ADR-0218."
    )
)]

//! Server-resolved Native transport capabilities and protocol adapters.

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        env!("CARGO_PKG_NAME"),
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
        ],
    )
}

extern crate self as novarocks_native_adapter;

pub mod backend_announce;
pub mod backend_application;
pub mod backend_heartbeat;
pub mod backend_metrics;
pub mod backend_readiness;
pub mod backend_rpc_service;
pub mod backend_task_execution;
#[cfg(any(test, novarocks_dev_controls))]
pub mod backend_test_support;
pub mod catalog_prune_rpc;
pub mod compiled_runtime_filter;
pub mod compiled_scan_binding;
pub mod compiled_writer_binding;
pub mod connector_write_data_plane;
#[cfg(any(test, novarocks_dev_controls))]
pub mod connector_write_test_support;
pub mod descriptor_snapshot;
pub mod exchange_data_plane;
mod exchange_grant_subscription;
pub mod exchange_transmitter;
pub mod management_http;
mod native_channel_cache;
mod native_channel_identity;
pub mod native_client;
mod native_connection_key_capacity;
pub mod native_fd_capacity;
mod native_incoming_key_capacity;
pub mod native_lane;
pub mod root_cow_selection_codec;
mod root_producer_pool;
pub mod root_record_assembly;
pub mod root_result_reader;
pub mod root_result_session;
pub mod root_result_unary;
pub mod root_scalar_container_codec;
pub mod root_scalar_leaf_codec;
pub mod root_statistics_codec;
pub mod root_write_commit_codec;
pub mod task_instance;
pub mod task_result_writer;
pub mod task_submission;
pub use native_client::NativeRpcClient;
pub mod native_codec;
pub mod native_ingress;
pub mod native_query_context;
#[cfg(test)]
mod native_query_context_tests;
pub mod native_server;
pub mod native_transport_admission;
pub mod native_transport_geometry;
pub(crate) mod native_type_construction;
pub(crate) mod native_type_decode;
pub use native_server::NativeRpcListener;
pub mod established_query_options;
pub mod query_options;
pub mod runtime_filter_feedback;
pub mod runtime_filter_ingress;
pub mod runtime_filter_install;
pub mod runtime_filter_membership;
pub mod runtime_filter_participant;
pub mod runtime_filter_rpc;
pub mod runtime_filter_terminal;
#[cfg(any(test, novarocks_dev_controls))]
pub mod runtime_filter_test_support;
pub mod runtime_filter_transport;
pub mod runtime_filter_typed_scan;
pub mod static_package_admission;
pub mod task_execution_observation;
pub mod task_protocol;
#[cfg(novarocks_dev_controls)]
pub mod task_protocol_fault;
pub mod task_result_diagnostics;
pub mod task_shared_facts;
#[cfg(any(test, novarocks_dev_controls))]
pub mod typed_connector_test_support;
pub mod worker_node_ingress;
mod worker_node_observation;
mod worker_node_projection;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use novarocks_native_trust::{
    AutomaticTlsMaterial, NativeEndpointConnector, NativeIncomingAdapter, NativeTlsMaterial,
    NativeTrust,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::NativeEndpoint;
use tokio::runtime::Handle;

/// Largest root-result payload admitted by the Native task wire.
pub const FRONTEND_NATIVE_ROOT_RESULT_PAYLOAD_LIMIT_BYTES: u64 =
    novarocks_task_codec::operation::MAX_NATIVE_ROOT_RESULT_PAYLOAD_BYTES;

#[derive(Clone, Debug)]
pub enum BackendNativeTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl BackendNativeTransport {
    pub fn connector_for(
        &self,
        endpoint: NativeEndpoint,
    ) -> Result<NativeEndpointConnector, String> {
        match self {
            Self::Plaintext => Ok(NativeEndpointConnector::plaintext(endpoint)),
            Self::Automatic(material) => NativeEndpointConnector::automatic(endpoint, material)
                .map_err(|error| format!("construct automatic native connector: {error}")),
            Self::Pem(material) => Ok(NativeEndpointConnector::pem(endpoint, material)),
        }
    }
    pub fn incoming_adapter(&self) -> NativeIncomingAdapter {
        match self {
            Self::Plaintext => NativeIncomingAdapter::plaintext(),
            Self::Automatic(material) => NativeIncomingAdapter::automatic(material),
            Self::Pem(material) => NativeIncomingAdapter::pem(material),
        }
    }
}

/// Server-materialized transport capability consumed by the Frontend role.
///
/// It contains no source configuration or filesystem path. The Server builds
/// it before role startup and Frontend uses it for every Native dial and the
/// report listener's incoming stream.
#[derive(Clone)]
pub enum FrontendNativeTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl FrontendNativeTransport {
    pub fn plaintext() -> Self {
        Self::Plaintext
    }

    pub fn automatic(material: AutomaticTlsMaterial) -> Self {
        Self::Automatic(material)
    }

    pub fn pem(material: NativeTlsMaterial) -> Self {
        Self::Pem(material)
    }

    /// Borrow the actual selected endpoint and this original frozen TLS policy.
    /// Tonic's dummy HTTP origin and later DNS addresses are not domain authors.
    /// This pure projection grants no connection, stream or dispatch capacity.
    pub fn selected_network_origin<'a>(
        &self,
        endpoint: &'a NativeEndpoint,
    ) -> Result<
        novarocks_resource_contract::SelectedNetworkOrigin<'a>,
        novarocks_resource_contract::DependencyFactError,
    > {
        let mode = match self {
            Self::Plaintext => novarocks_native_trust::NativeTransportMode::Disabled,
            Self::Automatic(_) => novarocks_native_trust::NativeTransportMode::Automatic,
            Self::Pem(_) => novarocks_native_trust::NativeTransportMode::Pem,
        };
        NativeEndpointConnector::selected_origin_for(endpoint, mode)
    }

    pub fn connector_for(
        &self,
        endpoint: NativeEndpoint,
    ) -> Result<NativeEndpointConnector, String> {
        match self {
            Self::Plaintext => Ok(NativeEndpointConnector::plaintext(endpoint)),
            Self::Automatic(material) => NativeEndpointConnector::automatic(endpoint, material)
                .map_err(|error| {
                    format!("construct automatic Native TLS connector failed: {error}")
                }),
            Self::Pem(material) => Ok(NativeEndpointConnector::pem(endpoint, material)),
        }
    }

    pub fn incoming_adapter(&self) -> NativeIncomingAdapter {
        match self {
            Self::Plaintext => NativeIncomingAdapter::plaintext(),
            Self::Automatic(material) => NativeIncomingAdapter::automatic(material),
            Self::Pem(material) => NativeIncomingAdapter::pem(material),
        }
    }
}

/// Server-frozen deployment limits for the Frontend Native Task transport.
///
/// The adapter alone materializes the task-codec budget consumed by its wire
/// ports. Frontend receives the validated value and never owns a second
/// codec-budget conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendTaskTransportBudget(TransportBudget);

impl FrontendTaskTransportBudget {
    pub const DEFAULT: Self = Self(TransportBudget::DEFAULT);

    pub fn try_new(
        max_batch_items: usize,
        max_batch_encoded_bytes: usize,
        max_descriptor_encoded_bytes: usize,
        max_query_backend_queued_operations: usize,
        max_query_backend_queued_bytes: usize,
        max_backend_queued_operations: usize,
        max_backend_queued_bytes: usize,
        max_tasks_per_participation: usize,
        max_active_tasks_per_backend: usize,
        frontend_queue_residence: Duration,
    ) -> Option<Self> {
        TransportBudget::new(
            max_batch_items,
            max_batch_encoded_bytes,
            max_descriptor_encoded_bytes,
            max_query_backend_queued_operations,
            max_query_backend_queued_bytes,
            max_backend_queued_operations,
            max_backend_queued_bytes,
            max_tasks_per_participation,
            max_active_tasks_per_backend,
            frontend_queue_residence,
        )
        .map(Self)
    }

    pub const fn max_batch_items(self) -> usize {
        self.0.max_batch_items()
    }

    pub const fn max_batch_encoded_bytes(self) -> usize {
        self.0.max_batch_encoded_bytes()
    }

    pub const fn max_descriptor_encoded_bytes(self) -> usize {
        self.0.max_descriptor_encoded_bytes()
    }

    pub const fn max_query_backend_queued_operations(self) -> usize {
        self.0.max_query_backend_queued_operations()
    }

    pub const fn max_query_backend_queued_bytes(self) -> usize {
        self.0.max_query_backend_queued_bytes()
    }

    pub const fn max_backend_queued_operations(self) -> usize {
        self.0.max_backend_queued_operations()
    }

    pub const fn max_backend_queued_bytes(self) -> usize {
        self.0.max_backend_queued_bytes()
    }

    pub const fn max_tasks_per_participation(self) -> usize {
        self.0.max_tasks_per_participation()
    }

    pub const fn max_active_tasks_per_backend(self) -> usize {
        self.0.max_active_tasks_per_backend()
    }

    pub const fn frontend_queue_residence(self) -> Duration {
        self.0.frontend_queue_residence()
    }

    pub const fn into_codec(self) -> TransportBudget {
        self.0
    }
}

#[derive(Clone)]
pub struct BackendDataRuntime {
    handle: Handle,
    native_trust: Arc<NativeTrust>,
    native_transport: BackendNativeTransport,
    channels: native_channel_cache::NativeChannelCache,
    transport_admission: Option<native_transport_admission::NativeTransportAdmission>,
}

impl BackendDataRuntime {
    pub fn new(
        handle: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: BackendNativeTransport,
    ) -> Self {
        Self {
            handle,
            native_trust,
            native_transport,
            channels: native_channel_cache::NativeChannelCache::bounded()
                .expect("frozen Native channel-cache geometry is finite"),
            transport_admission: None,
        }
    }
    /// A BE host gets its own channel generation over the process admission.
    /// Channels dialed before admission was installed are never reused.
    pub(crate) fn with_transport_admission(
        &self,
        admission: native_transport_admission::NativeTransportAdmission,
    ) -> std::io::Result<Self> {
        Ok(Self {
            handle: self.handle.clone(),
            native_trust: Arc::clone(&self.native_trust),
            native_transport: self.native_transport.clone(),
            channels: native_channel_cache::NativeChannelCache::bounded()?,
            transport_admission: Some(admission),
        })
    }

    pub(crate) fn transport_admission(
        &self,
    ) -> Option<&native_transport_admission::NativeTransportAdmission> {
        self.transport_admission.as_ref()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "Runs the original sender ExitObligation and wakes its Worker owner after actual drop; see ADR-0218."
    )]
    pub(crate) fn spawn_with_obligation<F>(
        &self,
        work: novarocks_worker::RuntimeFilterSenderWork,
        future: F,
    ) -> tokio::task::JoinHandle<
        Result<F::Output, novarocks_control_runtime::exit_work::ExitWorkError>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (obligation, panic, wake) = work.into_parts();
        let future = novarocks_control_runtime::exit_work::run_future(obligation, future, panic);
        self.handle.spawn(ParticipationExitWakeFuture {
            future: Box::pin(future),
            _wake: ParticipationExitWake(wake),
        })
    }

    pub(crate) fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.handle.block_on(future))
        } else {
            self.handle.block_on(future)
        }
    }
    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }
    pub fn native_trust(&self) -> &Arc<NativeTrust> {
        &self.native_trust
    }
    pub fn native_transport(&self) -> &BackendNativeTransport {
        &self.native_transport
    }
    pub(crate) fn channels(&self) -> &native_channel_cache::NativeChannelCache {
        &self.channels
    }
}

struct ParticipationExitWake(novarocks_worker::ParticipationWorkWake);
impl Drop for ParticipationExitWake {
    fn drop(&mut self) {
        self.0.notify();
    }
}
struct ParticipationExitWakeFuture<F> {
    // Rust destroys the actual obligated future before the wake, even when
    // that destruction unwinds and records a real Unresolved work.
    future: std::pin::Pin<Box<F>>,
    _wake: ParticipationExitWake,
}
impl<F: Future> Future for ParticipationExitWakeFuture<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::FrontendTaskTransportBudget;

    #[test]
    fn frontend_task_transport_budget_preserves_the_validated_codec_limits() {
        let budget = FrontendTaskTransportBudget::try_new(
            2,
            1024,
            512,
            4,
            4096,
            4,
            4096,
            1,
            2,
            Duration::from_secs(1),
        )
        .expect("valid native transport budget");

        assert_eq!(budget.max_batch_items(), 2);
        assert_eq!(budget.max_batch_encoded_bytes(), 1024);
        assert_eq!(budget.max_descriptor_encoded_bytes(), 512);
        assert_eq!(budget.max_query_backend_queued_operations(), 4);
        assert_eq!(budget.max_query_backend_queued_bytes(), 4096);
        assert_eq!(budget.max_backend_queued_operations(), 4);
        assert_eq!(budget.max_backend_queued_bytes(), 4096);
        assert_eq!(budget.max_tasks_per_participation(), 1);
        assert_eq!(budget.max_active_tasks_per_backend(), 2);
        assert_eq!(budget.frontend_queue_residence(), Duration::from_secs(1));
        assert_eq!(budget.into_codec().max_batch_items(), 2);
    }

    #[test]
    fn generated_native_stubs_reference_the_canonical_protocol_dtos() {
        let generated = include_str!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
        assert!(generated.contains("nova_rocks_grpc_client"));
        assert!(generated.contains("nova_rocks_grpc_server"));
        assert!(generated.contains("::novarocks_proto_models::novarocks::HeartbeatRequest"));
    }
}

#[cfg(novarocks_dev_controls)]
pub mod test_controls;

#[cfg(novarocks_dev_controls)]
pub mod test_observation_controls;

#[cfg(novarocks_dev_controls)]
pub mod backend_test_session;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");

#[cfg(test)]
mod frontend_selected_origin_tests {
    use super::FrontendNativeTransport;
    use novarocks_native_trust::{
        AutomaticTlsMaterial, DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust,
        ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_types::NativeEndpoint;

    #[test]
    fn frozen_frontend_policy_projects_the_same_origin_as_its_actual_connector_without_io() {
        let endpoint = NativeEndpoint::from_host_port("SELECTED.example", 443).unwrap();
        let trust = NativeTrust::new(
            DeploymentId::parse("frontend-origin-pure").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            NativeCallerSubject::parse("fe@localhost:19040").unwrap(),
            NativeTransportMode::Automatic,
        );
        let material = AutomaticTlsMaterial::for_endpoint(trust, endpoint.clone()).unwrap();
        for (transport, expected) in [
            (
                FrontendNativeTransport::plaintext(),
                "http://selected.example:443",
            ),
            (
                FrontendNativeTransport::automatic(material),
                "https://selected.example",
            ),
        ] {
            let projected = transport
                .selected_network_origin(&endpoint)
                .unwrap()
                .into_owned();
            let connector = transport.connector_for(endpoint.clone()).unwrap();
            assert_eq!(projected.as_str(), expected);
            assert_eq!(
                projected,
                connector.selected_network_origin().unwrap().into_owned()
            );
        }
    }
}

#[cfg(test)]
mod root_unknown_test;

#[cfg(novarocks_dev_controls)]
mod join_build_test_control;
