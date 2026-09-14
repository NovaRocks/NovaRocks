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

//! Server-resolved Native transport capabilities and protocol adapters.

extern crate self as novarocks_native_adapter;

pub mod backend_announce;
pub mod backend_application;
pub mod backend_heartbeat;
pub mod backend_metrics;
pub mod backend_readiness;
pub mod backend_rpc_service;
pub mod backend_task_execution;
#[cfg(any(test, feature = "test-support"))]
pub mod backend_test_support;
pub mod catalog_prune_rpc;
pub mod connector_blocking_io;
pub mod connector_write_data_plane;
#[cfg(any(test, feature = "test-support"))]
pub mod connector_write_test_support;
pub mod debug_environment;
pub mod descriptor_snapshot;
pub mod exchange_data_plane;
pub mod exchange_transmitter;
pub mod fragment_aggregate;
pub mod fragment_decode_context;
pub mod fragment_error;
pub mod fragment_exchange_receiver;
pub mod fragment_expression;
pub mod fragment_hash_join;
pub mod fragment_ingress_error;
pub mod fragment_instance;
pub mod fragment_layout;
pub mod fragment_plan_decode;
pub(crate) mod fragment_plan_decode_submission;
pub mod fragment_plan_node;
pub mod fragment_request;
pub mod fragment_result_writer;
pub mod fragment_runtime_filter;
pub mod fragment_runtime_filter_binding;
pub mod fragment_scan_decode;
pub mod fragment_scan_output;
pub mod fragment_sink;
pub mod fragment_submission;
pub mod fragment_typed_connector_scan;
pub mod fragment_validation;
pub mod fragment_variant_path;
pub mod fragment_window;
pub mod management_http;
pub mod native_client;
pub use native_client::NativeRpcClient;
pub mod native_codec;
pub mod native_fragment_query;
#[cfg(test)]
mod native_fragment_query_tests;
pub mod native_server;
pub use native_server::NativeRpcServerHandle;
pub mod query_options;
pub mod runtime_filter_feedback;
pub mod runtime_filter_ingress;
pub mod runtime_filter_install;
pub mod runtime_filter_membership;
pub mod runtime_filter_participant;
pub mod runtime_filter_rpc;
pub mod runtime_filter_terminal;
#[cfg(any(test, feature = "test-support"))]
pub mod runtime_filter_test_support;
pub mod runtime_filter_transport;
pub mod runtime_filter_typed_scan;
pub mod task_execution_observation;
pub mod task_protocol;
pub mod task_protocol_fault;
pub mod task_protocol_ingress;
pub mod task_query_context_options;
pub mod task_result_diagnostics;
pub mod task_shared_facts;
#[cfg(any(test, feature = "test-support"))]
pub mod typed_connector_test_support;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_native_trust::{
    AutomaticTlsMaterial, NativeEndpointConnector, NativeIncomingAdapter, NativeTlsMaterial,
    NativeTrust,
};
use novarocks_task_codec::{TransportBudget, domain::ConfidentialTransport};
use novarocks_types::NativeEndpoint;
use tokio::runtime::Handle;
use tonic::transport::Channel;

/// Largest root-result payload admitted by the Native task wire.
pub const FRONTEND_NATIVE_ROOT_RESULT_PAYLOAD_LIMIT_BYTES: u64 =
    novarocks_task_codec::operation::MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES;

#[derive(Clone, Debug)]
pub enum BackendNativeTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl BackendNativeTransport {
    pub const fn confidentiality(&self) -> ConfidentialTransport {
        match self {
            Self::Plaintext => ConfidentialTransport::Plaintext,
            Self::Automatic(_) | Self::Pem(_) => ConfidentialTransport::Confidential,
        }
    }
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

    /// Whether this concrete role-local Native transport encrypts the wire.
    /// Confidential query-attempt lease material is admitted only through this
    /// capability, never through an untrusted protobuf claim.
    pub const fn permits_confidential_credential_leases(&self) -> bool {
        matches!(self, Self::Automatic(_) | Self::Pem(_))
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

    #[expect(
        clippy::too_many_arguments,
        reason = "each deployment limit is independently configurable"
    )]
    pub fn try_new(
        max_batch_items: usize,
        max_batch_encoded_bytes: usize,
        max_descriptor_encoded_bytes: usize,
        max_query_backend_queued_operations: usize,
        max_query_backend_queued_bytes: usize,
        max_backend_queued_operations: usize,
        max_backend_queued_bytes: usize,
        max_tasks_per_context: usize,
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
            max_tasks_per_context,
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

    pub const fn max_tasks_per_context(self) -> usize {
        self.0.max_tasks_per_context()
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
    channels: Arc<Mutex<HashMap<NativeEndpoint, Channel>>>,
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
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn block_on<F>(&self, future: F) -> F::Output
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
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
    pub fn native_trust(&self) -> &Arc<NativeTrust> {
        &self.native_trust
    }
    pub fn native_transport(&self) -> &BackendNativeTransport {
        &self.native_transport
    }
    pub fn channels(&self) -> &Arc<Mutex<HashMap<NativeEndpoint, Channel>>> {
        &self.channels
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
        assert_eq!(budget.max_tasks_per_context(), 1);
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
