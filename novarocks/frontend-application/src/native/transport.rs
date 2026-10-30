//! Narrow FE-to-BE native transport adapters.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tonic::Request;
use tonic::service::interceptor::InterceptedService;

use crate::metrics::observe_backend_heartbeat_rtt;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution_contract::BackendProcessDescriptor as FrozenBackendDescriptor;
use novarocks_native_trust::NativeClientAuthInterceptor;
use novarocks_proto_codec::catalog::{
    PruneCatalogsOutcome, PruneCatalogsRequest, PruneCatalogsResponse,
};
use novarocks_proto_codec::membership::{
    BackendProcessDescriptor, BackendProcessId as ProtocolBackendProcessId, parse_reported_state,
};
use novarocks_proto_codec::native_rpc::{
    NativeEndpointDomain, NativeRpcMethod, NativeTrafficClass,
};
use novarocks_query_application::api::HeartbeatOutcome;
use novarocks_types::{BackendProcessId, NativeEndpoint};

use super::data_runtime::{CachedNativeChannel, FrontendDataRuntime, NativeChannelKey};
use novarocks_native_adapter::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
use novarocks_native_adapter::native_lane::{
    NativeLane, NativeLaneChannel, configure_native_endpoint, frontend_lane_connector,
};

mod cold_dial;
pub(super) use cold_dial::{NativeChannelSource, NativeDialAttempt, NativeDialHandoff};

const MAX_MESSAGE_BYTES: usize =
    novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;

/// One best-effort response from a Backend catalog reachability prune.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogPruneDispatchOutcome {
    Accepted,
    Rejected { safe_detail: String },
}

/// Sends one already-validated complete reachable-catalog snapshot to a live
/// Backend. This has no query lifecycle side effect: callers record failure
/// and retry on a later periodic round.
pub(crate) fn prune_catalogs(
    data_runtime: &FrontendDataRuntime,
    endpoint: RuntimeEndpoint,
    peer: BackendProcessId,
    request: &PruneCatalogsRequest,
    timeout: Duration,
) -> Result<CatalogPruneDispatchOutcome, String> {
    let client = Client::for_endpoint(
        endpoint.native_endpoint().clone(),
        NativeEndpointDomain::BackendData,
        peer,
        data_runtime.clone(),
    );
    let response = data_runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let mut grpc = client.grpc(NativeRpcMethod::PruneCatalogs).await?;
            grpc.prune_catalogs(Request::new(request.as_proto().clone()))
                .await
                .map(|response| response.into_inner())
                .map_err(|error| format!("prune_catalogs rpc failed: {error}"))
        })
        .await
        .map_err(|_| "prune_catalogs rpc deadline exceeded".to_string())?
    })??;
    match PruneCatalogsResponse::parse(response)
        .map_err(|error| format!("Backend returned an invalid PruneCatalogs response: {error}"))?
        .outcome()
    {
        PruneCatalogsOutcome::Accepted => Ok(CatalogPruneDispatchOutcome::Accepted),
        PruneCatalogsOutcome::Rejected { safe_detail } => {
            Ok(CatalogPruneDispatchOutcome::Rejected { safe_detail })
        }
    }
}

/// Every call takes one of its lane connection's stream positions before it
/// reaches Tonic, and the response body holds it until the body ends or is
/// dropped.
pub(super) type AuthenticatedNovaRocksGrpcClient =
    NovaRocksGrpcClient<InterceptedService<NativeLaneChannel, NativeClientAuthInterceptor>>;

/// The Root-only client keeps the original authenticated ResultData channel.
pub(super) type AuthenticatedRootResultGrpc =
    tonic::client::Grpc<InterceptedService<NativeLaneChannel, NativeClientAuthInterceptor>>;

/// A Native channel either fails before an outbound connection is attempted,
/// or while that connection is being established.  TaskUpdate must preserve
/// that distinction: the latter has an unknown remote outcome, while the
/// former cannot be repaired by resending an immutable request.
#[derive(Debug)]
pub(super) enum ChannelAcquisitionError {
    Fatal(String),
    RetryableNetwork(ChannelEstablishmentFailure),
}

impl ChannelAcquisitionError {
    pub(super) fn fatal(detail: impl Into<String>) -> Self {
        Self::Fatal(detail.into())
    }

    pub(super) fn retryable_network(detail: impl Into<String>) -> Self {
        Self::RetryableNetwork(ChannelEstablishmentFailure {
            detail: detail.into(),
            connection_lost: false,
        })
    }
}

impl std::fmt::Display for ChannelAcquisitionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fatal(detail) => formatter.write_str(detail),
            Self::RetryableNetwork(error) => formatter.write_str(&error.detail),
        }
    }
}

impl std::error::Error for ChannelAcquisitionError {}

/// Establishment may fail in a local admission/cache owner without observing a
/// broken backend connection. Keep that distinction before rendering text.
#[derive(Debug)]
pub(super) struct ChannelEstablishmentFailure {
    detail: String,
    connection_lost: bool,
}
impl ChannelAcquisitionError {
    pub(super) fn observed_connection_loss(&self) -> bool {
        matches!(self, Self::RetryableNetwork(error) if error.connection_lost)
    }
    fn failed_connection(error: tonic::transport::Error) -> Self {
        Self::RetryableNetwork(ChannelEstablishmentFailure {
            connection_lost: is_observed_connection_loss(&error),
            detail: format!("connect Native endpoint failed: {error}"),
        })
    }
}

/// Inspect original typed transport evidence only. A local timeout, a closed
/// admission gate, a TLS refusal or a source-less RPC status supplies no such
/// evidence. The bounded walk protects diagnostics from a pathological chain.
fn observed_dial_loss(failure: &novarocks_native_trust::NativeConnectFailure) -> bool {
    // DNS/configuration/TLS phases retain their own cause. In particular, an
    // EOF during certificate negotiation is not a failed Task process.
    match failure {
        novarocks_native_trust::NativeConnectFailure::Unreachable(error) => observed_io_loss(error),
        novarocks_native_trust::NativeConnectFailure::Resolution(_)
        | novarocks_native_trust::NativeConnectFailure::Handshake(_)
        | novarocks_native_trust::NativeConnectFailure::UnverifiableServerName => false,
    }
}
fn observed_io_loss(error: &std::io::Error) -> bool {
    // io::Error::source may skip the custom payload itself. Inspect that
    // original phase-bearing owner before following a child error.
    if let Some(failure) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<novarocks_native_trust::NativeConnectFailure>())
    {
        return match failure {
            novarocks_native_trust::NativeConnectFailure::Unreachable(error) => matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
            ),
            _ => false,
        };
    }
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::NotConnected
    )
}
pub(super) fn is_observed_connection_loss(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    for _ in 0..16 {
        let Some(error) = current else {
            break;
        };
        if let Some(failure) = error.downcast_ref::<novarocks_native_trust::NativeConnectFailure>()
        {
            return observed_dial_loss(failure);
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            if error
                .get_ref()
                .is_some_and(|source| source.is::<novarocks_native_trust::NativeConnectFailure>())
            {
                return observed_io_loss(error);
            }
            if observed_io_loss(error) {
                return true;
            }
        }
        if let Some(error) = error.downcast_ref::<h2::Error>() {
            // Locked h2 keeps IO in get_io rather than Error::source.
            if let Some(error) = error.get_io() {
                return observed_io_loss(error);
            }
            if error.is_remote()
                && error.is_go_away()
                && error.reason() == Some(h2::Reason::NO_ERROR)
            {
                return true;
            }
        }
        current = error.source();
    }
    false
}

#[derive(Clone)]
enum FrozenClientEndpoints {
    Backend(Box<FrozenBackendDescriptor>),
    Single {
        endpoint: NativeEndpoint,
        domain: NativeEndpointDomain,
        peer: BackendProcessId,
    },
}

#[derive(Clone)]
pub(super) struct Client {
    endpoints: FrozenClientEndpoints,
    data_runtime: FrontendDataRuntime,
}

impl Client {
    pub(super) fn for_backend(
        descriptor: FrozenBackendDescriptor,
        data_runtime: FrontendDataRuntime,
    ) -> Self {
        Self {
            endpoints: FrozenClientEndpoints::Backend(Box::new(descriptor)),
            data_runtime,
        }
    }

    pub(super) fn for_endpoint(
        endpoint: NativeEndpoint,
        domain: NativeEndpointDomain,
        peer: BackendProcessId,
        data_runtime: FrontendDataRuntime,
    ) -> Self {
        Self {
            endpoints: FrozenClientEndpoints::Single {
                endpoint,
                domain,
                peer,
            },
            data_runtime,
        }
    }

    fn endpoint_for(
        &self,
        method: NativeRpcMethod,
    ) -> Result<&NativeEndpoint, ChannelAcquisitionError> {
        let domain = method.contract().endpoint;
        if !method.is_allowed_at(domain) {
            return Err(ChannelAcquisitionError::fatal(
                "retired Native method cannot acquire a channel",
            ));
        }
        match &self.endpoints {
            FrozenClientEndpoints::Backend(descriptor) => match domain {
                NativeEndpointDomain::BackendData => Ok(descriptor.endpoint().native_endpoint()),
                NativeEndpointDomain::BackendControl => {
                    Ok(descriptor.control_endpoint().native_endpoint())
                }
                NativeEndpointDomain::FrontendMembership => Err(ChannelAcquisitionError::fatal(
                    "backend descriptor cannot address frontend membership",
                )),
            },
            FrozenClientEndpoints::Single {
                endpoint,
                domain: frozen_domain,
                ..
            } if *frozen_domain == domain => Ok(endpoint),
            FrozenClientEndpoints::Single { .. } => Err(ChannelAcquisitionError::fatal(
                "Native method conflicts with the frozen endpoint domain",
            )),
        }
    }

    fn channel_key(
        &self,
        method: NativeRpcMethod,
    ) -> Result<NativeChannelKey, ChannelAcquisitionError> {
        let NativeTrafficClass::Frontend(lane) = method.contract().traffic else {
            return Err(ChannelAcquisitionError::fatal(
                "method is not a Frontend Native lane",
            ));
        };
        let peer = match &self.endpoints {
            FrozenClientEndpoints::Backend(descriptor) => descriptor.process_id(),
            FrozenClientEndpoints::Single { peer, .. } => *peer,
        };
        Ok(NativeChannelKey {
            endpoint: self.endpoint_for(method)?.clone(),
            peer,
            lane,
        })
    }

    async fn grpc(
        &self,
        method: NativeRpcMethod,
    ) -> Result<AuthenticatedNovaRocksGrpcClient, String> {
        self.grpc_with_channel_error(method)
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn grpc_with_channel_error(
        &self,
        method: NativeRpcMethod,
    ) -> Result<AuthenticatedNovaRocksGrpcClient, ChannelAcquisitionError> {
        self.grpc_with_channel_identity(method)
            .await
            .map(|(grpc, _)| grpc)
    }

    pub(in crate::native) fn grpc_for_reserved_call(
        &self,
        call: novarocks_native_adapter::native_lane::ReservedNativeCall,
    ) -> tonic::client::Grpc<
        InterceptedService<
            novarocks_native_adapter::native_lane::ReservedNativeCall,
            NativeClientAuthInterceptor,
        >,
    > {
        tonic::client::Grpc::new(InterceptedService::new(
            call,
            NativeClientAuthInterceptor::new(self.data_runtime.native_trust().as_ref().clone()),
        ))
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
    }

    pub(super) async fn grpc_with_channel_identity(
        &self,
        method: NativeRpcMethod,
    ) -> Result<(AuthenticatedNovaRocksGrpcClient, CachedNativeChannel), ChannelAcquisitionError>
    {
        let acquired = channel(&self.data_runtime, self.channel_key(method)?).await?;
        let grpc = NovaRocksGrpcClient::with_interceptor(
            acquired.channel().clone(),
            NativeClientAuthInterceptor::new(self.data_runtime.native_trust().as_ref().clone()),
        )
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
        .max_decoding_message_size(MAX_MESSAGE_BYTES);
        Ok((grpc, acquired))
    }

    /// Acquire the same cached channel identity as generated Native calls.
    /// The caller supplies the Root codec and retains its original window.
    pub(super) async fn root_result_grpc_with_channel_identity(
        &self,
    ) -> Result<(AuthenticatedRootResultGrpc, CachedNativeChannel), ChannelAcquisitionError> {
        let method = NativeRpcMethod::FetchRootResult;
        let acquired = channel(&self.data_runtime, self.channel_key(method)?).await?;
        let service = InterceptedService::new(
            acquired.channel().clone(),
            NativeClientAuthInterceptor::new(self.data_runtime.native_trust().as_ref().clone()),
        );
        let grpc = tonic::client::Grpc::new(service)
            .max_encoding_message_size(super::root_result_codec::ROOT_REQUEST_MAX_BYTES)
            .max_decoding_message_size(super::root_result_codec::ROOT_RESPONSE_MAX_BYTES);
        Ok((grpc, acquired))
    }

    pub(super) fn invalidate_channel_if_current(
        &self,
        method: NativeRpcMethod,
        acquired: &CachedNativeChannel,
    ) -> bool {
        self.channel_key(method).is_ok_and(|key| {
            key == acquired.slot().key && self.data_runtime.invalidate_channel_if_current(acquired)
        })
    }
}

/// One of `key`'s geometry-sized connections, chosen in rotation. Each
/// connection has its own single-flight dial generation.
async fn channel(
    data_runtime: &FrontendDataRuntime,
    key: NativeChannelKey,
) -> Result<CachedNativeChannel, ChannelAcquisitionError> {
    let slot = data_runtime
        .select_slot(&key)
        .map_err(ChannelAcquisitionError::retryable_network)?;
    if let Some(channel) = data_runtime.cached_channel(&slot) {
        return Ok(channel);
    }
    // Elect before waiting on the lane. This one generation survives through
    // every wait; cancellation/retirement returns to the existing retry policy.
    let reservation = data_runtime
        .begin_dial(slot)
        .map_err(ChannelAcquisitionError::retryable_network)?;
    let _dial = data_runtime.dial_lane(key.lane).await;
    if let Some(channel) = reservation
        .ready_channel()
        .map_err(ChannelAcquisitionError::retryable_network)?
    {
        return Ok(channel);
    }
    if !reservation.is_leader() {
        return Err(ChannelAcquisitionError::retryable_network(
            "Native channel dial leader exited before publication",
        ));
    }
    let endpoint = key.endpoint.clone();
    // The URI only provides Tonic's HTTP/2 origin. The connector below owns
    // the actual TCP/TLS dial using the typed endpoint, after the Frontend's
    // dial admission; this never creates a bare h2c client factory.
    let origin = format!("http://{endpoint}");
    let connector = data_runtime
        .native_transport()
        .connector_for(endpoint.clone())
        .map_err(|error| {
            ChannelAcquisitionError::fatal(format!(
                "construct Native endpoint connector failed: {error}"
            ))
        })?;
    let connector = frontend_lane_connector(
        connector,
        data_runtime.transport_admission().clone(),
        key.lane,
    );
    let created = configure_native_endpoint(
        tonic::transport::Endpoint::from_shared(origin).map_err(|error| {
            ChannelAcquisitionError::fatal(format!(
                "construct Native client origin failed: {error}"
            ))
        })?,
    )
    .timeout(Duration::from_secs(600))
    .connect_with_connector(connector)
    .await
    .map_err(ChannelAcquisitionError::failed_connection)?;
    // One Channel is one connection; its stream positions live with it.
    let channel = NativeLaneChannel::new(
        created,
        NativeLane::frontend(key.lane),
        Some(data_runtime.transport_admission()),
    );
    reservation
        .publish(channel)
        .map_err(ChannelAcquisitionError::retryable_network)
}

// A failed or cancelled pull must not leave a reconnecting old-process
// channel occupying the replacement's one live Control connection. This
// retires only the acquired cache generation; topology and attempt verdicts
// remain with their existing owners.
pub(super) struct NativeChannelAttempt<'a> {
    client: &'a Client,
    acquired: CachedNativeChannel,
    method: NativeRpcMethod,
    rpc_completed: bool,
}

impl<'a> NativeChannelAttempt<'a> {
    pub(super) fn new(
        client: &'a Client,
        method: NativeRpcMethod,
        acquired: CachedNativeChannel,
    ) -> Self {
        Self {
            client,
            method,
            acquired,
            rpc_completed: false,
        }
    }
    pub(super) fn rpc_completed(&mut self) {
        self.rpc_completed = true;
    }
}

impl Drop for NativeChannelAttempt<'_> {
    fn drop(&mut self) {
        if !self.rpc_completed {
            self.client
                .invalidate_channel_if_current(self.method, &self.acquired);
        }
    }
}

pub(crate) fn heartbeat(
    data_runtime: &FrontendDataRuntime,
    process_id: BackendProcessId,
    endpoint: RuntimeEndpoint,
    timeout: Duration,
) -> HeartbeatOutcome {
    let started = Instant::now();
    let outcome = (|| -> Result<_, String> {
        let client = Client::for_endpoint(
            endpoint.native_endpoint().clone(),
            NativeEndpointDomain::BackendControl,
            process_id,
            data_runtime.clone(),
        );
        data_runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                let (mut grpc, acquired) = client
                    .grpc_with_channel_identity(NativeRpcMethod::Heartbeat)
                    .await
                    .map_err(|error| error.to_string())?;
                let mut attempt = NativeChannelAttempt {
                    method: NativeRpcMethod::Heartbeat,
                    client: &client,
                    acquired,
                    rpc_completed: false,
                };
                let response = grpc
                    .heartbeat(Request::new(
                        novarocks_proto_models::novarocks::HeartbeatRequest {
                            expected_process_id: Some(
                                ProtocolBackendProcessId::from_domain(process_id)
                                    .as_proto()
                                    .clone(),
                            ),
                        },
                    ))
                    .await
                    .map_err(|error| format!("heartbeat rpc failed: {error}"))?;
                attempt.rpc_completed = true;
                Ok(response.into_inner())
            })
            .await
            .map_err(|_| format!("heartbeat did not complete within {timeout:?}"))?
        })?
    })();
    observe_backend_heartbeat_rtt(started.elapsed());
    match outcome {
        Ok(response) => match response
            .descriptor
            .ok_or_else(|| "heartbeat response missing descriptor".to_string())
            .and_then(|descriptor| {
                BackendProcessDescriptor::parse(descriptor).map_err(|error| error.to_string())
            })
            .and_then(|descriptor| {
                parse_reported_state(response.reported_state)
                    .map(|reported_state| (descriptor, reported_state))
                    .map_err(|error| error.to_string())
            })
            .and_then(|(descriptor, reported_state)| {
                let capability = response
                    .admission_epoch_capability
                    .as_ref()
                    .ok_or_else(|| {
                        "heartbeat response missing admission epoch capability".to_string()
                    })?;
                let capability = novarocks_task_codec::identity::decode_admission_epoch_capability(
                    capability,
                    novarocks_proto_codec::FieldPath::root("heartbeat_response")
                        .field("admission_epoch_capability"),
                )
                .map_err(|error| error.to_string())?;
                descriptor
                    .to_contract()
                    .map(|descriptor| (descriptor, reported_state, capability))
                    .map_err(|error| error.to_string())
            }) {
            Ok((descriptor, reported_state, admission_epoch_capability)) => HeartbeatOutcome::Ok {
                descriptor,
                reported_state,
                num_cores: response.num_cores,
                admission_epoch_capability,
                now_ms: now_millis(),
            },
            Err(err) => HeartbeatOutcome::Failed { err },
        },
        Err(err) => HeartbeatOutcome::Failed { err },
    }
}
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    #[tokio::test]
    async fn frozen_method_manifest_selects_exact_endpoints_and_refuses_other_domains() {
        let descriptor = FrozenBackendDescriptor::try_new(
            BackendProcessId::new_v7(),
            RuntimeEndpoint::new("data.test.invalid", 19060).unwrap(),
            RuntimeEndpoint::new("control.test.invalid", 19061).unwrap(),
            "routing-test",
            "routing-test",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            4096,
        )
        .unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let client = Client::for_backend(descriptor.clone(), runtime.clone());
        for method in [
            NativeRpcMethod::ApplyTaskOperations,
            NativeRpcMethod::FetchTaskResult,
            NativeRpcMethod::SubscribeTaskStatus,
            NativeRpcMethod::FetchTaskDynamicFilters,
            NativeRpcMethod::GetFinalTaskInfo,
            NativeRpcMethod::PruneCatalogs,
        ] {
            assert_eq!(
                client.endpoint_for(method).unwrap(),
                descriptor.endpoint().native_endpoint()
            );
        }
        for method in [
            NativeRpcMethod::ApplyTaskControlOperations,
            NativeRpcMethod::Heartbeat,
        ] {
            assert_eq!(
                client.endpoint_for(method).unwrap(),
                descriptor.control_endpoint().native_endpoint()
            );
        }
        for method in [
            NativeRpcMethod::AnnounceBackend,
            NativeRpcMethod::RetiredFetchResult,
            NativeRpcMethod::RetiredExchange,
        ] {
            assert!(matches!(
                client.endpoint_for(method),
                Err(ChannelAcquisitionError::Fatal(_))
            ));
        }
        let only_data = Client::for_endpoint(
            descriptor.endpoint().native_endpoint().clone(),
            NativeEndpointDomain::BackendData,
            descriptor.process_id(),
            runtime,
        );
        assert!(matches!(
            only_data
                .grpc_with_channel_error(NativeRpcMethod::Heartbeat)
                .await,
            Err(ChannelAcquisitionError::Fatal(_))
        ));
    }
    #[tokio::test]
    async fn exact_process_and_manifest_lane_share_control_and_observation_but_isolate_data() {
        let descriptor = FrozenBackendDescriptor::try_new(
            BackendProcessId::new_v7(),
            RuntimeEndpoint::new("data.test.invalid", 19060).unwrap(),
            RuntimeEndpoint::new("control.test.invalid", 19061).unwrap(),
            "lane-test",
            "lane-test",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            4096,
        )
        .unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let client = Client::for_backend(descriptor.clone(), runtime.clone());
        let control = client
            .channel_key(NativeRpcMethod::ApplyTaskControlOperations)
            .unwrap();
        let heartbeat = Client::for_endpoint(
            descriptor.control_endpoint().native_endpoint().clone(),
            NativeEndpointDomain::BackendControl,
            descriptor.process_id(),
            runtime.clone(),
        );
        assert_eq!(
            control,
            heartbeat.channel_key(NativeRpcMethod::Heartbeat).unwrap()
        );
        let observation = client
            .channel_key(NativeRpcMethod::SubscribeTaskStatus)
            .unwrap();
        assert_eq!(
            observation,
            client
                .channel_key(NativeRpcMethod::FetchTaskDynamicFilters)
                .unwrap()
        );
        assert_eq!(
            observation,
            client
                .channel_key(NativeRpcMethod::GetFinalTaskInfo)
                .unwrap()
        );
        let submission = client
            .channel_key(NativeRpcMethod::ApplyTaskOperations)
            .unwrap();
        assert_eq!(
            submission,
            client.channel_key(NativeRpcMethod::PruneCatalogs).unwrap()
        );
        assert_ne!(observation, submission);
        assert_ne!(
            observation,
            client
                .channel_key(NativeRpcMethod::FetchTaskResult)
                .unwrap()
        );
        let replacement = Client::for_endpoint(
            descriptor.control_endpoint().native_endpoint().clone(),
            NativeEndpointDomain::BackendControl,
            BackendProcessId::new_v7(),
            runtime,
        );
        assert_ne!(
            control,
            replacement.channel_key(NativeRpcMethod::Heartbeat).unwrap()
        );
    }
    #[tokio::test]
    async fn failed_or_cancelled_heartbeat_retires_only_its_acquired_control_generation() {
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let client = Client::for_endpoint(
            NativeEndpoint::from_host_port("127.0.0.1", 12345).unwrap(),
            NativeEndpointDomain::BackendControl,
            BackendProcessId::new_v7(),
            runtime.clone(),
        );
        let lane = client.channel_key(NativeRpcMethod::Heartbeat).unwrap();
        // Lifecycle control keeps exactly one connection per Backend process.
        let key = runtime.select_slot(&lane).unwrap();
        assert_eq!(key.index, 0);
        let lane_channel =
            |channel| NativeLaneChannel::new(channel, NativeLane::LifecycleControl, None);
        for rpc_completed in [false, true] {
            runtime.invalidate_peer(lane.peer);
            let (channel, _) = tonic::transport::Channel::balance_channel::<String>(1);
            let acquired = runtime
                .begin_dial(key.clone())
                .unwrap()
                .publish(lane_channel(channel))
                .unwrap();
            drop(NativeChannelAttempt {
                method: NativeRpcMethod::Heartbeat,
                client: &client,
                acquired,
                rpc_completed,
            });
            assert_eq!(runtime.cached_channel(&key).is_some(), rpc_completed);
        }
        let old = runtime.cached_channel(&key).unwrap();
        runtime.invalidate_peer(lane.peer);
        let (channel, _) = tonic::transport::Channel::balance_channel::<String>(1);
        let replacement = runtime
            .begin_dial(key.clone())
            .unwrap()
            .publish(lane_channel(channel))
            .unwrap();
        drop(NativeChannelAttempt {
            method: NativeRpcMethod::Heartbeat,
            client: &client,
            acquired: old,
            rpc_completed: false,
        });
        assert!(runtime.cached_channel(&key).is_some());
        assert!(client.invalidate_channel_if_current(NativeRpcMethod::Heartbeat, &replacement));
    }

    #[tokio::test]
    async fn original_admitted_root_dial_preserves_actual_tcp_failure_evidence() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            NativeEndpoint::from_host_port("127.0.0.1", listener.local_addr().unwrap().port())
                .unwrap();
        drop(listener);
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let client = Client::for_endpoint(
            endpoint,
            NativeEndpointDomain::BackendData,
            BackendProcessId::new_v7(),
            runtime,
        );
        let failure = tokio::time::timeout(
            Duration::from_secs(10),
            client.root_result_grpc_with_channel_identity(),
        )
        .await
        .expect("original local dial exits")
        .err()
        .expect("unbound local endpoint refuses TCP");
        assert!(matches!(
            failure,
            ChannelAcquisitionError::RetryableNetwork(_)
        ));
        assert!(
            failure.observed_connection_loss(),
            "the original admitted connector must retain its TCP evidence"
        );
    }

    #[tokio::test]
    async fn original_connection_phase_and_h2_io_are_preserved_without_text_classification() {
        use novarocks_native_trust::NativeConnectFailure;
        let tcp = std::io::Error::other(NativeConnectFailure::Unreachable(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "private endpoint detail",
        )));
        assert!(is_observed_connection_loss(&tcp));
        let tls = std::io::Error::other(NativeConnectFailure::Handshake(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "peer closed TLS",
        )));
        assert!(!is_observed_connection_loss(&tls));
        let dns = std::io::Error::other(NativeConnectFailure::Resolution(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "resolver reset",
        )));
        assert!(!is_observed_connection_loss(&dns));
        let timeout = std::io::Error::other(NativeConnectFailure::Unreachable(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "dial budget expired"),
        ));
        assert!(!is_observed_connection_loss(&timeout));
        let (local, peer) = tokio::io::duplex(64);
        drop(peer);
        let h2 = h2::client::handshake(local)
            .await
            .expect_err("the actual closed IO refuses the h2 preface");
        assert!(h2.get_io().is_some());
        assert!(is_observed_connection_loss(&h2));
    }

    /// Accepts and holds every connection; a stock Tonic dial completes
    /// without the peer's SETTINGS, so this counts physical connections.
    async fn counting_listener() -> (
        NativeEndpoint,
        tokio::sync::watch::Receiver<usize>,
        tokio::task::JoinHandle<()>,
    ) {
        counting_listener_with_gate(None).await
    }

    async fn counting_listener_with_gate(
        accept_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    ) -> (
        NativeEndpoint,
        tokio::sync::watch::Receiver<usize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            NativeEndpoint::from_host_port("127.0.0.1", listener.local_addr().unwrap().port())
                .unwrap();
        let (counted, accepted) = tokio::sync::watch::channel(0usize);
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                if let Some(gate) = &accept_gate {
                    let Ok(permit) = gate.acquire().await else {
                        break;
                    };
                    permit.forget();
                }
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                counted.send_modify(|count| *count += 1);
                held.push(socket);
            }
        });
        (endpoint, accepted, task)
    }

    // The caller's original dial timeout covers both client IO and server accept.
    async fn dial_with_accept_confirmation(
        client: &Client,
        method: NativeRpcMethod,
        accepted: &tokio::sync::watch::Receiver<usize>,
        expected_accepted: usize,
        client_ready: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<(AuthenticatedNovaRocksGrpcClient, CachedNativeChannel), ChannelAcquisitionError>
    {
        let channel = client.grpc_with_channel_identity(method).await?;
        if let Some(ready) = client_ready {
            ready
                .send(())
                .expect("controlled dial observer remains live");
        }
        // Client IO can exist while the listener's accept loop is still waiting
        // to run. The watch value preserves acceptance before this subscription.
        let mut observed = accepted.clone();
        observed
            .wait_for(|count| *count >= expected_accepted)
            .await
            .map_err(|error| {
                ChannelAcquisitionError::fatal(format!(
                    "counting listener exited before acceptance: {error}"
                ))
            })?;
        Ok(channel)
    }

    struct GatedResultLane {
        runtime: FrontendDataRuntime,
        client: Client,
        accepted: tokio::sync::watch::Receiver<usize>,
        accept_gate: std::sync::Arc<tokio::sync::Semaphore>,
        listener_task: tokio::task::JoinHandle<()>,
        peer: BackendProcessId,
    }

    impl GatedResultLane {
        async fn with_three_accepted() -> Self {
            use novarocks_proto_codec::native_rpc::FrontendNativeLane;
            assert_eq!(
                novarocks_native_adapter::native_lane::frontend_lane_connections(
                    FrontendNativeLane::ResultData
                ),
                4
            );
            let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
            let peer = BackendProcessId::new_v7();
            let accept_gate = std::sync::Arc::new(tokio::sync::Semaphore::new(3));
            let (endpoint, accepted, listener_task) =
                counting_listener_with_gate(Some(std::sync::Arc::clone(&accept_gate))).await;
            let client = Client::for_endpoint(
                endpoint,
                NativeEndpointDomain::BackendData,
                peer,
                runtime.clone(),
            );
            for index in 0..3 {
                let (_, acquired) = tokio::time::timeout(
                    Duration::from_secs(10),
                    dial_with_accept_confirmation(
                        &client,
                        NativeRpcMethod::FetchTaskResult,
                        &accepted,
                        index + 1,
                        None,
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(acquired.slot().index, index);
            }
            assert_eq!(*accepted.borrow(), 3);
            Self {
                runtime,
                client,
                accepted,
                accept_gate,
                listener_task,
                peer,
            }
        }

        fn assert_four_client_channels_but_three_server_accepts(&self) {
            let key = self
                .client
                .channel_key(NativeRpcMethod::FetchTaskResult)
                .unwrap();
            assert_eq!(self.runtime.cached_connections(&key), 4);
            assert_eq!(*self.accepted.borrow(), 3);
            // The old immediate counter oracle would deterministically fail 3 != 4.
            assert_ne!(*self.accepted.borrow(), 4);
        }
    }

    impl Drop for GatedResultLane {
        fn drop(&mut self) {
            self.runtime.invalidate_peer(self.peer);
            self.listener_task.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frontend_lane_accept_barrier_waits_for_the_fourth_real_socket() {
        let case = GatedResultLane::with_three_accepted().await;
        let (ready, mut client_ready) = tokio::sync::oneshot::channel();
        let mut fourth = Box::pin(tokio::time::timeout(
            Duration::from_secs(10),
            dial_with_accept_confirmation(
                &case.client,
                NativeRpcMethod::FetchTaskResult,
                &case.accepted,
                4,
                Some(ready),
            ),
        ));
        tokio::select! {
            ready = &mut client_ready => ready.unwrap(),
            _ = &mut fourth => panic!("fourth dial ended before the controlled accept gate opened"),
        }
        case.assert_four_client_channels_but_three_server_accepts();
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(fourth.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        case.accept_gate.add_permits(1);
        let (_, acquired) = fourth.await.unwrap().unwrap();
        assert_eq!(acquired.slot().index, 3);
        assert_eq!(*case.accepted.borrow(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frontend_lane_accept_barrier_keeps_the_original_dial_deadline() {
        let case = GatedResultLane::with_three_accepted().await;
        let (ready, mut client_ready) = tokio::sync::oneshot::channel();
        let started = tokio::time::Instant::now();
        let mut fourth = Box::pin(tokio::time::timeout_at(
            started + Duration::from_secs(10),
            dial_with_accept_confirmation(
                &case.client,
                NativeRpcMethod::FetchTaskResult,
                &case.accepted,
                4,
                Some(ready),
            ),
        ));
        tokio::select! {
            ready = &mut client_ready => ready.unwrap(),
            _ = &mut fourth => panic!("fourth dial ended before the controlled accept gate opened"),
        }
        case.assert_four_client_channels_but_three_server_accepts();
        // Leave the gate closed. Real time, without test-util or a new timer,
        // must exhaust the same dial deadline while the accepted count is three.
        assert!(fourth.await.is_err());
        assert!(started.elapsed() >= Duration::from_secs(10));
        assert_eq!(*case.accepted.borrow(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frontend_lane_connection_count_equals_the_geometry() {
        use novarocks_native_adapter::native_lane::frontend_lane_connections;
        use novarocks_native_adapter::native_transport_admission::TransportClass;
        use novarocks_proto_codec::native_rpc::FrontendNativeLane;
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let admission = runtime.transport_admission().clone();
        let peer = BackendProcessId::new_v7();
        let (data, data_accepted, data_task) = counting_listener().await;
        let (control, control_accepted, control_task) = counting_listener().await;
        let data_client = Client::for_endpoint(
            data,
            NativeEndpointDomain::BackendData,
            peer,
            runtime.clone(),
        );
        let control_client = Client::for_endpoint(
            control,
            NativeEndpointDomain::BackendControl,
            peer,
            runtime.clone(),
        );
        let mut expected_data = 0;
        for (client, method, lane, accepted) in [
            (
                &data_client,
                NativeRpcMethod::FetchTaskResult,
                FrontendNativeLane::ResultData,
                &data_accepted,
            ),
            (
                &data_client,
                NativeRpcMethod::ApplyTaskOperations,
                FrontendNativeLane::Submission,
                &data_accepted,
            ),
            (
                &data_client,
                NativeRpcMethod::SubscribeTaskStatus,
                FrontendNativeLane::Observation,
                &data_accepted,
            ),
            (
                &control_client,
                NativeRpcMethod::Heartbeat,
                FrontendNativeLane::LifecycleControl,
                &control_accepted,
            ),
        ] {
            let connections = frontend_lane_connections(lane);
            let before = *accepted.borrow();
            let mut slots = Vec::new();
            // Two full rotations: the first dials every connection once, the
            // second reuses them in the same order.
            for rotation in 0..2 * connections {
                let expected_accepted = before + (rotation + 1).min(connections);
                let (_, acquired) = tokio::time::timeout(
                    Duration::from_secs(10),
                    dial_with_accept_confirmation(
                        client,
                        method,
                        accepted,
                        expected_accepted,
                        None,
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(acquired.slot().key.lane, lane);
                assert_eq!(acquired.channel().lane(), NativeLane::frontend(lane));
                slots.push(acquired.slot().index);
            }
            let expected: Vec<_> = (0..connections).chain(0..connections).collect();
            assert_eq!(slots, expected, "{lane:?} rotates over its connections");
            assert_eq!(
                *accepted.borrow() - before,
                connections,
                "{lane:?} opens exactly the geometry's connections"
            );
            let key = client.channel_key(method).unwrap();
            assert_eq!(runtime.cached_connections(&key), connections);
            if lane != FrontendNativeLane::LifecycleControl {
                expected_data += connections;
            }
        }
        assert_eq!(expected_data, 4 + 2 + 4);
        assert_eq!(
            admission.positions(TransportClass::Data)
                - admission.available_positions(TransportClass::Data),
            expected_data
        );
        assert_eq!(
            admission.positions(TransportClass::Control)
                - admission.available_positions(TransportClass::Control),
            1
        );
        // Every dial returned its handshake position once its IO existed.
        assert_eq!(
            admission.available_handshakes(TransportClass::Data),
            admission.handshake_positions(TransportClass::Data)
        );
        // A replaced process's connections leave the cache.
        runtime.invalidate_peer(peer);
        assert_eq!(
            runtime.cached_connections(
                &data_client
                    .channel_key(NativeRpcMethod::FetchTaskResult)
                    .unwrap()
            ),
            0
        );
        data_task.abort();
        control_task.abort();
    }

    #[tokio::test]
    async fn waiting_and_cancelled_dial_generations_cannot_reopen_a_retired_peer() {
        use novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission;
        use std::future::Future;
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let endpoint =
            NativeEndpoint::from_host_port("127.0.0.1", socket.local_addr().unwrap().port())
                .unwrap();
        for cancel_leader in [false, true] {
            let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
            let peer = BackendProcessId::new_v7();
            let client = Client::for_endpoint(
                endpoint.clone(),
                NativeEndpointDomain::BackendData,
                peer,
                runtime.clone(),
            );
            let gate = runtime.dial_lane(Submission).await;
            let mut leader =
                Box::pin(client.grpc_with_channel_error(NativeRpcMethod::ApplyTaskOperations));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    leader.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            let mut follower =
                Box::pin(client.grpc_with_channel_error(NativeRpcMethod::PruneCatalogs));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    follower.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            if cancel_leader {
                drop(leader);
            } else {
                runtime.invalidate_peer(peer);
                drop(leader);
            }
            runtime.invalidate_peer(peer);
            drop(gate);
            assert!(matches!(
                follower.await,
                Err(ChannelAcquisitionError::RetryableNetwork(_))
            ));
            assert_eq!(
                socket.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "retired waiter must not open a TCP connection"
            );
        }
    }
}

#[cfg(test)]
#[path = "eager_tls_reconnect_tests.rs"]
mod eager_tls_reconnect_tests;
