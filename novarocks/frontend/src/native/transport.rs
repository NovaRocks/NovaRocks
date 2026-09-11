//! Narrow FE-to-BE native transport adapters.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::common::backend_topology::HeartbeatOutcome;
use crate::metrics::observe_backend_heartbeat_rtt;
use crate::native::fragment_transport::{
    ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher, decode_fetched_query_batch,
};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_native_trust::{
    AutomaticTlsMaterial, NativeClientAuthInterceptor, NativeEndpointConnector,
    NativeIncomingAdapter, NativeTlsMaterial,
};
use novarocks_proto_codec::catalog::{
    PruneCatalogsOutcome, PruneCatalogsRequest, PruneCatalogsResponse,
};
use novarocks_proto_codec::membership::{
    BackendProcessDescriptor, BackendProcessId as ProtocolBackendProcessId, parse_reported_state,
};
use novarocks_proto_models::common::UniqueId as ProtoUniqueId;
use novarocks_proto_models::novarocks::{
    FetchResultRequest, fetch_result_response::Status as FetchStatus,
};
use novarocks_types::{BackendProcessId, NativeEndpoint, UniqueId};

use super::data_runtime::FrontendDataRuntime;
use super::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;

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
    request: &PruneCatalogsRequest,
    timeout: Duration,
) -> Result<CatalogPruneDispatchOutcome, String> {
    let client = Client::new(endpoint.native_endpoint().clone(), data_runtime.clone());
    let response = data_runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let mut grpc = client.grpc().await?;
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

/// Server-materialized transport capability consumed by the Frontend role.
///
/// It contains no source configuration or filesystem path.  The Server builds
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
    pub(crate) const fn permits_confidential_credential_leases(&self) -> bool {
        matches!(self, Self::Automatic(_) | Self::Pem(_))
    }

    pub(crate) fn connector(
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

    pub(crate) fn incoming_adapter(&self) -> NativeIncomingAdapter {
        match self {
            Self::Plaintext => NativeIncomingAdapter::plaintext(),
            Self::Automatic(material) => NativeIncomingAdapter::automatic(material),
            Self::Pem(material) => NativeIncomingAdapter::pem(material),
        }
    }
}

pub(super) type AuthenticatedNovaRocksGrpcClient =
    NovaRocksGrpcClient<InterceptedService<Channel, NativeClientAuthInterceptor>>;

/// A Native channel either fails before an outbound connection is attempted,
/// or while that connection is being established.  TaskUpdate must preserve
/// that distinction: the latter has an unknown remote outcome, while the
/// former cannot be repaired by resending an immutable request.
#[derive(Debug)]
pub(super) enum ChannelAcquisitionError {
    Fatal(String),
    RetryableNetwork(String),
}

impl ChannelAcquisitionError {
    pub(super) fn fatal(detail: impl Into<String>) -> Self {
        Self::Fatal(detail.into())
    }

    pub(super) fn retryable_network(detail: impl Into<String>) -> Self {
        Self::RetryableNetwork(detail.into())
    }
}

impl std::fmt::Display for ChannelAcquisitionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fatal(detail) | Self::RetryableNetwork(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for ChannelAcquisitionError {}

#[derive(Clone)]
pub(super) struct Client {
    endpoint: NativeEndpoint,
    data_runtime: FrontendDataRuntime,
}

impl Client {
    pub(super) fn new(endpoint: NativeEndpoint, data_runtime: FrontendDataRuntime) -> Self {
        Self {
            endpoint,
            data_runtime,
        }
    }

    async fn grpc(&self) -> Result<AuthenticatedNovaRocksGrpcClient, String> {
        self.grpc_with_channel_error()
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn grpc_with_channel_error(
        &self,
    ) -> Result<AuthenticatedNovaRocksGrpcClient, ChannelAcquisitionError> {
        Ok(NovaRocksGrpcClient::with_interceptor(
            channel(&self.data_runtime, self.endpoint.clone()).await?,
            NativeClientAuthInterceptor::new(self.data_runtime.native_trust().as_ref().clone()),
        )
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
        .max_decoding_message_size(MAX_MESSAGE_BYTES))
    }
}

async fn channel(
    data_runtime: &FrontendDataRuntime,
    endpoint: NativeEndpoint,
) -> Result<Channel, ChannelAcquisitionError> {
    if let Some(channel) = data_runtime.cached_channel(&endpoint) {
        return Ok(channel);
    }
    // The URI only provides Tonic's HTTP/2 origin. The connector below owns
    // the actual TCP/TLS dial using the typed endpoint; this never creates a
    // bare h2c client factory.
    let origin = format!("http://{endpoint}");
    let connector = data_runtime
        .native_transport()
        .connector(endpoint.clone())
        .map_err(|error| {
            ChannelAcquisitionError::fatal(format!(
                "construct Native endpoint connector failed: {error}"
            ))
        })?;
    let created = tonic::transport::Endpoint::from_shared(origin)
        .map_err(|error| {
            ChannelAcquisitionError::fatal(format!(
                "construct Native client origin failed: {error}"
            ))
        })?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .initial_stream_window_size(Some(32 * 1024 * 1024))
        .initial_connection_window_size(Some(128 * 1024 * 1024))
        .connect_with_connector(connector)
        .await
        .map_err(|error| {
            ChannelAcquisitionError::retryable_network(format!(
                "connect Native endpoint failed: {error}"
            ))
        })?;
    data_runtime.cache_channel(endpoint, created.clone());
    Ok(created)
}

pub(crate) fn new_fragment_dispatcher(
    backends: &[(usize, RuntimeEndpoint)],
    data_runtime: FrontendDataRuntime,
) -> Result<Arc<dyn FragmentDispatcher>, String> {
    Ok(Arc::new(RemoteDispatcher::new(backends, data_runtime)?))
}

struct RemoteDispatcher {
    clients: BTreeMap<usize, Client>,
    endpoints: BTreeMap<usize, RuntimeEndpoint>,
}
impl RemoteDispatcher {
    fn new(
        backends: &[(usize, RuntimeEndpoint)],
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("RemoteDispatcher requires at least one backend".to_string());
        }
        let mut clients = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for (id, endpoint) in backends {
            if clients
                .insert(
                    *id,
                    Client::new(endpoint.native_endpoint().clone(), data_runtime.clone()),
                )
                .is_some()
            {
                return Err(format!("duplicate backend_idx {id}"));
            }
            endpoints.insert(*id, endpoint.clone());
        }
        Ok(Self { clients, endpoints })
    }
}
impl FragmentDispatcher for RemoteDispatcher {
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String> {
        let client = self.clients.get(&backend_idx).ok_or_else(|| {
            format!(
                "backend_idx {backend_idx} out of range (have {} backends)",
                self.clients.len()
            )
        })?;
        let addr = &self.endpoints[&backend_idx];
        let request = FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.high(),
                lo: finst_id.low(),
            }),
            max_wait_ms,
        };
        let response = client.data_runtime.block_on(async {
            let mut grpc = client.grpc().await?;
            grpc.fetch_result(request)
                .await
                .map(|response| response.into_inner())
                .map_err(|error| format!("fetch_result rpc failed: {error}"))
        })??;
        match FetchStatus::try_from(response.status).map_err(|_| {
            format!(
                "BE[{backend_idx}] ({addr}): remote fetch_result returned unknown status {}",
                response.status
            )
        })? {
            FetchStatus::Ready if response.eos => Ok(FetchOutcome::Eof),
            FetchStatus::Ready if response.result_arrow_ipc.is_empty() => Err(format!(
                "BE[{backend_idx}] ({addr}): fetch_result READY without result_arrow_ipc"
            )),
            FetchStatus::Ready => decode_fetched_query_batch(&response.result_arrow_ipc, expected)
                .map(FetchOutcome::Ready)
                .map_err(|error| {
                    format!(
                        "BE[{backend_idx}] ({addr}): {}",
                        error.replacen("typed root result", "typed fetch_result", 1)
                    )
                }),
            FetchStatus::NotReady => Ok(FetchOutcome::NotReady),
            FetchStatus::Eof => Ok(FetchOutcome::Eof),
            FetchStatus::Error => Ok(FetchOutcome::Err(response.message)),
            FetchStatus::ResultStatusUnspecified => Err(format!(
                "BE[{backend_idx}] ({addr}): remote fetch_result returned unspecified status"
            )),
        }
    }
    fn backend_count(&self) -> usize {
        self.clients.len()
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
        let client = Client::new(endpoint.native_endpoint().clone(), data_runtime.clone());
        data_runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                let mut grpc = client.grpc().await?;
                grpc.heartbeat(Request::new(
                    novarocks_proto_models::novarocks::HeartbeatRequest {
                        expected_process_id: Some(
                            ProtocolBackendProcessId::from_domain(process_id)
                                .as_proto()
                                .clone(),
                        ),
                    },
                ))
                .await
                .map(|value| value.into_inner())
                .map_err(|error| format!("heartbeat rpc failed: {error}"))
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
                Ok((descriptor, reported_state, capability))
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
