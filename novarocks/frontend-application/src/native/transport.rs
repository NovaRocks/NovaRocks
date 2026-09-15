//! Narrow FE-to-BE native transport adapters.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::metrics::observe_backend_heartbeat_rtt;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_native_trust::NativeClientAuthInterceptor;
use novarocks_proto_codec::catalog::{
    PruneCatalogsOutcome, PruneCatalogsRequest, PruneCatalogsResponse,
};
use novarocks_proto_codec::membership::{
    BackendProcessDescriptor, BackendProcessId as ProtocolBackendProcessId, parse_reported_state,
};
use novarocks_query_application::api::HeartbeatOutcome;
use novarocks_types::{BackendProcessId, NativeEndpoint};

use super::data_runtime::FrontendDataRuntime;
use novarocks_native_adapter::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;

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
        .connector_for(endpoint.clone())
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
