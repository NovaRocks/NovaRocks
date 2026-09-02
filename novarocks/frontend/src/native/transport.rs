//! Narrow FE-to-BE native transport adapters.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::common::backend_topology::{HeartbeatOutcome, LiveBackendTarget};
use crate::metrics::observe_backend_heartbeat_rtt;
use crate::native::fragment_transport::{
    ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher, decode_fetched_query_batch,
};
use crate::query_execution::lifecycle_plan::QueryLifecycleTarget;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_native_trust::{
    AutomaticTlsMaterial, NativeClientAuthInterceptor, NativeEndpointConnector,
    NativeIncomingAdapter, NativeTlsMaterial,
};
use novarocks_proto_codec::catalog::{
    PruneCatalogsOutcome, PruneCatalogsRequest, PruneCatalogsResponse,
};
use novarocks_proto_codec::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand, QueryControlEvent, QueryInitAck,
    QueryInitRequest, QueryStageAck, QueryStageRequest, QueryStartAck, QueryStartRequest,
    QueryTerminationAck, QueryTerminationReason,
};
use novarocks_proto_codec::membership::{
    BackendProcessDescriptor, BackendProcessId as ProtocolBackendProcessId, parse_reported_state,
};
use novarocks_proto_models::common::UniqueId as ProtoUniqueId;
use novarocks_proto_models::novarocks::{
    FetchResultRequest, QueryExecutionId as ProtoQueryExecutionId,
    fetch_result_response::Status as FetchStatus,
};
use novarocks_types::{BackendProcessId, NativeEndpoint, UniqueId};

use super::data_runtime::FrontendDataRuntime;
use super::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
use super::query_lifecycle::{
    QueryControlSession, QueryLifecycleTransport, QueryLifecycleTransportError,
    QueryLifecycleTransportErrorKind,
};

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const QUERY_CONTROL_CHANNEL_CAPACITY: usize = 32;

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

    async fn grpc_deadline(
        &self,
        operation: &str,
        deadline: tokio::time::Instant,
    ) -> Result<AuthenticatedNovaRocksGrpcClient, String> {
        tokio::time::timeout_at(deadline, self.grpc())
            .await
            .map_err(|_| format!("{operation} deadline exceeded during channel acquisition"))?
            .map_err(|error| format!("{operation} channel acquisition failed: {error}"))
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
) -> HeartbeatOutcome {
    let started = Instant::now();
    let outcome = (|| -> Result<_, String> {
        let client = Client::new(endpoint.native_endpoint().clone(), data_runtime.clone());
        data_runtime.block_on(async {
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
            }) {
            Ok((descriptor, reported_state)) => HeartbeatOutcome::Ok {
                descriptor,
                reported_state,
                num_cores: response.num_cores,
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

pub(crate) fn new_query_lifecycle_transport(
    backends: &[LiveBackendTarget],
    data_runtime: FrontendDataRuntime,
) -> Result<Arc<dyn QueryLifecycleTransport>, String> {
    if backends.is_empty() {
        return Err("gRPC query lifecycle transport requires at least one backend".to_string());
    }
    let mut entries = BTreeMap::new();
    for backend in backends {
        if entries
            .insert(
                backend.backend_idx(),
                (
                    QueryLifecycleTarget::new(
                        backend.backend_idx(),
                        backend.endpoint().map_err(|error| error.to_string())?,
                        backend.process_id().map_err(|error| error.to_string())?,
                    ),
                    Client::new(
                        backend
                            .endpoint()
                            .map_err(|error| error.to_string())?
                            .native_endpoint()
                            .clone(),
                        data_runtime.clone(),
                    ),
                ),
            )
            .is_some()
        {
            return Err(format!("duplicate backend_idx {}", backend.backend_idx()));
        }
    }
    Ok(Arc::new(LifecycleTransport {
        backends: entries,
        data_runtime,
    }))
}
struct LifecycleTransport {
    backends: BTreeMap<usize, (QueryLifecycleTarget, Client)>,
    data_runtime: FrontendDataRuntime,
}
impl LifecycleTransport {
    fn client(
        &self,
        target: QueryLifecycleTarget,
    ) -> Result<&Client, QueryLifecycleTransportError> {
        let (actual, client) = self
            .backends
            .get(&target.backend_idx())
            .ok_or_else(|| unavailable("backend is absent from frozen lifecycle topology"))?;
        if *actual != target {
            return Err(unavailable(format!(
                "backend {} target changed from {}@{} to {}@{}",
                target.backend_idx(),
                actual.endpoint(),
                actual.process_id(),
                target.endpoint(),
                target.process_id()
            )));
        }
        Ok(client)
    }
}
impl QueryLifecycleTransport for LifecycleTransport {
    fn permits_confidential_credential_leases(&self) -> bool {
        self.data_runtime
            .native_transport()
            .permits_confidential_credential_leases()
    }

    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
        validate_init_target(&target, &request)?;
        let identity = request
            .manifest()
            .and_then(|manifest| manifest.execution_id())
            .map_err(invalid)?;
        let confidential = !request
            .credential_lease_envelopes()
            .map_err(invalid)?
            .is_empty();
        if confidential && !self.permits_confidential_credential_leases() {
            return Err(invalid(
                "credential lease envelopes require TLS Native transport",
            ));
        }
        // The exact role-local TLS selection is the confidential-carrier
        // admission point. Reparse with the matching codec entrypoint so an
        // h2c path cannot reuse a request constructed by a TLS caller.
        if confidential {
            QueryInitRequest::parse_tls(request.as_proto().clone()).map_err(invalid)?;
        } else {
            QueryInitRequest::parse(request.as_proto().clone()).map_err(invalid)?;
        }
        let response = unary(
            self.client(target)?,
            "InitQuery",
            timeout,
            |mut grpc, wire| async move { grpc.init_query(wire).await },
            request.as_proto().clone(),
        )?;
        let ack = QueryInitAck::parse(response).map_err(invalid)?;
        if ack.execution_id().map_err(invalid)? != identity {
            return Err(invalid("InitQuery acknowledgement identity mismatch"));
        }
        // The acknowledged manifest identity is compared by the coordinator
        // against the value it retained when it materialized the participant.
        // Re-deriving it here would hash the manifest a second time per attempt.
        Ok(ack)
    }
    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
        let (tx, rx) = mpsc::channel(QUERY_CONTROL_CHANNEL_CAPACITY);
        tx.try_send(novarocks_proto_models::novarocks::QueryControlRequest {
            command: Some(
                novarocks_proto_models::novarocks::query_control_request::Command::Attach(
                    attach.as_proto().clone(),
                ),
            ),
        })
        .map_err(|error| unavailable(error.to_string()))?;
        let client = self.client(target)?.clone();
        let stream = self
            .data_runtime
            .block_on(async move {
                let deadline = tokio::time::Instant::now() + timeout;
                let mut grpc = client
                    .grpc_deadline("query_control_attach", deadline)
                    .await
                    .map_err(unavailable)?;
                tokio::time::timeout_at(
                    deadline,
                    grpc.query_control_stream(Request::new(ReceiverStream::new(rx))),
                )
                .await
                .map_err(|_| deadline_error("query control attach deadline exceeded"))?
                .map(|value| value.into_inner())
                .map_err(status_error)
            })
            .map_err(unavailable)??;
        let (events_tx, events_rx) = mpsc::channel(QUERY_CONTROL_CHANNEL_CAPACITY);
        let commands = Arc::new(Mutex::new(ControlCommands {
            sender: Some(tx),
            pending: VecDeque::new(),
            accepted_termination: None,
            terminal: None,
        }));
        let bridge_commands = Arc::clone(&commands);
        let bridge = self
            .data_runtime
            .spawn(bridge(stream, events_tx, bridge_commands));
        Ok(Arc::new(ControlSession {
            commands,
            events: Mutex::new(events_rx),
            bridge: Mutex::new(Some(bridge)),
            data_runtime: self.data_runtime.clone(),
            permits_confidential_credential_leases: self
                .data_runtime
                .native_transport()
                .permits_confidential_credential_leases(),
        }))
    }
    fn stage_fragments(
        &self,
        target: QueryLifecycleTarget,
        request: &QueryStageRequest,
        timeout: Duration,
    ) -> Result<QueryStageAck, QueryLifecycleTransportError> {
        let response = unary(
            self.client(target)?,
            "StageFragments",
            timeout,
            |mut grpc, wire| async move { grpc.stage_fragments(wire).await },
            request.as_proto().clone(),
        )?;
        let ack = QueryStageAck::parse(response).map_err(invalid)?;
        if ack.execution_id() != request.participant().execution_id().map_err(invalid)? {
            return Err(invalid("StageFragments acknowledgement identity mismatch"));
        }
        // The acknowledged stage identity is compared by the coordinator
        // against the value its StageBatch retained when the batch was frozen.
        Ok(ack)
    }
    fn start_prepared_query(
        &self,
        target: QueryLifecycleTarget,
        request: &QueryStartRequest,
        timeout: Duration,
    ) -> Result<QueryStartAck, QueryLifecycleTransportError> {
        let response = unary(
            self.client(target)?,
            "StartPreparedQuery",
            timeout,
            |mut grpc, wire| async move { grpc.start_prepared_query(wire).await },
            request.as_proto().clone(),
        )?;
        let ack = QueryStartAck::parse(response).map_err(invalid)?;
        if ack.execution_id() != request.execution_id() || ack.digest() != request.digest() {
            return Err(invalid(
                "StartPreparedQuery acknowledgement identity or digest mismatch",
            ));
        }
        Ok(ack)
    }
    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError> {
        let execution_id = request.execution_id().map_err(invalid)?;
        let response = unary(
            self.client(target)?,
            "AbortQuery",
            timeout,
            |mut grpc, wire| async move { grpc.abort_query(wire).await },
            request.as_proto().clone(),
        )?;
        let ack = QueryTerminationAck::parse(response).map_err(invalid)?;
        if ack.execution_id().map_err(invalid)? != execution_id {
            return Err(invalid("AbortQuery acknowledgement execution id mismatch"));
        }
        Ok(ack)
    }
}

async fn call_unary<T, R, F, Fut>(
    client: Client,
    operation: &'static str,
    timeout: Duration,
    request: T,
    call: F,
) -> Result<R, QueryLifecycleTransportError>
where
    T: Send + 'static,
    F: FnOnce(AuthenticatedNovaRocksGrpcClient, Request<T>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<tonic::Response<R>, tonic::Status>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let grpc = client
        .grpc_deadline(operation, deadline)
        .await
        .map_err(unavailable)?;
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(unavailable(format!(
            "{operation} deadline exceeded before RPC submission"
        )));
    }
    let mut request = Request::new(request);
    request.set_timeout(remaining);
    tokio::time::timeout_at(deadline, call(grpc, request))
        .await
        .map_err(|_| deadline_error(format!("{operation} deadline exceeded during RPC")))?
        .map(|value| value.into_inner())
        .map_err(status_error)
}

fn unary<T, R, F, Fut>(
    client: &Client,
    operation: &'static str,
    timeout: Duration,
    call: F,
    request: T,
) -> Result<R, QueryLifecycleTransportError>
where
    T: Send + 'static,
    R: Send + 'static,
    F: FnOnce(AuthenticatedNovaRocksGrpcClient, Request<T>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<tonic::Response<R>, tonic::Status>>,
{
    client
        .data_runtime
        .block_on(call_unary(
            client.clone(),
            operation,
            timeout,
            request,
            call,
        ))
        .map_err(unavailable)?
}

struct ControlSession {
    commands: Arc<Mutex<ControlCommands>>,
    events: Mutex<mpsc::Receiver<Result<QueryControlEvent, QueryLifecycleTransportError>>>,
    bridge: Mutex<Option<tokio::task::JoinHandle<()>>>,
    data_runtime: FrontendDataRuntime,
    permits_confidential_credential_leases: bool,
}
struct ControlCommands {
    sender: Option<mpsc::Sender<novarocks_proto_models::novarocks::QueryControlRequest>>,
    pending: VecDeque<Pending>,
    /// The single terminal reason already accepted on this stream.  A later
    /// coordinator Abort cannot change a completed Finalize, but the BE may
    /// legitimately replay that immutable Finalize acknowledgement while it
    /// drains the terminal record for unary fallback.
    accepted_termination: Option<QueryTerminationReason>,
    terminal: Option<QueryLifecycleTransportError>,
}
#[derive(Clone, Copy, Debug)]
enum Pending {
    Heartbeat(u64),
    Abort,
    Finalize,
}
impl QueryControlSession for ControlSession {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
        let credential_lease_control = matches!(
            command.as_proto().command.as_ref(),
            Some(
                novarocks_proto_models::novarocks::query_control_request::Command::CredentialLeasePrepare(_)
                    | novarocks_proto_models::novarocks::query_control_request::Command::CredentialLeaseCommit(_)
            )
        );
        if credential_lease_control && !self.permits_confidential_credential_leases {
            return Err(invalid(
                "credential lease control requires TLS Native transport",
            ));
        }
        if credential_lease_control {
            QueryControlCommand::parse_tls(command.as_proto().clone()).map_err(invalid)?;
        } else {
            QueryControlCommand::parse(command.as_proto().clone()).map_err(invalid)?;
        }
        let mut state = self
            .commands
            .lock()
            .map_err(|_| unavailable("query control command lock poisoned"))?;
        if state.sender.is_none() {
            return Err(state
                .terminal
                .clone()
                .unwrap_or_else(|| closed("query control stream is closed")));
        }
        if state.pending.len() >= QUERY_CONTROL_CHANNEL_CAPACITY {
            return Err(backpressure(
                "query control pending command capacity is exhausted",
            ));
        }
        let sender = state.sender.as_ref().ok_or_else(|| {
            state
                .terminal
                .clone()
                .unwrap_or_else(|| closed("query control stream is closed"))
        })?;
        sender
            .try_send(command.as_proto().clone())
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    backpressure("query control command capacity is exhausted")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    closed("query control command stream is closed")
                }
            })?;
        match command.as_proto().command.as_ref() {
            Some(novarocks_proto_models::novarocks::query_control_request::Command::Heartbeat(
                heartbeat,
            )) => state
                .pending
                .push_back(Pending::Heartbeat(heartbeat.sequence)),
            Some(novarocks_proto_models::novarocks::query_control_request::Command::Abort(_)) => {
                state.pending.push_back(Pending::Abort)
            }
            Some(novarocks_proto_models::novarocks::query_control_request::Command::Finalize(
                _,
            )) => state.pending.push_back(Pending::Finalize),
            Some(
                novarocks_proto_models::novarocks::query_control_request::Command::TerminalAck(_),
            ) => {}
            Some(
                novarocks_proto_models::novarocks::query_control_request::Command::CredentialLeasePrepare(_)
                | novarocks_proto_models::novarocks::query_control_request::Command::CredentialLeaseCommit(_),
            ) => {}
            Some(novarocks_proto_models::novarocks::query_control_request::Command::Attach(_))
            | None => {
                return Err(invalid(
                    "validated query control command has an invalid variant",
                ));
            }
        }
        Ok(())
    }
    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError> {
        let mut events = self
            .events
            .lock()
            .map_err(|_| unavailable("query control event lock poisoned"))?;
        self.data_runtime
            .block_on(async {
                tokio::time::timeout(timeout, events.recv())
                    .await
                    .map_err(|_| deadline_error("query control event receive deadline exceeded"))?
                    .ok_or_else(|| closed("query control event stream is closed"))?
            })
            .map_err(unavailable)?
    }
}
impl Drop for ControlSession {
    fn drop(&mut self) {
        if let Ok(mut commands) = self.commands.lock() {
            commands.sender.take();
        }
        if let Ok(bridge) = self.bridge.get_mut()
            && let Some(bridge) = bridge.take()
        {
            bridge.abort();
        }
    }
}
async fn bridge(
    mut stream: tonic::Streaming<novarocks_proto_models::novarocks::QueryControlResponse>,
    events: mpsc::Sender<Result<QueryControlEvent, QueryLifecycleTransportError>>,
    commands: Arc<Mutex<ControlCommands>>,
) {
    loop {
        let next = match stream.message().await {
            Ok(Some(response)) => QueryControlEvent::parse(response).map_err(invalid),
            Ok(None) => Err(closed("query control response stream closed")),
            Err(status) => Err(stream_status_error(status)),
        };
        let terminal = next.is_err();
        if let Ok(event) = &next
            && let Err(error) = validate_control_event(event, &commands)
        {
            if let Ok(mut commands) = commands.lock() {
                commands.sender.take();
                commands.terminal = Some(error.clone());
            }
            let _ = events.send(Err(error)).await;
            break;
        }
        if events.send(next.clone()).await.is_err() {
            break;
        }
        if terminal {
            if let Ok(mut commands) = commands.lock() {
                commands.sender.take();
                commands.terminal = Some(closed("query control stream is closed"));
            }
            break;
        }
    }
}
fn validate_control_event(
    event: &QueryControlEvent,
    commands: &Mutex<ControlCommands>,
) -> Result<(), QueryLifecycleTransportError> {
    let mut commands = commands
        .lock()
        .map_err(|_| unavailable("query control command lock poisoned"))?;
    match event.as_proto().event.as_ref() {
        Some(novarocks_proto_models::novarocks::query_control_response::Event::HeartbeatAck(
            ack,
        )) => match commands.pending.front().copied() {
            Some(Pending::Heartbeat(expected)) if expected == ack.sequence => {
                commands.pending.pop_front();
                Ok(())
            }
            Some(other) => Err(invalid(format!(
                "unexpected heartbeat acknowledgement {} for {other:?}",
                ack.sequence
            ))),
            None => Err(invalid(format!(
                "received unsolicited heartbeat sequence {}",
                ack.sequence
            ))),
        },
        Some(
            novarocks_proto_models::novarocks::query_control_response::Event::TerminationAccepted(
                accepted,
            ),
        ) => {
            let reason = QueryTerminationReason::try_from(accepted.reason).map_err(|_| {
                invalid(format!(
                    "unknown query termination reason {}",
                    accepted.reason
                ))
            })?;
            let expected = commands
                .pending
                .iter()
                .copied()
                .find(|command| !matches!(command, Pending::Heartbeat(_)));
            let matches_reason = match (expected, reason) {
                (
                    Some(Pending::Abort),
                    QueryTerminationReason::QueryTerminationCoordinatorAbort,
                )
                | (
                    Some(Pending::Finalize),
                    QueryTerminationReason::QueryTerminationCoordinatorFinalize,
                ) => true,
                // Finalize is first-wins at the BE. If FE later begins abort
                // cleanup (for example after a different participant's
                // control stream drops), a replayed Finalize acknowledgement
                // is the only valid answer for that already-finalized
                // participant. This is intentionally narrower than accepting
                // arbitrary mismatched termination reasons: the same stream
                // must already have accepted Finalize.
                (
                    Some(Pending::Abort),
                    QueryTerminationReason::QueryTerminationCoordinatorFinalize,
                ) if commands.accepted_termination
                    == Some(QueryTerminationReason::QueryTerminationCoordinatorFinalize) =>
                {
                    true
                }
                (None, _) => {
                    commands
                        .pending
                        .retain(|command| !matches!(command, Pending::Heartbeat(_)));
                    return Ok(());
                }
                _ => false,
            };
            if !matches_reason {
                return Err(invalid(format!(
                    "unexpected termination acknowledgement {reason:?} for {expected:?}"
                )));
            }
            commands
                .pending
                .retain(|command| !matches!(command, Pending::Heartbeat(_)));
            commands.pending.pop_front();
            commands.accepted_termination = Some(reason);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_init_target(
    target: &QueryLifecycleTarget,
    request: &QueryInitRequest,
) -> Result<(), QueryLifecycleTransportError> {
    let identity = request
        .manifest()
        .and_then(|manifest| manifest.backend())
        .map_err(invalid)?;
    let endpoint = identity.endpoint().map_err(invalid)?;
    let process_id = identity.process_id().map_err(invalid)?;
    if endpoint.host() != target.endpoint().host()
        || i32::from(endpoint.port()) != target.endpoint().port()
        || process_id != target.process_id()
    {
        return Err(invalid(
            "InitQuery manifest backend identity does not match frozen target",
        ));
    }
    Ok(())
}
fn unavailable(detail: impl ToString) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(
        QueryLifecycleTransportErrorKind::Unavailable,
        detail.to_string(),
    )
}
fn invalid(detail: impl ToString) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(
        QueryLifecycleTransportErrorKind::InvalidResponse,
        detail.to_string(),
    )
}
fn deadline_error(detail: impl ToString) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(
        QueryLifecycleTransportErrorKind::DeadlineExceeded,
        detail.to_string(),
    )
}
fn backpressure(detail: impl ToString) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(
        QueryLifecycleTransportErrorKind::Backpressure,
        detail.to_string(),
    )
}
fn closed(detail: impl ToString) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(
        QueryLifecycleTransportErrorKind::StreamClosed,
        detail.to_string(),
    )
}
fn status_error(status: tonic::Status) -> QueryLifecycleTransportError {
    match status.code() {
        tonic::Code::DeadlineExceeded => deadline_error(format!(
            "rpc status {:?}: {}",
            status.code(),
            status.message()
        )),
        tonic::Code::ResourceExhausted => backpressure(format!(
            "rpc status {:?}: {}",
            status.code(),
            status.message()
        )),
        tonic::Code::Cancelled if status.message().to_ascii_lowercase().contains("timeout") => {
            deadline_error(format!(
                "rpc status {:?}: {}",
                status.code(),
                status.message()
            ))
        }
        tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown => unavailable(
            format!("rpc status {:?}: {}", status.code(), status.message()),
        ),
        _ => invalid(format!(
            "rpc status {:?}: {}",
            status.code(),
            status.message()
        )),
    }
}

fn stream_status_error(status: tonic::Status) -> QueryLifecycleTransportError {
    match status.code() {
        tonic::Code::InvalidArgument | tonic::Code::DataLoss => invalid(format!(
            "QueryControl stream status {:?}: {}",
            status.code(),
            status.message()
        )),
        _ => closed(format!(
            "QueryControl stream status {:?}: {}",
            status.code(),
            status.message()
        )),
    }
}

/// Native transport for runtime split assignment.
///
/// One client per admitted backend, resolved once for the round. Delivery is
/// synchronous from the driver's point of view: the driver keeps one update in
/// flight per task and uses the acknowledgement for backpressure, so there is
/// no queue of unacknowledged updates to reconcile after a failure.
pub(crate) struct GrpcTaskUpdateTransport {
    clients: std::collections::BTreeMap<usize, Client>,
}

#[allow(
    dead_code,
    reason = "Constructed by the coordinator round driver in the same PR."
)]
impl GrpcTaskUpdateTransport {
    pub(crate) fn new(
        backends: &[(usize, RuntimeEndpoint)],
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        let mut clients = std::collections::BTreeMap::new();
        for (backend_idx, endpoint) in backends {
            if clients
                .insert(
                    *backend_idx,
                    Client::new(endpoint.native_endpoint().clone(), data_runtime.clone()),
                )
                .is_some()
            {
                return Err(format!("duplicate task update backend {backend_idx}"));
            }
        }
        Ok(Self { clients })
    }
}

// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)
impl crate::query_execution::split_assignment::TaskUpdateTransport for GrpcTaskUpdateTransport {
    fn send(
        &self,
        execution_id: novarocks_proto_codec::lifecycle::QueryExecutionId,
        target: &crate::query_execution::split_assignment::AssignmentTarget,
        request: &crate::query_execution::connector_domain::TaskUpdateRequest,
        timeout: Duration,
        stop: &crate::query_execution::split_assignment::SplitAssignmentStop,
    ) -> Result<
        crate::query_execution::split_assignment::TaskUpdateOutcome,
        crate::query_execution::split_assignment::TaskUpdateTransportError,
    > {
        use crate::query_execution::split_assignment::{
            TaskUpdateOutcome, TaskUpdateTransportError,
        };

        let client = self.clients.get(&target.backend_idx).ok_or_else(|| {
            TaskUpdateTransportError::fatal(format!(
                "task update client for backend {} is missing",
                target.backend_idx
            ))
        })?;
        let request = novarocks_proto_models::novarocks::TaskUpdateRequest {
            execution_id: Some(ProtoQueryExecutionId {
                query_id: Some(ProtoUniqueId {
                    hi: execution_id.query_id().high(),
                    lo: execution_id.query_id().low(),
                }),
                attempt_id: execution_id.attempt_id().get(),
            }),
            fragment_instance_id: Some(ProtoUniqueId {
                hi: request.fragment_instance_id().high(),
                lo: request.fragment_instance_id().low(),
            }),
            assignments: request
                .to_proto_assignments()
                .map_err(TaskUpdateTransportError::fatal)?,
        };
        let response = match client.data_runtime.block_on(async {
                let deadline = tokio::time::Instant::now() + timeout;
                let mut stop = stop.subscribe();
                let mut grpc = tokio::select! {
                    _ = stop.changed() => Err(TaskUpdateTransportError::closed("task_update round stopped during channel acquisition")),
                    result = tokio::time::timeout_at(deadline, client.grpc_with_channel_error()) => result
                        .map_err(|_| TaskUpdateTransportError::retryable_network("task_update rpc timeout during channel acquisition"))?
                        .map_err(task_update_channel_acquisition_error),
                }?;
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(TaskUpdateTransportError::retryable_network(
                        "task_update rpc timeout before unary submission",
                    ));
                }
                let mut request = tonic::Request::new(request);
                request.set_timeout(remaining);
                tokio::select! {
                    _ = stop.changed() => Err(TaskUpdateTransportError::closed("task_update round stopped awaiting response")),
                    result = tokio::time::timeout_at(deadline, grpc.task_update(request)) => result
                        .map_err(|_| TaskUpdateTransportError::retryable_network("task_update rpc timeout awaiting response"))?
                        .map(|value| value.into_inner())
                        .map_err(task_update_status_error),
                }
            }) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                // A retryable outcome may have left the shared HTTP/2 stream
                // unusable. Reacquire a channel before replaying the exact
                // immutable request instead of letting a poisoned cache stall
                // its retry until the statement deadline.
                if error.kind() == crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::RetryableNetwork {
                    client.data_runtime.invalidate_channel(&client.endpoint);
                }
                return Err(error);
            }
            Err(error) => {
                return Err(TaskUpdateTransportError::fatal(format!(
                    "task_update runtime execution failed: {error}"
                )));
            }
        };

        match response.outcome {
            Some(novarocks_proto_models::novarocks::task_update_response::Outcome::Accepted(
                accepted,
            )) => Ok(TaskUpdateOutcome::Accepted(
                accepted
                    .nodes
                    .into_iter()
                    .map(
                        |node| crate::query_execution::split_assignment::AcceptedPlanNode {
                            plan_node_id: node.plan_node_id,
                            accepted_through_sequence: node.accepted_through_sequence,
                            no_more_splits: node.no_more_splits,
                            queued_splits: node.queued_splits,
                        },
                    )
                    .collect(),
            )),
            Some(novarocks_proto_models::novarocks::task_update_response::Outcome::Rejection(
                rejection,
            )) => {
                let reason =
                    novarocks_proto_models::novarocks::TaskUpdateRejectionReason::try_from(
                        rejection.reason,
                    )
                    .map(|reason| reason.as_str_name().to_owned())
                    .unwrap_or_else(|_| "UNKNOWN".to_owned());
                Ok(TaskUpdateOutcome::Rejected {
                    reason,
                    detail: rejection.safe_detail,
                })
            }
            // A response with no outcome cannot be interpreted, and guessing
            // "accepted" would lose splits silently.
            None => Err(TaskUpdateTransportError::fatal(
                "task update response carried no outcome",
            )),
        }
    }
}

/// Preserve the typed channel-acquisition boundary for TaskUpdate retries.
/// URI and connector construction are deterministic local failures, while a
/// completed connector that cannot dial or establish its HTTP/2 stream leaves
/// the remote outcome unknown.
fn task_update_channel_acquisition_error(
    error: ChannelAcquisitionError,
) -> crate::query_execution::split_assignment::TaskUpdateTransportError {
    use crate::query_execution::split_assignment::TaskUpdateTransportError;

    match error {
        ChannelAcquisitionError::Fatal(detail) => TaskUpdateTransportError::fatal(format!(
            "task_update channel acquisition failed: {detail}"
        )),
        ChannelAcquisitionError::RetryableNetwork(detail) => {
            TaskUpdateTransportError::retryable_network(format!(
                "task_update channel acquisition failed: {detail}"
            ))
        }
    }
}

/// Classify only typed unary RPC statuses. A retrying caller must retain the
/// exact request because every allowed status has an unknown remote outcome.
fn task_update_status_error(
    status: tonic::Status,
) -> crate::query_execution::split_assignment::TaskUpdateTransportError {
    use crate::query_execution::split_assignment::TaskUpdateTransportError;

    let detail = format!(
        "task_update rpc status {:?}: {}",
        status.code(),
        status.message()
    );
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => TaskUpdateTransportError::retryable_network(detail),
        _ => TaskUpdateTransportError::fatal(detail),
    }
}

#[cfg(test)]
mod task_update_transport_tests {
    use super::*;
    use crate::query_execution::split_assignment::TaskUpdateTransportErrorKind;

    #[test]
    fn task_update_status_allowlist_is_exact() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::Unknown,
        ] {
            assert_eq!(
                task_update_status_error(tonic::Status::new(code, "unknown outcome")).kind(),
                TaskUpdateTransportErrorKind::RetryableNetwork,
                "{code:?} must preserve the immutable request for retry"
            );
        }

        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::AlreadyExists,
            tonic::Code::PermissionDenied,
            tonic::Code::ResourceExhausted,
            tonic::Code::FailedPrecondition,
            tonic::Code::Aborted,
            tonic::Code::OutOfRange,
            tonic::Code::Unimplemented,
            tonic::Code::Internal,
            tonic::Code::DataLoss,
            tonic::Code::Unauthenticated,
        ] {
            assert_eq!(
                task_update_status_error(tonic::Status::new(code, "fatal")).kind(),
                TaskUpdateTransportErrorKind::Fatal,
                "{code:?} must not be retried"
            );
        }
    }

    #[test]
    fn task_update_channel_acquisition_preserves_typed_retryability() {
        let connector_construction = task_update_channel_acquisition_error(
            ChannelAcquisitionError::fatal("invalid TLS material"),
        );
        assert_eq!(
            connector_construction.kind(),
            TaskUpdateTransportErrorKind::Fatal
        );

        let dial_or_stream = task_update_channel_acquisition_error(
            ChannelAcquisitionError::retryable_network("connection interrupted"),
        );
        assert_eq!(
            dial_or_stream.kind(),
            TaskUpdateTransportErrorKind::RetryableNetwork
        );
    }
}
