//! Narrow FE-to-BE native transport adapters.

use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

use crate::common::backend_topology::{BeId, HeartbeatOutcome, LiveBackendTarget};
use crate::metrics::observe_backend_heartbeat_rtt;
use crate::native::fragment_transport::{
    ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher, decode_fetched_query_batch,
};
use crate::query_execution::artifact::ConnectorBindingDispatcher;
use crate::query_execution::connector_binding::{
    ConnectorBindingDispatchError, ConnectorBindingRetirementError,
};
use crate::query_execution::lifecycle_plan::QueryLifecycleTarget;
use novarocks_proto::common::UniqueId as ProtoUniqueId;
use novarocks_proto::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryControlCommand, QueryControlEvent, QueryInitAck,
    QueryInitRequest, QueryStageAck, QueryStageRequest, QueryStartAck, QueryStartRequest,
    QueryTerminationAck, QueryTerminationReason,
};
use novarocks_proto::novarocks::{
    ConnectorExecutionBindingDeclaration, EnsureConnectorExecutionBindingRequest,
    FetchResultRequest, IcebergExecutionBindingDeclaration,
    QueryExecutionId as ProtoQueryExecutionId, RetireConnectorExecutionBindingRequest,
    StarRocksExecutionBindingDeclaration,
    connector_execution_binding_declaration::Provider as ConnectorExecutionBindingProvider,
    fetch_result_response::Status as FetchStatus,
};
use novarocks_proto::provider::{
    EnsureConnectorExecutionBindingOutcome, EnsureConnectorExecutionBindingResult,
    RetireConnectorExecutionBindingOutcome, RetireConnectorExecutionBindingResult,
};
use novarocks_spi::connector::ConnectorExecutionDeclarationProvider;
use novarocks_types::{UniqueId, format_host_for_url};

use super::data_runtime::FrontendDataRuntime;
use super::generated::nova_rocks_grpc_client::NovaRocksGrpcClient;
use super::query_lifecycle::{
    QueryControlSession, QueryLifecycleTransport, QueryLifecycleTransportError,
    QueryLifecycleTransportErrorKind,
};

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const QUERY_CONTROL_CHANNEL_CAPACITY: usize = 32;

/// Encodes a validated SPI declaration at the FE-owned Protocol boundary.
///
// Design: ADR-0105 (docs/adr/ADR-0105-wire-authority-and-domain-carrier-separation.md)
/// This is deliberately the only frontend mapping from the transport-neutral
/// declaration into the generated wire DTO. The declaration's closed variant
/// selects the wire oneof; canonical digesting remains Protocol-owned and is
/// performed by the BE over this generated DTO.
#[doc(hidden)]
pub fn encode_connector_execution_declaration(
    declaration: &novarocks_spi::connector::ConnectorExecutionDeclaration,
) -> ConnectorExecutionBindingDeclaration {
    let binding_key = declaration.binding_key();
    let provider = match declaration.provider() {
        ConnectorExecutionDeclarationProvider::Iceberg { access_binding } => {
            ConnectorExecutionBindingProvider::Iceberg(IcebergExecutionBindingDeclaration {
                access_binding: access_binding.to_string(),
            })
        }
        ConnectorExecutionDeclarationProvider::StarRocks { local_binding } => {
            ConnectorExecutionBindingProvider::Starrocks(StarRocksExecutionBindingDeclaration {
                local_binding: local_binding.to_string(),
            })
        }
    };
    ConnectorExecutionBindingDeclaration {
        instance_id: binding_key.instance_id.as_str().to_string(),
        incarnation: binding_key.incarnation.to_bytes().to_vec(),
        provider: Some(provider),
    }
}

#[derive(Clone)]
struct Client {
    host: String,
    port: u16,
    data_runtime: FrontendDataRuntime,
}

impl Client {
    fn new(addr: SocketAddr, data_runtime: FrontendDataRuntime) -> Result<Self, String> {
        let client = Self {
            host: addr.ip().to_string(),
            port: addr.port(),
            data_runtime,
        };
        endpoint(&client.host, client.port)
            .map_err(|error| format!("invalid BE endpoint {addr}: {error}"))?;
        Ok(client)
    }

    async fn grpc(&self) -> Result<NovaRocksGrpcClient<Channel>, String> {
        Ok(
            NovaRocksGrpcClient::new(channel(&self.data_runtime, &self.host, self.port).await?)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MESSAGE_BYTES),
        )
    }

    async fn grpc_deadline(
        &self,
        operation: &str,
        deadline: tokio::time::Instant,
    ) -> Result<NovaRocksGrpcClient<Channel>, String> {
        tokio::time::timeout_at(deadline, self.grpc())
            .await
            .map_err(|_| format!("{operation} deadline exceeded during channel acquisition"))?
            .map_err(|error| format!("{operation} channel acquisition failed: {error}"))
    }
}

fn endpoint(host: &str, port: u16) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
    format!("http://{}:{port}", format_host_for_url(host)).parse()
}

async fn channel(
    data_runtime: &FrontendDataRuntime,
    host: &str,
    port: u16,
) -> Result<Channel, String> {
    let key = format!("{}:{port}", format_host_for_url(host));
    if let Some(channel) = data_runtime.cached_channel(&key) {
        return Ok(channel);
    }
    let created = endpoint(host, port)
        .map_err(|error| format!("invalid endpoint: {error}"))?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .initial_stream_window_size(Some(32 * 1024 * 1024))
        .initial_connection_window_size(Some(128 * 1024 * 1024))
        .connect()
        .await
        .map_err(|error| format!("connect exchange endpoint failed: {error}"))?;
    data_runtime.cache_channel(key, created.clone());
    Ok(created)
}

pub(crate) fn new_fragment_dispatcher(
    backends: &[(usize, SocketAddr)],
    data_runtime: FrontendDataRuntime,
) -> Result<Arc<dyn FragmentDispatcher>, String> {
    Ok(Arc::new(RemoteDispatcher::new(backends, data_runtime)?))
}

struct RemoteDispatcher {
    clients: BTreeMap<usize, Client>,
    endpoints: BTreeMap<usize, SocketAddr>,
}
impl RemoteDispatcher {
    fn new(
        backends: &[(usize, SocketAddr)],
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("RemoteDispatcher requires at least one backend".to_string());
        }
        let mut clients = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for (id, endpoint) in backends {
            if clients
                .insert(*id, Client::new(*endpoint, data_runtime.clone())?)
                .is_some()
            {
                return Err(format!("duplicate backend_idx {id}"));
            }
            endpoints.insert(*id, *endpoint);
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
        let addr = self.endpoints[&backend_idx];
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

pub(crate) fn new_connector_binding_dispatcher(
    backends: &[(usize, SocketAddr)],
    data_runtime: FrontendDataRuntime,
) -> Result<Arc<dyn ConnectorBindingDispatcher>, String> {
    Ok(Arc::new(ConnectorBindingControl::new(
        backends,
        data_runtime,
    )?))
}
struct ConnectorBindingControl {
    clients: BTreeMap<usize, Client>,
    endpoints: BTreeMap<usize, SocketAddr>,
}
impl ConnectorBindingControl {
    fn new(
        backends: &[(usize, SocketAddr)],
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("GrpcConnectorBindingControl requires at least one backend".to_string());
        }
        let mut clients = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for (id, endpoint) in backends {
            if clients
                .insert(*id, Client::new(*endpoint, data_runtime.clone())?)
                .is_some()
            {
                return Err(format!("duplicate connector binding backend {id}"));
            }
            endpoints.insert(*id, *endpoint);
        }
        Ok(Self { clients, endpoints })
    }
    fn client(&self, backend_idx: usize, endpoint: SocketAddr) -> Result<&Client, String> {
        if self.endpoints.get(&backend_idx) != Some(&endpoint) {
            return Err(format!(
                "connector binding endpoint mismatch for backend {backend_idx}"
            ));
        }
        self.clients
            .get(&backend_idx)
            .ok_or_else(|| format!("connector binding client for backend {backend_idx} is missing"))
    }
}
impl ConnectorBindingDispatcher for ConnectorBindingControl {
    fn install(
        &self,
        execution_id: novarocks_proto::lifecycle::QueryExecutionId,
        backend_idx: usize,
        endpoint: SocketAddr,
        declaration: &novarocks_spi::connector::ConnectorExecutionDeclaration,
    ) -> Result<(), ConnectorBindingDispatchError> {
        let client = self
            .client(backend_idx, endpoint)
            .map_err(ConnectorBindingDispatchError::Transport)?;
        let request = EnsureConnectorExecutionBindingRequest {
            execution_id: Some(ProtoQueryExecutionId {
                query_id: Some(ProtoUniqueId {
                    hi: execution_id.query_id().high(),
                    lo: execution_id.query_id().low(),
                }),
                attempt_id: execution_id.attempt_id().get(),
            }),
            declaration: Some(encode_connector_execution_declaration(declaration)),
        };
        let response = client
            .data_runtime
            .block_on(async {
                let mut grpc = client.grpc().await?;
                grpc.ensure_connector_execution_binding(request)
                    .await
                    .map(|value| value.into_inner())
                    .map_err(|error| {
                        format!("ensure_connector_execution_binding rpc failed: {error}")
                    })
            })
            .map_err(ConnectorBindingDispatchError::Transport)?
            .map_err(ConnectorBindingDispatchError::Transport)?;
        match EnsureConnectorExecutionBindingResult::try_from_proto(response)
            .map_err(|error| ConnectorBindingDispatchError::Transport(format!(
                "BE[{backend_idx}] ({endpoint}) returned an invalid ensure connector execution binding outcome: {error}"
            )))?
            .outcome()
        {
            EnsureConnectorExecutionBindingOutcome::Ensured => Ok(()),
            EnsureConnectorExecutionBindingOutcome::Rejected(rejection) => {
                Err(ConnectorBindingDispatchError::Rejected(rejection.clone()))
            }
        }
    }
    fn retire(
        &self,
        endpoint: SocketAddr,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<(), ConnectorBindingRetirementError> {
        let client = self.endpoints.iter().find_map(|(id, configured)| (*configured == endpoint).then(|| self.clients.get(id))).flatten().ok_or_else(|| ConnectorBindingRetirementError::Transport(format!("connector retirement endpoint {endpoint} is absent from configured backend snapshot")))?;
        let request = RetireConnectorExecutionBindingRequest {
            instance_id: key.instance_id.as_str().to_string(),
            incarnation: key.incarnation.to_bytes().to_vec(),
        };
        let response = client
            .data_runtime
            .block_on(async {
                let mut grpc = client.grpc().await?;
                grpc.retire_connector_execution_binding(request)
                    .await
                    .map(|value| value.into_inner())
                    .map_err(|error| {
                        format!("retire_connector_execution_binding rpc failed: {error}")
                    })
            })
            .map_err(ConnectorBindingRetirementError::Transport)?
            .map_err(ConnectorBindingRetirementError::Transport)?;
        match RetireConnectorExecutionBindingResult::try_from_proto(response)
            .map_err(|error| ConnectorBindingRetirementError::Transport(format!(
                "{endpoint} returned an invalid retire connector execution binding outcome: {error}"
            )))?
            .outcome()
        {
            RetireConnectorExecutionBindingOutcome::Accepted => Ok(()),
            outcome => Err(ConnectorBindingRetirementError::Outcome(outcome)),
        }
    }
}

pub(crate) fn heartbeat(
    data_runtime: &FrontendDataRuntime,
    be_id: BeId,
    endpoint: SocketAddr,
) -> HeartbeatOutcome {
    let started = Instant::now();
    let outcome = (|| -> Result<_, String> {
        let client = Client::new(endpoint, data_runtime.clone())?;
        data_runtime.block_on(async {
            let mut grpc = client.grpc().await?;
            grpc.heartbeat(Request::new(novarocks_proto::novarocks::HeartbeatRequest {
                assigned_be_id: be_id,
                fe_epoch: 0,
            }))
            .await
            .map(|value| value.into_inner())
            .map_err(|error| format!("heartbeat rpc failed: {error}"))
        })?
    })();
    observe_backend_heartbeat_rtt(started.elapsed());
    match outcome {
        Ok(response) if response.status_code == 0 => HeartbeatOutcome::Ok {
            start_epoch: response.start_epoch,
            version: response.version,
            num_cores: response.num_cores,
            now_ms: now_millis(),
        },
        Ok(response) => HeartbeatOutcome::Failed {
            err: format!(
                "heartbeat returned nonzero status_code {}",
                response.status_code
            ),
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
                        backend.endpoint(),
                        backend.start_epoch(),
                    ),
                    Client::new(backend.endpoint(), data_runtime.clone())?,
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
                actual.start_epoch(),
                target.endpoint(),
                target.start_epoch()
            )));
        }
        Ok(client)
    }
}
impl QueryLifecycleTransport for LifecycleTransport {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
        validate_init_target(target, &request)?;
        let identity = request
            .manifest()
            .and_then(|manifest| manifest.execution_id())
            .map_err(invalid)?;
        let digest = request.digest().map_err(invalid)?;
        let response = unary(
            self.client(target)?,
            "InitQuery",
            timeout,
            |mut grpc, wire| async move { grpc.init_query(wire).await },
            request.as_proto().clone(),
        )?;
        let ack = QueryInitAck::parse(response).map_err(invalid)?;
        if ack.execution_id().map_err(invalid)? != identity
            || ack.digest().map_err(invalid)? != digest
        {
            return Err(invalid(
                "InitQuery acknowledgement identity or digest mismatch",
            ));
        }
        Ok(ack)
    }
    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
        let (tx, rx) = mpsc::channel(QUERY_CONTROL_CHANNEL_CAPACITY);
        tx.try_send(novarocks_proto::novarocks::QueryControlRequest {
            command: Some(
                novarocks_proto::novarocks::query_control_request::Command::Attach(
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
        if ack.execution_id() != request.execution_id()
            || ack.digest_version() != request.digest_version()
            || ack.digest() != request.digest()
        {
            return Err(invalid(
                "StageFragments acknowledgement identity or digest mismatch",
            ));
        }
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
        if ack.execution_id() != request.execution_id()
            || ack.digest_version() != request.digest_version()
            || ack.digest() != request.digest()
        {
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
    F: FnOnce(NovaRocksGrpcClient<Channel>, Request<T>) -> Fut + Send + 'static,
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
    F: FnOnce(NovaRocksGrpcClient<Channel>, Request<T>) -> Fut + Send + 'static,
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
}
struct ControlCommands {
    sender: Option<mpsc::Sender<novarocks_proto::novarocks::QueryControlRequest>>,
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
            Some(novarocks_proto::novarocks::query_control_request::Command::Heartbeat(
                heartbeat,
            )) => state
                .pending
                .push_back(Pending::Heartbeat(heartbeat.sequence)),
            Some(novarocks_proto::novarocks::query_control_request::Command::Abort(_)) => {
                state.pending.push_back(Pending::Abort)
            }
            Some(novarocks_proto::novarocks::query_control_request::Command::Finalize(_)) => {
                state.pending.push_back(Pending::Finalize)
            }
            Some(novarocks_proto::novarocks::query_control_request::Command::TerminalAck(_)) => {}
            Some(novarocks_proto::novarocks::query_control_request::Command::Attach(_)) | None => {
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
    mut stream: tonic::Streaming<novarocks_proto::novarocks::QueryControlResponse>,
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
        Some(novarocks_proto::novarocks::query_control_response::Event::HeartbeatAck(ack)) => {
            match commands.pending.front().copied() {
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
            }
        }
        Some(novarocks_proto::novarocks::query_control_response::Event::TerminationAccepted(
            accepted,
        )) => {
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
    target: QueryLifecycleTarget,
    request: &QueryInitRequest,
) -> Result<(), QueryLifecycleTransportError> {
    let identity = request
        .manifest()
        .and_then(|manifest| manifest.backend())
        .map_err(invalid)?;
    let id = usize::try_from(identity.backend_id())
        .map_err(|_| invalid("InitQuery backend id exceeds usize"))?;
    let endpoint = identity.endpoint().map_err(invalid)?;
    let ip = IpAddr::from_str(endpoint.host()).map_err(|error| {
        invalid(format!(
            "InitQuery backend endpoint is not an IP address: {error}"
        ))
    })?;
    if id != target.backend_idx()
        || SocketAddr::new(ip, endpoint.port()) != target.endpoint()
        || identity.start_epoch() != target.start_epoch()
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

#[cfg(test)]
mod tests {
    use novarocks_proto::novarocks::{
        IcebergExecutionBindingDeclaration, StarRocksExecutionBindingDeclaration,
        connector_execution_binding_declaration::Provider,
    };
    use novarocks_spi::connector::ConnectorExecutionDeclaration;

    use super::encode_connector_execution_declaration;

    #[test]
    fn encodes_iceberg_declaration_from_the_closed_spi_variant() {
        let declaration =
            ConnectorExecutionDeclaration::iceberg("catalog", [7; 16], "warehouse-binding")
                .unwrap();

        assert_eq!(
            encode_connector_execution_declaration(&declaration),
            novarocks_proto::novarocks::ConnectorExecutionBindingDeclaration {
                instance_id: "catalog".to_string(),
                incarnation: vec![7; 16],
                provider: Some(Provider::Iceberg(IcebergExecutionBindingDeclaration {
                    access_binding: "warehouse-binding".to_string(),
                })),
            }
        );
    }

    #[test]
    fn encodes_starrocks_declaration_from_the_closed_spi_variant() {
        let declaration =
            ConnectorExecutionDeclaration::starrocks("catalog", [9; 16], "local-binding").unwrap();

        assert_eq!(
            encode_connector_execution_declaration(&declaration),
            novarocks_proto::novarocks::ConnectorExecutionBindingDeclaration {
                instance_id: "catalog".to_string(),
                incarnation: vec![9; 16],
                provider: Some(Provider::Starrocks(StarRocksExecutionBindingDeclaration {
                    local_binding: "local-binding".to_string(),
                })),
            }
        );
    }
}
