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

//! gRPC fragment dispatcher adapter.
//!
//! `FragmentDispatcher` decouples coordinator from where fragments actually
//! run. `RemoteDispatcher` talks to one or more BEs over gRPC by index;
//! `FragmentScheduler` chooses which backend each fragment instance lands on.
//! Product execution routes fragments through `RemoteDispatcher`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
#[cfg(test)]
use crate::exec::chunk::Chunk;
#[cfg(test)]
use crate::exec::chunk::ChunkSchema;
use crate::proto::common::UniqueId as ProtoUniqueId;
use crate::proto::novarocks::{
    EnsureConnectorExecutionBindingRequest, FetchResultRequest,
    QueryExecutionId as ProtoQueryExecutionId, RetireConnectorExecutionBindingRequest,
    fetch_result_response::Status as FetchStatus,
};
use crate::query_execution::contract::QueryId;
use crate::query_execution::fragment_transport::{
    FetchOutcome, FetchedQueryBatch, FragmentDispatcher,
};
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::service::grpc_client::NovaRocksGrpcRemoteClient;
#[cfg(test)]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use tracing::warn;

static REMOTE_FETCH_CALLS: AtomicUsize = AtomicUsize::new(0);

/// gRPC adapter for the connector instance control plane. Declarations are
/// installed before a fragment carrier exists, so provider selection never
/// crosses the fragment carrier boundary.
pub(crate) struct GrpcConnectorBindingControl {
    clients: BTreeMap<usize, NovaRocksGrpcRemoteClient>,
    endpoints: BTreeMap<usize, SocketAddr>,
}

impl GrpcConnectorBindingControl {
    pub(crate) fn new(backends: &[(usize, SocketAddr)]) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("GrpcConnectorBindingControl requires at least one backend".to_string());
        }
        let mut clients = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for (backend_idx, endpoint) in backends {
            if clients.contains_key(backend_idx) {
                return Err(format!("duplicate connector binding backend {backend_idx}"));
            }
            clients.insert(
                *backend_idx,
                NovaRocksGrpcRemoteClient::connect_blocking(*endpoint)?,
            );
            endpoints.insert(*backend_idx, *endpoint);
        }
        Ok(Self { clients, endpoints })
    }

    fn client_and_endpoint(
        &self,
        backend_idx: usize,
        endpoint: SocketAddr,
    ) -> Result<&NovaRocksGrpcRemoteClient, String> {
        let configured = self.endpoints.get(&backend_idx).ok_or_else(|| {
            format!("connector binding backend {backend_idx} is absent from configured snapshot")
        })?;
        if *configured != endpoint {
            return Err(format!(
                "connector binding endpoint mismatch for backend {backend_idx}: configured {configured}, received {endpoint}"
            ));
        }
        self.clients
            .get(&backend_idx)
            .ok_or_else(|| format!("connector binding client for backend {backend_idx} is missing"))
    }
}

impl crate::query_execution::artifact::ConnectorBindingDispatcher for GrpcConnectorBindingControl {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        backend_idx: usize,
        endpoint: SocketAddr,
        declaration: &novarocks_spi::connector::ConnectorExecutionDeclaration,
    ) -> Result<(), String> {
        let client = self.client_and_endpoint(backend_idx, endpoint)?;
        let request = EnsureConnectorExecutionBindingRequest {
            execution_id: Some(ProtoQueryExecutionId {
                query_id: Some(ProtoUniqueId {
                    hi: execution_id.query_id().high(),
                    lo: execution_id.query_id().low(),
                }),
                attempt_id: execution_id.attempt_id().get(),
            }),
            provider_id: declaration.descriptor().provider_id.as_str().to_string(),
            instance_id: declaration.descriptor().instance_id.as_str().to_string(),
            incarnation: declaration.incarnation().to_bytes().to_vec(),
            declaration_payload: declaration.payload().to_vec(),
        };
        let response = client
            .blocking_ensure_connector_execution_binding(request)
            .map_err(|error| {
                format!(
                    "connector execution binding ensure RPC failed for BE[{backend_idx}] ({endpoint}): {error}"
                )
            })?;
        if response.status_code != 0 {
            return Err(format!(
                "connector execution binding ensure was rejected by BE[{backend_idx}] ({endpoint}): {}",
                response.message
            ));
        }
        Ok(())
    }

    fn retire(
        &self,
        endpoint: SocketAddr,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<(), String> {
        let client = self
            .endpoints
            .iter()
            .find_map(|(backend_idx, configured)| (*configured == endpoint).then_some(backend_idx))
            .and_then(|backend_idx| self.clients.get(backend_idx))
            .ok_or_else(|| {
                format!(
                    "connector retirement endpoint {endpoint} is absent from configured backend snapshot"
                )
            })?;
        let response = client
            .blocking_retire_connector_execution_binding(RetireConnectorExecutionBindingRequest {
                instance_id: key.instance_id.as_str().to_string(),
                incarnation: key.incarnation.to_bytes().to_vec(),
            })
            .map_err(|error| {
                format!("connector execution binding retire RPC failed for {endpoint}: {error}")
            })?;
        if response.status_code != 0 {
            return Err(format!(
                "connector execution binding retirement was rejected by {endpoint}: {}",
                response.message
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RemoteDispatcher
// ---------------------------------------------------------------------------

pub struct RemoteDispatcher {
    clients: BTreeMap<usize, NovaRocksGrpcRemoteClient>,
    addrs: BTreeMap<usize, std::net::SocketAddr>,
    #[cfg(test)]
    rpc_timeout: Option<Duration>,
}

impl RemoteDispatcher {
    /// Build a `RemoteDispatcher` with one lazy gRPC client per backend.
    ///
    /// Clients are constructed via `connect_blocking`, which is lazy and cheap
    /// (no TCP dial at construction). Errors if `backends` is empty.
    pub fn new(backends: &[SocketAddr]) -> Result<Self, String> {
        let entries = backends
            .iter()
            .copied()
            .enumerate()
            .collect::<Vec<(usize, SocketAddr)>>();
        Self::new_with_backend_ids(&entries)
    }

    pub fn new_with_backend_ids(backends: &[(usize, SocketAddr)]) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("RemoteDispatcher requires at least one backend".to_string());
        }
        let mut clients = BTreeMap::new();
        let mut addrs = BTreeMap::new();
        for (backend_id, addr) in backends {
            if clients.contains_key(backend_id) {
                return Err(format!("duplicate backend_idx {backend_id}"));
            }
            clients.insert(
                *backend_id,
                NovaRocksGrpcRemoteClient::connect_blocking(*addr)?,
            );
            addrs.insert(*backend_id, *addr);
        }
        Ok(Self {
            clients,
            addrs,
            #[cfg(test)]
            rpc_timeout: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_backend_ids_and_rpc_timeout_for_test(
        backends: &[(usize, SocketAddr)],
        rpc_timeout: Duration,
    ) -> Result<Self, String> {
        if rpc_timeout.is_zero() || rpc_timeout > Duration::from_secs(5) {
            return Err("test fragment RPC timeout must be within (0, 5s]".to_string());
        }
        let mut dispatcher = Self::new_with_backend_ids(backends)?;
        dispatcher.rpc_timeout = Some(rpc_timeout);
        Ok(dispatcher)
    }

    /// The address of `backend_idx`/backend id, if present.
    pub fn addr_of(&self, backend_idx: usize) -> Option<SocketAddr> {
        self.addrs.get(&backend_idx).copied()
    }

    fn check_idx(&self, idx: usize) -> Result<(), String> {
        if !self.clients.contains_key(&idx) {
            return Err(format!(
                "backend_idx {} out of range (have {} backends)",
                idx,
                self.clients.len()
            ));
        }
        Ok(())
    }

    fn client_and_addr(
        &self,
        idx: usize,
    ) -> Result<(&NovaRocksGrpcRemoteClient, SocketAddr), String> {
        self.check_idx(idx)?;
        Ok((
            self.clients
                .get(&idx)
                .expect("client exists after check_idx"),
            *self.addrs.get(&idx).expect("addr exists after check_idx"),
        ))
    }
}

impl FragmentDispatcher for RemoteDispatcher {
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected_output_schema: Option<
            crate::query_execution::fragment_transport::ExpectedOutputSchemaView<'_>,
        >,
    ) -> Result<FetchOutcome, String> {
        let (client, addr) = self.client_and_addr(backend_idx)?;
        // Counter increments only after a successful check_idx, so only valid-index
        // calls are counted — matches the fault-injection test assumptions.
        let call_index = REMOTE_FETCH_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            println!("NOVAROCKS_FETCH_NOT_READY call={call_index}");
            let _ = std::io::Write::flush(&mut std::io::stdout());
            return Ok(FetchOutcome::NotReady);
        }
        let request = FetchResultRequest {
            finst_id: Some(ProtoUniqueId {
                hi: finst_id.high(),
                lo: finst_id.low(),
            }),
            max_wait_ms,
        };
        #[cfg(test)]
        let resp = if let Some(timeout) = self.rpc_timeout {
            client.blocking_fetch_result_with_timeout(request, timeout)
        } else {
            client.blocking_fetch_result(request)
        };
        #[cfg(not(test))]
        let resp = client.blocking_fetch_result(request);
        let resp = resp.map_err(|e| format!("BE[{backend_idx}] ({addr}): {e}"))?;
        let status = FetchStatus::try_from(resp.status).map_err(|_| {
            format!(
                "BE[{backend_idx}] ({}): remote fetch_result returned unknown status {}",
                addr, resp.status
            )
        })?;
        match status {
            FetchStatus::Ready => {
                if resp.eos {
                    return Ok(FetchOutcome::Eof);
                }
                if resp.result_arrow_ipc.is_empty() {
                    return Err(format!(
                        "BE[{backend_idx}] ({addr}): fetch_result READY without result_arrow_ipc"
                    ));
                }
                let mut chunks = crate::runtime::exchange::decode_root_result_chunks(
                    &resp.result_arrow_ipc,
                    expected_output_schema.map(|view| view.chunk_schema()),
                )?;
                if chunks.len() != 1 {
                    return Err(format!(
                        "BE[{backend_idx}] ({addr}): typed fetch_result decoded {} chunks, expected 1",
                        chunks.len()
                    ));
                }
                let chunk = chunks.remove(0);
                Ok(FetchOutcome::Ready(FetchedQueryBatch::new(chunk)))
            }
            FetchStatus::NotReady => Ok(FetchOutcome::NotReady),
            FetchStatus::Eof => Ok(FetchOutcome::Eof),
            FetchStatus::Error => Ok(FetchOutcome::Err(resp.message)),
            FetchStatus::ResultStatusUnspecified => Err(format!(
                "BE[{backend_idx}] ({addr}): remote fetch_result returned unspecified status"
            )),
        }
    }

    fn backend_count(&self) -> usize {
        self.clients.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    use crate::proto;
    use crate::query_execution::contract::QueryId;
    use arrow::array::Int32Array;
    use proto::filter::{LookupRequest, LookupResponse};
    use proto::novarocks::fetch_result_response::Status as FetchStatus;
    use proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc;
    use proto::novarocks::{
        EnsureConnectorExecutionBindingRequest, EnsureConnectorExecutionBindingResponse,
        ExchangeRequest, ExchangeResponse, FetchResultRequest, FetchResultResponse,
        HeartbeatRequest, HeartbeatResponse, ReportQueryTerminalRequest,
        ReportQueryTerminalResponse, RetireConnectorExecutionBindingRequest,
        RetireConnectorExecutionBindingResponse,
    };
    use tonic::{Request, Response, Status, Streaming};

    fn make_finst_id(hi: i64, lo: i64) -> UniqueId {
        UniqueId::new(hi, lo)
    }

    #[derive(Clone)]
    struct MockGrpc(Arc<MockState>);

    struct MockState {
        fetch_status: AtomicI32,
        fetch_eos: AtomicBool,
        fetch_arrow: Mutex<Vec<u8>>,
    }

    impl Default for MockState {
        fn default() -> Self {
            Self {
                fetch_status: AtomicI32::new(FetchStatus::Eof as i32),
                fetch_eos: AtomicBool::new(false),
                fetch_arrow: Mutex::new(Vec::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl NovaRocksGrpc for MockGrpc {
        type ExchangeStream =
            Pin<Box<dyn tokio_stream::Stream<Item = Result<ExchangeResponse, Status>> + Send>>;
        type QueryControlStreamStream = Pin<
            Box<
                dyn tokio_stream::Stream<
                        Item = Result<proto::novarocks::QueryControlResponse, Status>,
                    > + Send,
            >,
        >;

        async fn exchange(
            &self,
            _request: Request<Streaming<ExchangeRequest>>,
        ) -> Result<Response<Self::ExchangeStream>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn exchange_unary(
            &self,
            _request: Request<ExchangeRequest>,
        ) -> Result<Response<ExchangeResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn transmit_runtime_filter_envelope(
            &self,
            _request: Request<proto::filter::RuntimeFilterEnvelope>,
        ) -> Result<Response<proto::filter::RuntimeFilterEnvelopeResponse>, Status> {
            Err(Status::unimplemented(
                "runtime filter envelope outbound transport is not implemented",
            ))
        }

        async fn lookup(
            &self,
            _request: Request<LookupRequest>,
        ) -> Result<Response<LookupResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn fetch_result(
            &self,
            _request: Request<FetchResultRequest>,
        ) -> Result<Response<FetchResultResponse>, Status> {
            Ok(Response::new(FetchResultResponse {
                status: self.0.fetch_status.load(Ordering::SeqCst),
                message: "fetch failed".to_string(),
                packet_seq: 0,
                eos: self.0.fetch_eos.load(Ordering::SeqCst),
                result_arrow_ipc: self.0.fetch_arrow.lock().expect("fetch arrow lock").clone(),
            }))
        }

        async fn init_query(
            &self,
            _request: Request<proto::novarocks::InitQueryRequest>,
        ) -> Result<Response<proto::novarocks::InitQueryResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn stage_fragments(
            &self,
            _request: Request<proto::novarocks::StageFragmentsRequest>,
        ) -> Result<Response<proto::novarocks::StageFragmentsResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn start_prepared_query(
            &self,
            _request: Request<proto::novarocks::StartPreparedQueryRequest>,
        ) -> Result<Response<proto::novarocks::StartPreparedQueryResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn abort_query(
            &self,
            _request: Request<proto::novarocks::AbortQueryRequest>,
        ) -> Result<Response<proto::novarocks::AbortQueryResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn report_query_terminal(
            &self,
            _request: Request<ReportQueryTerminalRequest>,
        ) -> Result<Response<ReportQueryTerminalResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn query_control_stream(
            &self,
            _request: Request<Streaming<proto::novarocks::QueryControlRequest>>,
        ) -> Result<Response<Self::QueryControlStreamStream>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn ensure_connector_execution_binding(
            &self,
            _request: Request<EnsureConnectorExecutionBindingRequest>,
        ) -> Result<Response<EnsureConnectorExecutionBindingResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn retire_connector_execution_binding(
            &self,
            _request: Request<RetireConnectorExecutionBindingRequest>,
        ) -> Result<Response<RetireConnectorExecutionBindingResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn heartbeat(
            &self,
            _request: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            Ok(Response::new(HeartbeatResponse {
                start_epoch: 1,
                version: "test".into(),
                num_cores: 1,
                status_code: 0,
            }))
        }
    }

    fn spawn_mock_server(state: Arc<MockState>) -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock server local addr");
        let mock = MockGrpc(Arc::clone(&state));
        crate::runtime::global_async_runtime::data_block_on(async move {
            listener
                .set_nonblocking(true)
                .expect("set mock server nonblocking");
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            let incoming = futures::stream::unfold(listener, |listener| async {
                let item = listener.accept().await.map(|(stream, _)| stream);
                Some((item, listener))
            });
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(
                        proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(mock),
                    )
                    .serve_with_incoming(incoming),
            );
        })
        .expect("spawn mock server");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(50))
                .is_ok()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "mock grpc server did not become ready at {addr}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        addr
    }

    #[test]
    fn remote_dispatcher_fetch_eof_returns_eof() {
        let state = Arc::new(MockState::default());
        state
            .fetch_status
            .store(FetchStatus::Eof as i32, Ordering::SeqCst);
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        let outcome = dispatcher
            .fetch_result(0, make_finst_id(1, 2), 0, None)
            .expect("fetch");

        assert!(matches!(outcome, FetchOutcome::Eof));
    }

    #[test]
    fn remote_dispatcher_fetch_ready_decodes_typed_payload() {
        let state = Arc::new(MockState::default());
        state
            .fetch_status
            .store(FetchStatus::Ready as i32, Ordering::SeqCst);
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .expect("typed batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(7)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        *state.fetch_arrow.lock().expect("fetch arrow lock") =
            crate::runtime::exchange::encode_chunks(&[chunk], true).expect("encode typed result");
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        let outcome = dispatcher
            .fetch_result(0, make_finst_id(1, 2), 0, None)
            .expect("fetch");

        let FetchOutcome::Ready(batch) = outcome else {
            panic!("expected ready chunk");
        };
        let chunk = batch.into_chunk();
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(chunk.len(), 1);
        assert_eq!(chunk.columns()[0].data_type(), &DataType::Int32);
    }

    #[test]
    fn remote_dispatcher_fetch_ready_eos_returns_eof() {
        let state = Arc::new(MockState::default());
        state
            .fetch_status
            .store(FetchStatus::Ready as i32, Ordering::SeqCst);
        state.fetch_eos.store(true, Ordering::SeqCst);
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        let outcome = dispatcher
            .fetch_result(0, make_finst_id(1, 2), 0, None)
            .expect("fetch");

        assert!(matches!(outcome, FetchOutcome::Eof));
    }

    #[test]
    fn remote_dispatcher_fetch_ready_requires_typed_payload() {
        let state = Arc::new(MockState::default());
        state
            .fetch_status
            .store(FetchStatus::Ready as i32, Ordering::SeqCst);
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        let err = match dispatcher.fetch_result(0, make_finst_id(1, 2), 0, None) {
            Ok(_) => panic!("missing typed payload must fail"),
            Err(err) => err,
        };

        assert!(err.contains("result_arrow_ipc"), "{err}");
    }

    #[test]
    fn remote_dispatcher_holds_multiple_clients() {
        let a1 = spawn_mock_server(Arc::new(MockState::default()));
        let a2 = spawn_mock_server(Arc::new(MockState::default()));
        let d = RemoteDispatcher::new(&[a1, a2]).expect("construct");
        assert_eq!(d.backend_count(), 2);
    }

    #[test]
    fn remote_dispatcher_can_route_sparse_backend_ids() {
        let a = spawn_mock_server(Arc::new(MockState::default()));
        let d = RemoteDispatcher::new_with_backend_ids(&[(2, a)]).expect("construct");
        assert_eq!(d.backend_count(), 1);
        assert_eq!(d.addr_of(2), Some(a));
        assert_eq!(d.addr_of(0), None);
    }
}
