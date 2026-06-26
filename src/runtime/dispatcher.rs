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

//! Fragment dispatcher abstraction.
//!
//! `FragmentDispatcher` decouples coordinator from where fragments actually
//! run. `RemoteDispatcher` talks to one or more BEs over gRPC by index;
//! `FragmentScheduler` chooses which backend each fragment instance lands on.
//! Product execution routes fragments through `RemoteDispatcher`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use thrift::protocol::{TBinaryOutputProtocol, TSerializable};
use thrift::transport::{TBufferChannel, TIoChannel};

#[cfg(test)]
use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
#[cfg(test)]
use crate::exec::chunk::ChunkSchema;
use crate::exec::chunk::ChunkSchemaRef;
use crate::proto::novarocks::{
    CancelFragmentRequest, FetchResultRequest, PUniqueId, SubmitFragmentRequest,
    fetch_result_response::Status as FetchStatus,
};
use crate::service::grpc_client::NovaRocksGrpcRemoteClient;
#[cfg(test)]
use crate::thrift::data_sinks;
use crate::thrift::internal_service;
use crate::thrift::types;
use tracing::warn;

static REMOTE_SUBMIT_CALLS: AtomicUsize = AtomicUsize::new(0);
static REMOTE_FETCH_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Outcome of a single `fetch_result` call.
pub enum FetchOutcome {
    /// A result chunk is available.
    Ready(Chunk),
    /// No chunk available yet; fragment is still running.
    NotReady,
    /// All chunks have been delivered; the root fragment is complete.
    Eof,
    /// Fragment execution failed.
    Err(String),
}

/// Fragment dispatcher trait.
///
/// Implementations choose where and how fragments run.  The coordinator
/// calls `submit_fragment` for each fragment (non-blocking), then polls
/// `fetch_result` for the root fragment instance until `Eof` or `Err`.
pub trait FragmentDispatcher: Send + Sync + 'static {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;

    /// Submit a fragment for asynchronous execution to the given backend.
    /// Returns immediately.
    fn submit_fragment(
        &self,
        backend_idx: usize,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String>;

    /// Poll for the next result chunk from the root fragment on the given
    /// backend.
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: types::TUniqueId,
        max_wait_ms: i64,
        expected_chunk_schema: Option<&ChunkSchemaRef>,
    ) -> Result<FetchOutcome, String>;

    /// Cancel all listed fragment instances on the given backend.  Idempotent.
    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[types::TUniqueId]);

    /// Number of backends this dispatcher can route to.
    fn backend_count(&self) -> usize;

    /// Whether non-write fragments need final status reports back to the
    /// standalone coordinator.
    fn needs_fragment_status_report(&self) -> bool {
        false
    }
}

/// Return the pipeline DOP for standalone distributed execution.
///
/// Unified with the FE-compatible path: delegates to `exec_env::calc_pipeline_dop`, which honors a
/// positive `session_dop` override (from `SET pipeline_dop = N`) and otherwise auto-derives
/// cores/2 (= half the executor threads). This replaced the former hardcoded `min(cores, 4)` cap
/// that left most cores idle on machines with >8 cores (e.g. TPC-H q18's CPU-bound partitioned
/// joins ran only 4 build + 4 probe drivers on a 10-core box). The cores/2 headroom is deliberate —
/// it leaves cores for scan threads, the exchange IO pool, and the gRPC server. Pass 0 for auto.
pub(crate) fn compute_pipeline_dop(session_dop: i32) -> i32 {
    crate::runtime::exec_env::calc_pipeline_dop(session_dop)
}

/// Pipeline DOP for a standalone fragment whose output is a write/data sink (load/insert/export).
/// Delegates to `exec_env::calc_sink_pipeline_dop` (StarRocks `getSinkDefaultDOP`): a positive
/// `session_dop` override wins; otherwise the lower sink curve (cores/3, or min(32, cores/4) above
/// 24 cores) so writes don't starve query CPU. Compute fragments keep `compute_pipeline_dop`.
pub(crate) fn compute_sink_pipeline_dop(session_dop: i32) -> i32 {
    crate::runtime::exec_env::calc_sink_pipeline_dop(session_dop)
}

fn serialize_thrift_binary<T: TSerializable>(value: &T) -> Result<Vec<u8>, String> {
    const INITIAL_CAPACITY: usize = 256;
    const MAX_CAPACITY: usize = 64 * 1024 * 1024;

    let mut capacity = INITIAL_CAPACITY;
    loop {
        let channel = TBufferChannel::with_capacity(0, capacity);
        let (_, w) = channel.split().map_err(|e| e.to_string())?;
        let mut protocol = TBinaryOutputProtocol::new(w, true);
        match value.write_to_out_protocol(&mut protocol) {
            Ok(()) => return Ok(protocol.transport.write_bytes()),
            Err(e) => {
                if capacity >= MAX_CAPACITY {
                    return Err(e.to_string());
                }
                capacity = capacity.saturating_mul(2).min(MAX_CAPACITY);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RemoteDispatcher
// ---------------------------------------------------------------------------

pub struct RemoteDispatcher {
    clients: BTreeMap<usize, NovaRocksGrpcRemoteClient>,
    addrs: BTreeMap<usize, std::net::SocketAddr>,
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
        Ok(Self { clients, addrs })
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
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn submit_fragment(
        &self,
        backend_idx: usize,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String> {
        let (client, addr) = self.client_and_addr(backend_idx)?;
        // Counter increments only after a successful check_idx, so only valid-index
        // calls are counted — matches the fault-injection test assumptions.
        let call_index = REMOTE_SUBMIT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_fault_inject_submit_fail_after()
            .is_some_and(|successes| call_index > successes)
        {
            println!("NOVAROCKS_SUBMIT_FAIL call={call_index}");
            let _ = std::io::Write::flush(&mut std::io::stdout());
            return Err(format!("debug submit fault injected on call {call_index}"));
        }
        let payload = serialize_thrift_binary(&params)
            .map_err(|e| format!("serialize fragment params for remote submit failed: {e}"))?;
        let resp = client
            .blocking_submit_fragment(SubmitFragmentRequest {
                exec_plan_fragment_params_thrift: payload,
            })
            .map_err(|e| format!("BE[{backend_idx}] ({addr}): {e}"))?;
        if resp.status_code != 0 {
            return Err(format!(
                "remote submit_fragment failed on {}: {}",
                addr, resp.message
            ));
        }
        Ok(())
    }

    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: types::TUniqueId,
        max_wait_ms: i64,
        expected_chunk_schema: Option<&ChunkSchemaRef>,
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
        let resp = client
            .blocking_fetch_result(FetchResultRequest {
                finst_id: Some(PUniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                }),
                max_wait_ms,
                typed_result: true,
            })
            .map_err(|e| format!("BE[{backend_idx}] ({addr}): {e}"))?;
        let status = FetchStatus::try_from(resp.status).map_err(|_| {
            format!(
                "BE[{backend_idx}] ({}): remote fetch_result returned unknown status {}",
                addr, resp.status
            )
        })?;
        match status {
            FetchStatus::Ready => {
                if resp.result_arrow_ipc.is_empty() {
                    return Err(format!(
                        "BE[{backend_idx}] ({addr}): typed fetch_result READY without result_arrow_ipc"
                    ));
                }
                let mut chunks = crate::runtime::exchange::decode_root_result_chunks(
                    &resp.result_arrow_ipc,
                    expected_chunk_schema,
                )?;
                if chunks.len() != 1 {
                    return Err(format!(
                        "BE[{backend_idx}] ({addr}): typed fetch_result decoded {} chunks, expected 1",
                        chunks.len()
                    ));
                }
                let chunk = chunks.remove(0);
                Ok(FetchOutcome::Ready(chunk))
            }
            FetchStatus::NotReady => Ok(FetchOutcome::NotReady),
            FetchStatus::Eof => Ok(FetchOutcome::Eof),
            FetchStatus::Error => Ok(FetchOutcome::Err(resp.message)),
        }
    }

    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[types::TUniqueId]) {
        if self.check_idx(backend_idx).is_err() {
            return;
        }
        let addr = self.addrs[&backend_idx];
        let req = CancelFragmentRequest {
            finst_ids: finst_ids
                .iter()
                .map(|id| PUniqueId {
                    hi: id.hi,
                    lo: id.lo,
                })
                .collect(),
            reason: "coordinator cancel".to_string(),
            start_epoch: 0,
        };
        let runtime_handle = match crate::runtime::global_async_runtime::data_runtime_handle() {
            Ok(handle) => handle,
            Err(e) => {
                warn!(
                    "remote cancel_fragment runtime unavailable for {}: {}",
                    addr, e
                );
                return;
            }
        };
        runtime_handle.spawn(async move {
            match NovaRocksGrpcRemoteClient::new(addr) {
                Ok(client) => match client.cancel_fragment_async(req).await {
                    Ok(resp) if resp.status_code == 0 => {}
                    Ok(resp) => warn!(
                        "remote cancel_fragment returned nonzero status from {}: {}",
                        addr, resp.status_code
                    ),
                    Err(e) => warn!("remote cancel_fragment failed for {}: {}", addr, e),
                },
                Err(e) => warn!("remote cancel_fragment failed for {}: {}", addr, e),
            }
        });
    }

    fn backend_count(&self) -> usize {
        self.clients.len()
    }

    fn needs_fragment_status_report(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};

    use crate::proto;
    use arrow::array::Int32Array;
    use proto::novarocks::fetch_result_response::Status as FetchStatus;
    use proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc;
    use proto::novarocks::{
        BatchReportExecStatusRequest, BatchReportExecStatusResponse, CancelFragmentRequest,
        ExchangeRequest, ExchangeResponse, FetchResultRequest, FetchResultResponse,
        HeartbeatRequest, HeartbeatResponse, ReportExecStatusRequest, ReportExecStatusResponse,
        SubmitFragmentRequest, SubmitFragmentResponse,
    };
    use proto::starrocks::{
        PLookUpRequest, PLookUpResponse, PTransmitRuntimeFilterParams, PTransmitRuntimeFilterResult,
    };
    use tonic::{Request, Response, Status, Streaming};

    fn make_finst_id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
    }

    fn make_empty_exec_params(hi: i64, lo: i64) -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams {
            query_id: types::TUniqueId::new(hi, lo),
            fragment_instance_id: types::TUniqueId::new(hi, lo),
            per_node_scan_ranges: Default::default(),
            per_exch_num_senders: Default::default(),
            destinations: None,
            sender_id: None,
            num_senders: None,
            send_query_statistics_with_every_batch: None,
            use_vectorized: None,
            runtime_filter_params: None,
            instances_number: None,
            enable_exchange_pass_through: None,
            node_to_per_driver_seq_scan_ranges: None,
            enable_exchange_perf: None,
            pipeline_sink_dop: None,
            report_when_finish: None,
            exec_debug_options: None,
        }
    }

    /// Build a minimal TExecPlanFragmentParams with a non-result (NOOP) sink.
    fn make_noop_sink_params(hi: i64, lo: i64) -> internal_service::TExecPlanFragmentParams {
        use crate::thrift::partitions;
        let noop_sink = data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::NOOP_SINK,
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            None::<data_sinks::TIcebergTableSink>,
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
        );
        let fragment = crate::thrift::planner::TPlanFragment::new(
            None::<crate::thrift::plan_nodes::TPlan>,
            None::<Vec<crate::thrift::exprs::TExpr>>,
            Some(noop_sink),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::thrift::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            None::<i64>,
            None::<i64>,
            None::<Vec<crate::thrift::data::TGlobalDict>>,
            None::<Vec<crate::thrift::data::TGlobalDict>>,
            None::<crate::thrift::planner::TCacheParam>,
            None::<std::collections::BTreeMap<i32, crate::thrift::exprs::TExpr>>,
            None::<crate::thrift::planner::TGroupExecutionParam>,
        );
        internal_service::TExecPlanFragmentParams::new(
            internal_service::InternalServiceVersion::V1,
            Some(fragment),
            None::<crate::thrift::descriptors::TDescriptorTable>,
            Some(make_empty_exec_params(hi, lo)),
            None::<types::TNetworkAddress>,
            None::<i32>,
            None::<internal_service::TQueryGlobals>,
            None::<internal_service::TQueryOptions>,
            None::<bool>,
            None::<types::TResourceInfo>,
            None::<String>,
            None::<String>,
            None::<i64>,
            None::<internal_service::TLoadErrorHubInfo>,
            None::<bool>,
            None::<i32>,
            None::<std::collections::BTreeMap<types::TPlanNodeId, i32>>,
            None::<crate::thrift::work_group::TWorkGroup>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<internal_service::TAdaptiveDopParam>,
            None::<i32>,
            None::<internal_service::TPredicateTreeParams>,
            None::<Vec<i32>>,
            None::<i32>,
            None::<types::TNetworkAddress>,
            None::<bool>,
        )
    }

    #[derive(Clone)]
    struct MockGrpc(Arc<MockState>);

    struct MockState {
        submit_code: AtomicI32,
        fetch_status: AtomicI32,
        fetch_arrow: Mutex<Vec<u8>>,
        last_fetch_typed_result: AtomicBool,
        cancel_count: AtomicUsize,
        cancel_delay_ms: AtomicU64,
        report_status_code: AtomicI32,
        report_message: Mutex<String>,
    }

    impl Default for MockState {
        fn default() -> Self {
            Self {
                submit_code: AtomicI32::new(0),
                fetch_status: AtomicI32::new(FetchStatus::Eof as i32),
                fetch_arrow: Mutex::new(Vec::new()),
                last_fetch_typed_result: AtomicBool::new(false),
                cancel_count: AtomicUsize::new(0),
                cancel_delay_ms: AtomicU64::new(0),
                report_status_code: AtomicI32::new(0),
                report_message: Mutex::new(String::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl NovaRocksGrpc for MockGrpc {
        type ExchangeStream =
            Pin<Box<dyn tokio_stream::Stream<Item = Result<ExchangeResponse, Status>> + Send>>;

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

        async fn transmit_runtime_filter(
            &self,
            _request: Request<PTransmitRuntimeFilterParams>,
        ) -> Result<Response<PTransmitRuntimeFilterResult>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn lookup(
            &self,
            _request: Request<PLookUpRequest>,
        ) -> Result<Response<PLookUpResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn submit_fragment(
            &self,
            _request: Request<SubmitFragmentRequest>,
        ) -> Result<Response<SubmitFragmentResponse>, Status> {
            Ok(Response::new(SubmitFragmentResponse {
                status_code: self.0.submit_code.load(Ordering::SeqCst),
                message: "submit failed".to_string(),
            }))
        }

        async fn fetch_result(
            &self,
            request: Request<FetchResultRequest>,
        ) -> Result<Response<FetchResultResponse>, Status> {
            self.0
                .last_fetch_typed_result
                .store(request.into_inner().typed_result, Ordering::SeqCst);
            Ok(Response::new(FetchResultResponse {
                status: self.0.fetch_status.load(Ordering::SeqCst),
                result_batch_thrift: Vec::new(),
                result_arrow_ipc: self.0.fetch_arrow.lock().expect("fetch arrow lock").clone(),
                message: "fetch failed".to_string(),
            }))
        }

        async fn cancel_fragment(
            &self,
            _request: Request<CancelFragmentRequest>,
        ) -> Result<Response<proto::novarocks::CancelFragmentResponse>, Status> {
            let delay_ms = self.0.cancel_delay_ms.load(Ordering::SeqCst);
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            self.0.cancel_count.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(proto::novarocks::CancelFragmentResponse {
                status_code: 0,
            }))
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

        async fn report_exec_status(
            &self,
            _request: Request<ReportExecStatusRequest>,
        ) -> Result<Response<ReportExecStatusResponse>, Status> {
            Ok(Response::new(ReportExecStatusResponse {
                status_code: self.0.report_status_code.load(Ordering::SeqCst),
                message: self
                    .0
                    .report_message
                    .lock()
                    .expect("report message lock")
                    .clone(),
                error_code: String::new(),
            }))
        }

        async fn batch_report_exec_status(
            &self,
            _request: Request<BatchReportExecStatusRequest>,
        ) -> Result<Response<BatchReportExecStatusResponse>, Status> {
            Ok(Response::new(BatchReportExecStatusResponse {
                status_code: self.0.report_status_code.load(Ordering::SeqCst),
                message: self
                    .0
                    .report_message
                    .lock()
                    .expect("report message lock")
                    .clone(),
                error_code: String::new(),
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
    fn remote_dispatcher_submit_nonzero_status_returns_err() {
        let state = Arc::new(MockState::default());
        state.submit_code.store(1, Ordering::SeqCst);
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        let err = dispatcher
            .submit_fragment(0, make_noop_sink_params(1, 2))
            .expect_err("nonzero submit status should error");

        assert!(err.contains("submit failed"));
    }

    #[test]
    fn grpc_client_report_exec_status_preserves_business_error() {
        let state = Arc::new(MockState::default());
        state.report_status_code.store(7, Ordering::SeqCst);
        *state.report_message.lock().expect("report message lock") = "report failed".to_string();
        let addr = spawn_mock_server(Arc::clone(&state));
        let client =
            NovaRocksGrpcRemoteClient::connect_blocking(addr).expect("construct grpc client");

        let resp = client
            .blocking_report_exec_status(ReportExecStatusRequest {
                report_exec_status_params_thrift: vec![0xff],
            })
            .expect("RPC level success");

        assert_ne!(resp.status_code, 0);
        assert!(!resp.message.is_empty());
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

        let FetchOutcome::Ready(chunk) = outcome else {
            panic!("expected ready chunk");
        };
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(chunk.len(), 1);
        assert_eq!(chunk.columns()[0].data_type(), &DataType::Int32);
        assert!(state.last_fetch_typed_result.load(Ordering::SeqCst));
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
    fn remote_dispatcher_cancel_is_sent() {
        let state = Arc::new(MockState::default());
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        dispatcher.cancel_fragments(0, &[make_finst_id(1, 2)]);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while state.cancel_count.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "cancel rpc was not observed by mock server"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn remote_dispatcher_cancel_returns_promptly_when_rpc_blocks() {
        let state = Arc::new(MockState::default());
        state.cancel_delay_ms.store(1_000, Ordering::SeqCst);
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(&[addr]).expect("construct");

        let start = std::time::Instant::now();
        dispatcher.cancel_fragments(0, &[make_finst_id(7, 8)]);
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "cancel_fragments should return promptly, took {elapsed:?}"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while state.cancel_count.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "cancel rpc was not observed by mock server"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn remote_dispatcher_source_has_no_native_cancel_thread_spawn() {
        let source = include_str!("dispatcher.rs");
        let needle = ["remote", "-cancel", "-fragment"].concat();
        assert!(
            !source.contains(&needle),
            "remote cancel should not use a dedicated native thread"
        );
    }

    #[test]
    fn remote_dispatcher_cancel_async_path_does_not_call_connect_blocking() {
        let source = include_str!("dispatcher.rs");
        let impl_source = source
            .split_once("// Tests")
            .map(|(before, _)| before)
            .expect("dispatcher tests section exists");
        let cancel_start = source
            .find(
                "fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[types::TUniqueId]) {",
            )
            .expect("cancel_fragments implementation exists");
        let cancel_tail = &impl_source[cancel_start..];
        assert!(
            !cancel_tail.contains("NovaRocksGrpcRemoteClient::connect_blocking(backend)"),
            "async remote cancel path must not call connect_blocking"
        );
    }

    #[test]
    fn dispatcher_source_has_no_local_dispatcher_legacy() {
        let source = include_str!("dispatcher.rs");
        let legacy_type_name = ["In", "Process", "Dispatcher"].concat();
        assert!(
            !source.contains(&legacy_type_name),
            "dispatcher should not keep a local-only dispatcher implementation"
        );
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

    #[test]
    fn remote_dispatcher_returns_err_on_out_of_range_idx() {
        let a = spawn_mock_server(Arc::new(MockState::default()));
        let d = RemoteDispatcher::new(&[a]).expect("construct");
        let err = d
            .submit_fragment(5, make_noop_sink_params(1, 2))
            .expect_err("oob idx");
        assert!(err.contains("backend_idx") && err.contains('5'));
    }

    #[test]
    fn compute_pipeline_dop_is_unified_with_calc_pipeline_dop() {
        // Auto mode (session_dop = 0): no more hardcoded `min(cores, 4)` cap — must equal the shared
        // cores/2 derivation used by the FE-compatible path.
        let dop = compute_pipeline_dop(0);
        assert!(dop > 0, "dop must be positive, got {dop}");
        assert_eq!(
            dop,
            crate::runtime::exec_env::calc_pipeline_dop(0),
            "standalone DOP must delegate to exec_env::calc_pipeline_dop"
        );
        // A positive session override (SET pipeline_dop = N) is honored verbatim.
        assert_eq!(compute_pipeline_dop(7), 7);
        assert_eq!(compute_pipeline_dop(1), 1);
    }

    #[test]
    fn compute_sink_pipeline_dop_is_lower_and_honors_override() {
        // Auto mode: the sink curve is never higher than the compute curve (cores/3..4 vs cores/2),
        // so write fragments don't starve query CPU.
        let sink = compute_sink_pipeline_dop(0);
        let compute = compute_pipeline_dop(0);
        assert!(sink > 0, "sink dop must be positive, got {sink}");
        assert!(
            sink <= compute,
            "sink dop ({sink}) must be <= compute dop ({compute})"
        );
        // A positive session override pins the sink DOP too.
        assert_eq!(compute_sink_pipeline_dop(7), 7);
    }
}
