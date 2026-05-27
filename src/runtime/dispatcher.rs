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
//! run. `InProcessDispatcher` keeps the all-in-one mode using
//! `std::thread::spawn`; `RemoteDispatcher` (PR-4) will talk to a remote BE
//! over gRPC.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, BinaryBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use thrift::protocol::{TBinaryOutputProtocol, TSerializable};
use thrift::transport::{TBufferChannel, TIoChannel};

use crate::common::ids::SlotId;
use crate::common::thrift::thrift_binary_deserialize;
use crate::exec::chunk::Chunk;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::internal_service;
use crate::lower::layout::{build_tuple_slot_order, reorder_tuple_slots};
use crate::lower::thrift::lower_plan;
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_state::RuntimeState;
use crate::service::grpc_client::NovaRocksGrpcRemoteClient;
use crate::service::grpc_proto::novarocks::{
    CancelFragmentRequest, FetchResultRequest, PUniqueId, SubmitFragmentRequest,
    fetch_result_response::Status as FetchStatus,
};
use crate::{data_sinks, types};
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
    /// The gRPC exchange address that fragment sinks must route data to.
    ///
    /// The coordinator embeds this address into `TPlanFragmentDestination`
    /// entries so CTE and stream producers know where to push exchange data.
    fn exchange_addr(&self) -> types::TNetworkAddress;

    /// Submit a fragment for asynchronous execution.  Returns immediately.
    fn submit_fragment(
        &self,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String>;

    /// Poll for the next result chunk from the root fragment.
    fn fetch_result(
        &self,
        finst_id: types::TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String>;

    /// Cancel all listed fragment instances.  Idempotent.
    fn cancel_fragments(&self, finst_ids: &[types::TUniqueId]);
}

/// Return a reasonable pipeline DOP for standalone execution.
pub(crate) fn compute_pipeline_dop() -> i32 {
    std::thread::available_parallelism()
        .map(|p| p.get().min(4))
        .unwrap_or(4) as i32
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

fn empty_chunk() -> Result<Chunk, String> {
    Chunk::try_new_with_chunk_schema(
        RecordBatch::new_empty(Arc::new(Schema::empty())),
        Arc::new(ChunkSchema::empty()),
    )
}

fn read_lenenc_len(row: &[u8], cursor: &mut usize) -> Result<Option<usize>, String> {
    let marker = *row
        .get(*cursor)
        .ok_or_else(|| "lenenc field missing marker byte".to_string())?;
    *cursor += 1;
    match marker {
        0xfb => Ok(None),
        n if n < 0xfb => Ok(Some(n as usize)),
        0xfc => {
            let bytes = row
                .get(*cursor..*cursor + 2)
                .ok_or_else(|| "lenenc field truncated reading 2-byte length".to_string())?;
            *cursor += 2;
            Ok(Some(u16::from_le_bytes([bytes[0], bytes[1]]) as usize))
        }
        0xfd => {
            let bytes = row
                .get(*cursor..*cursor + 3)
                .ok_or_else(|| "lenenc field truncated reading 3-byte length".to_string())?;
            *cursor += 3;
            Ok(Some(
                (bytes[0] as usize) | ((bytes[1] as usize) << 8) | ((bytes[2] as usize) << 16),
            ))
        }
        0xfe => {
            let bytes = row
                .get(*cursor..*cursor + 8)
                .ok_or_else(|| "lenenc field truncated reading 8-byte length".to_string())?;
            *cursor += 8;
            let len = u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            usize::try_from(len)
                .map(Some)
                .map_err(|_| format!("lenenc field length {len} does not fit in usize"))
        }
        0xff => Err("invalid lenenc marker 0xff".to_string()),
        _ => Err(format!("invalid lenenc marker {marker:#x}")),
    }
}

fn parse_all_lenenc_fields(row: &[u8]) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut fields = Vec::new();
    let mut cursor = 0usize;
    while cursor < row.len() {
        let len = read_lenenc_len(row, &mut cursor)?;
        let Some(len) = len else {
            fields.push(None);
            continue;
        };
        let value = row.get(cursor..cursor + len).ok_or_else(|| {
            format!("lenenc field truncated: need {len} bytes at offset {cursor}")
        })?;
        cursor += len;
        fields.push(Some(value.to_vec()));
    }
    Ok(fields)
}

fn decode_result_batch_to_chunk(bytes: &[u8]) -> Result<Chunk, String> {
    if bytes.is_empty() {
        return empty_chunk();
    }

    let batch: crate::data::TResultBatch = thrift_binary_deserialize(bytes)?;
    if batch.rows.is_empty() {
        return empty_chunk();
    }

    let first = parse_all_lenenc_fields(&batch.rows[0])?;
    let column_count = first.len();
    let mut columns = vec![Vec::<Option<Vec<u8>>>::with_capacity(batch.rows.len()); column_count];
    for (idx, field) in first.into_iter().enumerate() {
        columns[idx].push(field);
    }

    for row in batch.rows.iter().skip(1) {
        let fields = parse_all_lenenc_fields(row)?;
        if fields.len() != column_count {
            return Err(format!(
                "result batch row has {} columns, expected {}",
                fields.len(),
                column_count
            ));
        }
        for (idx, field) in fields.into_iter().enumerate() {
            columns[idx].push(field);
        }
    }

    let mut fields = Vec::with_capacity(column_count);
    let mut slots = Vec::with_capacity(column_count);
    let mut arrays = Vec::<ArrayRef>::with_capacity(column_count);
    for (idx, values) in columns.into_iter().enumerate() {
        let field = Field::new(format!("col_{idx}"), DataType::Binary, true);
        let mut builder = BinaryBuilder::new();
        for value in values {
            match value {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        arrays.push(Arc::new(builder.finish()));
        slots.push(ChunkSlotSchema::new_with_field(
            SlotId(idx as u32),
            field.clone(),
            None,
            None,
        ));
        fields.push(field);
    }

    let arrow_schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| format!("build remote result record batch failed: {e}"))?;
    let chunk_schema = Arc::new(ChunkSchema::try_new(slots)?);
    Chunk::try_new_with_chunk_schema(record_batch, chunk_schema)
}

// ---------------------------------------------------------------------------
// Root-fragment slot (RESULT_SINK path)
// ---------------------------------------------------------------------------

enum RootSlotState {
    Running,
    Done(VecDeque<Chunk>),
    Error(String),
}

struct RootSlot {
    state: Mutex<RootSlotState>,
    notify: Condvar,
}

impl RootSlot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RootSlotState::Running),
            notify: Condvar::new(),
        })
    }

    fn set_done(&self, chunks: Vec<Chunk>) {
        let mut guard = self.state.lock().expect("root slot lock");
        *guard = RootSlotState::Done(VecDeque::from(chunks));
        self.notify.notify_all();
    }

    fn set_error(&self, msg: String) {
        let mut guard = self.state.lock().expect("root slot lock");
        *guard = RootSlotState::Error(msg);
        self.notify.notify_all();
    }
}

// ---------------------------------------------------------------------------
// InProcessDispatcher
// ---------------------------------------------------------------------------

struct InProcessState {
    /// gRPC exchange endpoint for this process.
    exchange_host: String,
    exchange_port: u16,
    /// Root fragment result slots (keyed by finst (hi, lo)).
    root_slots: Mutex<HashMap<(i64, i64), Arc<RootSlot>>>,
    /// All submitted fragment instance IDs, used for bulk cancel.
    submitted_ids: Mutex<Vec<(i64, i64)>>,
}

/// Dispatcher that runs all fragments in-process via `std::thread::spawn`.
///
/// Used in all-in-one mode.  Keeps all existing execution semantics:
/// non-root fragments use `execute_plan_fragment_sync`; the root fragment
/// (RESULT_SINK) runs the lowering + pipeline executor directly and
/// delivers `Chunk`s via a `ResultSinkHandle`.
pub struct InProcessDispatcher {
    state: Arc<InProcessState>,
}

impl InProcessDispatcher {
    /// Create an `InProcessDispatcher` bound to the given exchange endpoint.
    pub fn new(exchange_host: impl Into<String>, exchange_port: u16) -> Self {
        Self {
            state: Arc::new(InProcessState {
                exchange_host: exchange_host.into(),
                exchange_port,
                root_slots: Mutex::new(HashMap::new()),
                submitted_ids: Mutex::new(Vec::new()),
            }),
        }
    }
}

impl Default for InProcessDispatcher {
    fn default() -> Self {
        Self::new("127.0.0.1", 0)
    }
}

fn submitted_ids_snapshot(state: &InProcessState) -> Vec<(i64, i64)> {
    state
        .submitted_ids
        .lock()
        .expect("submitted_ids lock")
        .clone()
}

fn cancel_fragment_instance(hi: i64, lo: i64) {
    crate::runtime::result_buffer::cancel(crate::common::types::UniqueId { hi, lo });
    crate::runtime::exchange::cancel_fragment(hi, lo);
}

/// Idempotent bulk cancellation shared by autonomous fragment failures and the
/// coordinator-side cleanup path. Multiple overlapping cancel waves may race
/// across the same finst ids; the underlying result buffer and exchange cancel
/// operations are intentionally safe to repeat.
fn cancel_all_submitted(state: &InProcessState) {
    for (hi, lo) in submitted_ids_snapshot(state) {
        cancel_fragment_instance(hi, lo);
    }
}

fn format_fragment_error(finst_key: (i64, i64), error: &str) -> String {
    format!(
        "fragment {}/{} failed during in-process execution: {}",
        finst_key.0, finst_key.1, error
    )
}

/// Returns true if the TExecPlanFragmentParams carries a RESULT_SINK (root
/// fragment).
fn is_result_sink(params: &internal_service::TExecPlanFragmentParams) -> bool {
    params
        .fragment
        .as_ref()
        .and_then(|f| f.output_sink.as_ref())
        .map(|s| s.type_ == data_sinks::TDataSinkType::RESULT_SINK)
        .unwrap_or(false)
}

impl FragmentDispatcher for InProcessDispatcher {
    fn exchange_addr(&self) -> types::TNetworkAddress {
        types::TNetworkAddress::new(
            self.state.exchange_host.clone(),
            self.state.exchange_port as i32,
        )
    }

    fn submit_fragment(
        &self,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String> {
        let exec_params = params
            .params
            .as_ref()
            .ok_or_else(|| "submit_fragment: missing exec params".to_string())?;
        let finst_key = (
            exec_params.fragment_instance_id.hi,
            exec_params.fragment_instance_id.lo,
        );

        // Track this finst_id for bulk cancel.
        {
            let mut ids = self.state.submitted_ids.lock().expect("submitted_ids lock");
            ids.push(finst_key);
        }

        if is_result_sink(&params) {
            // Root fragment path: run with ResultSinkHandle so chunks are
            // available as Arrow data without going through result_buffer
            // serialization.
            let slot = RootSlot::new();
            {
                let mut slots = self.state.root_slots.lock().expect("root_slots lock");
                slots.insert(finst_key, Arc::clone(&slot));
            }

            let state = Arc::clone(&self.state);
            std::thread::spawn(move || {
                let result = run_root_fragment_in_process(params);
                match result {
                    Ok(chunks) => slot.set_done(chunks),
                    Err(msg) => {
                        // Cancel all exchanges so blocked receivers unblock.
                        warn!("{}", format_fragment_error(finst_key, &msg));
                        cancel_all_submitted(&state);
                        slot.set_error(msg);
                    }
                }
            });
        } else {
            // Non-root fragment path: execute_plan_fragment_sync in a thread.
            let state = Arc::clone(&self.state);
            std::thread::spawn(move || {
                let result = crate::service::internal_service::execute_plan_fragment_sync(params);
                if let Err(e) = result {
                    warn!("{}", format_fragment_error(finst_key, &e));
                    // Cancel all exchanges so blocked receivers (including root) unblock.
                    cancel_all_submitted(&state);
                }
            });
        }

        Ok(())
    }

    fn fetch_result(
        &self,
        finst_id: types::TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String> {
        let key = (finst_id.hi, finst_id.lo);
        let slot = {
            let slots = self.state.root_slots.lock().expect("root_slots lock");
            match slots.get(&key) {
                Some(slot) => Arc::clone(slot),
                // Not a root fragment finst_id: treat as Eof (no data).
                None => {
                    warn!(
                        "fetch_result called for unknown root finst_id {}/{}",
                        key.0, key.1
                    );
                    return Ok(FetchOutcome::Eof);
                }
            }
        };

        let wait = Duration::from_millis(max_wait_ms.max(1) as u64);
        let mut guard = slot.state.lock().expect("root slot lock");

        // Wait only if still running.
        if matches!(*guard, RootSlotState::Running) {
            let (_g, _timeout) = slot.notify.wait_timeout(guard, wait).expect("condvar wait");
            guard = _g;
        }

        match &mut *guard {
            RootSlotState::Running => Ok(FetchOutcome::NotReady),
            RootSlotState::Done(queue) => {
                if let Some(chunk) = queue.pop_front() {
                    Ok(FetchOutcome::Ready(chunk))
                } else {
                    Ok(FetchOutcome::Eof)
                }
            }
            RootSlotState::Error(msg) => Ok(FetchOutcome::Err(msg.clone())),
        }
    }

    fn cancel_fragments(&self, finst_ids: &[types::TUniqueId]) {
        for fid in finst_ids {
            cancel_fragment_instance(fid.hi, fid.lo);
        }
    }
}

// ---------------------------------------------------------------------------
// RemoteDispatcher
// ---------------------------------------------------------------------------

pub struct RemoteDispatcher {
    backend: SocketAddr,
}

impl RemoteDispatcher {
    pub fn new(backend: SocketAddr) -> Self {
        Self { backend }
    }

    fn client(&self) -> Result<NovaRocksGrpcRemoteClient, String> {
        NovaRocksGrpcRemoteClient::connect_blocking(self.backend)
    }
}

impl FragmentDispatcher for RemoteDispatcher {
    fn exchange_addr(&self) -> types::TNetworkAddress {
        // All fragments dispatched through RemoteDispatcher run on the BE.
        // Inter-fragment exchange data must flow through the BE's own gRPC
        // server (self.backend) so that both the producer and the consumer
        // share the same local exchange registry on the BE.  Using the FE's
        // exchange server would send data to the FE's registry while the
        // consumer reads from the BE's registry, causing a permanent stall.
        types::TNetworkAddress::new(self.backend.ip().to_string(), self.backend.port() as i32)
    }

    fn submit_fragment(
        &self,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String> {
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
        let resp = self
            .client()?
            .blocking_submit_fragment(SubmitFragmentRequest {
                exec_plan_fragment_params_thrift: payload,
            })?;
        if resp.status_code != 0 {
            return Err(format!(
                "remote submit_fragment failed on {}: {}",
                self.backend, resp.message
            ));
        }
        Ok(())
    }

    fn fetch_result(
        &self,
        finst_id: types::TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String> {
        let call_index = REMOTE_FETCH_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            println!("NOVAROCKS_FETCH_NOT_READY call={call_index}");
            let _ = std::io::Write::flush(&mut std::io::stdout());
            return Ok(FetchOutcome::NotReady);
        }
        let resp = self.client()?.blocking_fetch_result(FetchResultRequest {
            finst_id: Some(PUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms,
        })?;
        let status = FetchStatus::try_from(resp.status).map_err(|_| {
            format!(
                "remote fetch_result returned unknown status {}",
                resp.status
            )
        })?;
        match status {
            FetchStatus::Ready => {
                let chunk = decode_result_batch_to_chunk(&resp.result_batch_thrift)?;
                Ok(FetchOutcome::Ready(chunk))
            }
            FetchStatus::NotReady => Ok(FetchOutcome::NotReady),
            FetchStatus::Eof => Ok(FetchOutcome::Eof),
            FetchStatus::Error => Ok(FetchOutcome::Err(resp.message)),
        }
    }

    fn cancel_fragments(&self, finst_ids: &[types::TUniqueId]) {
        let backend = self.backend;
        let req = CancelFragmentRequest {
            finst_ids: finst_ids
                .iter()
                .map(|id| PUniqueId {
                    hi: id.hi,
                    lo: id.lo,
                })
                .collect(),
            reason: "coordinator cancel".to_string(),
        };
        if let Err(e) = std::thread::Builder::new()
            .name("remote-cancel-fragment".to_string())
            .spawn(move || {
                match NovaRocksGrpcRemoteClient::connect_blocking(backend)
                    .and_then(|client| client.blocking_cancel_fragment(req))
                {
                    Ok(resp) if resp.status_code == 0 => {}
                    Ok(resp) => warn!(
                        "remote cancel_fragment returned nonzero status from {}: {}",
                        backend, resp.status_code
                    ),
                    Err(e) => warn!("remote cancel_fragment failed for {}: {}", backend, e),
                }
            })
        {
            warn!("remote cancel_fragment spawn failed for {}: {}", backend, e);
        }
    }
}

/// Run the root (RESULT_SINK) fragment in-process and return result chunks.
///
/// Mirrors the root-fragment execution path from `ExecutionCoordinator` but
/// operates on the pre-built `TExecPlanFragmentParams` produced by
/// `build_exec_plan_fragment_params`.
fn run_root_fragment_in_process(
    params: internal_service::TExecPlanFragmentParams,
) -> Result<Vec<Chunk>, String> {
    use crate::exec::expr::ExprArena;

    let fragment = params
        .fragment
        .as_ref()
        .ok_or_else(|| "run_root_fragment: missing fragment".to_string())?;
    let plan = fragment
        .plan
        .as_ref()
        .ok_or_else(|| "run_root_fragment: fragment has no plan".to_string())?;
    let exec_p = params
        .params
        .as_ref()
        .ok_or_else(|| "run_root_fragment: missing exec params".to_string())?;

    let desc_tbl = params.desc_tbl.as_ref();
    let query_opts = params.query_options.as_ref();

    let mut tuple_slots = build_tuple_slot_order(desc_tbl);
    reorder_tuple_slots(&mut tuple_slots, desc_tbl);
    let layout_hints = tuple_slots.clone();

    let mut arena = ExprArena::default();
    let connectors = crate::connector::ConnectorRegistry::default();
    let lowered = lower_plan(
        plan,
        &mut arena,
        &tuple_slots,
        desc_tbl,
        None, // query_global_dicts
        None, // query_global_dict_exprs
        Some(exec_p),
        query_opts,
        None, // db_name
        &connectors,
        &layout_hints,
        None, // last_query_id
        None, // fe_addr
        None, // iceberg_catalogs
    )?;

    let mut exec_plan = ExecPlan {
        arena,
        root: lowered.node,
    };
    push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);

    let handle = ResultSinkHandle::new();
    let exchange_finst_id = Some((
        exec_p.fragment_instance_id.hi,
        exec_p.fragment_instance_id.lo,
    ));

    let query_id = Some(QueryId {
        hi: exec_p.query_id.hi,
        lo: exec_p.query_id.lo,
    });
    let finst_id = Some(crate::common::types::UniqueId {
        hi: exec_p.fragment_instance_id.hi,
        lo: exec_p.fragment_instance_id.lo,
    });

    let runtime_state = Arc::new(RuntimeState::new(
        query_opts.cloned(),
        None, // cache_options
        query_id,
        exec_p.runtime_filter_params.clone(),
        finst_id,
        None, // backend_num
        None, // mem_tracker
        None, // spill_config
        None, // spill_manager
    ));

    let dop = resolve_root_pipeline_dop(&params);

    execute_plan_with_pipeline(
        exec_plan,
        false,
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(handle.clone())),
        exchange_finst_id,
        None, // profiler
        dop,
        runtime_state,
        query_id,
        None, // fe_addr
        None, // backend_num
    )?;

    Ok(handle.take_chunks())
}

fn resolve_root_pipeline_dop(params: &internal_service::TExecPlanFragmentParams) -> i32 {
    params
        .pipeline_dop
        .map(crate::runtime::exec_env::calc_pipeline_dop)
        .unwrap_or_else(compute_pipeline_dop)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};

    use crate::common::thrift::thrift_binary_serialize;
    use crate::service::grpc_proto as proto;
    use arrow::array::Array;
    use proto::novarocks::fetch_result_response::Status as FetchStatus;
    use proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc;
    use proto::novarocks::{
        CancelFragmentRequest, ExchangeRequest, ExchangeResponse, FetchResultRequest,
        FetchResultResponse, SubmitFragmentRequest, SubmitFragmentResponse,
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
        use crate::partitions;
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
        let fragment = crate::planner::TPlanFragment::new(
            None::<crate::plan_nodes::TPlan>,
            None::<Vec<crate::exprs::TExpr>>,
            Some(noop_sink),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            None::<i64>,
            None::<i64>,
            None::<Vec<crate::data::TGlobalDict>>,
            None::<Vec<crate::data::TGlobalDict>>,
            None::<crate::planner::TCacheParam>,
            None::<std::collections::BTreeMap<i32, crate::exprs::TExpr>>,
            None::<crate::planner::TGroupExecutionParam>,
        );
        internal_service::TExecPlanFragmentParams::new(
            internal_service::InternalServiceVersion::V1,
            Some(fragment),
            None::<crate::descriptors::TDescriptorTable>,
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
            None::<crate::work_group::TWorkGroup>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<internal_service::TAdaptiveDopParam>,
            None::<i32>,
            None::<internal_service::TPredicateTreeParams>,
            None::<Vec<i32>>,
        )
    }

    #[test]
    fn parse_all_lenenc_fields_empty_row() {
        let fields = parse_all_lenenc_fields(&[]).expect("parse empty row");
        assert!(fields.is_empty());
    }

    #[test]
    fn parse_all_lenenc_fields_single_col_short() {
        let fields = parse_all_lenenc_fields(b"\x011").expect("parse single field");
        assert_eq!(fields, vec![Some(b"1".to_vec())]);
    }

    #[test]
    fn parse_all_lenenc_fields_null_col() {
        let fields = parse_all_lenenc_fields(b"\xfb").expect("parse null field");
        assert_eq!(fields, vec![None]);
    }

    #[test]
    fn decode_result_batch_to_chunk_empty_bytes_returns_empty() {
        let chunk = decode_result_batch_to_chunk(&[]).expect("decode empty bytes");
        assert_eq!(chunk.columns().len(), 0);
        assert_eq!(chunk.len(), 0);
    }

    #[test]
    fn decode_result_batch_to_chunk_single_row_single_col() {
        let batch = crate::data::TResultBatch::new(vec![b"\x011".to_vec()], false, 0, None);
        let bytes = thrift_binary_serialize(&batch).expect("serialize result batch");

        let chunk = decode_result_batch_to_chunk(&bytes).expect("decode chunk");

        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(chunk.len(), 1);
        let col = chunk.columns()[0]
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("binary column");
        assert_eq!(col.value(0), b"1");
    }

    #[derive(Clone)]
    struct MockGrpc(Arc<MockState>);

    struct MockState {
        submit_code: AtomicI32,
        fetch_status: AtomicI32,
        fetch_batch: Mutex<Vec<u8>>,
        cancel_count: AtomicUsize,
        cancel_delay_ms: AtomicU64,
    }

    impl Default for MockState {
        fn default() -> Self {
            Self {
                submit_code: AtomicI32::new(0),
                fetch_status: AtomicI32::new(FetchStatus::Eof as i32),
                fetch_batch: Mutex::new(Vec::new()),
                cancel_count: AtomicUsize::new(0),
                cancel_delay_ms: AtomicU64::new(0),
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
            _request: Request<FetchResultRequest>,
        ) -> Result<Response<FetchResultResponse>, Status> {
            Ok(Response::new(FetchResultResponse {
                status: self.0.fetch_status.load(Ordering::SeqCst),
                result_batch_thrift: self.0.fetch_batch.lock().expect("fetch batch lock").clone(),
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
        let dispatcher = RemoteDispatcher::new(addr);

        let err = dispatcher
            .submit_fragment(make_noop_sink_params(1, 2))
            .expect_err("nonzero submit status should error");

        assert!(err.contains("submit failed"));
    }

    #[test]
    fn remote_dispatcher_fetch_eof_returns_eof() {
        let state = Arc::new(MockState::default());
        state
            .fetch_status
            .store(FetchStatus::Eof as i32, Ordering::SeqCst);
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(addr);

        let outcome = dispatcher
            .fetch_result(make_finst_id(1, 2), 0)
            .expect("fetch");

        assert!(matches!(outcome, FetchOutcome::Eof));
    }

    #[test]
    fn remote_dispatcher_fetch_ready_decodes_batch() {
        let state = Arc::new(MockState::default());
        state
            .fetch_status
            .store(FetchStatus::Ready as i32, Ordering::SeqCst);
        let batch = crate::data::TResultBatch::new(vec![b"\x011".to_vec()], false, 0, None);
        *state.fetch_batch.lock().expect("fetch batch lock") =
            thrift_binary_serialize(&batch).expect("serialize result batch");
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(addr);

        let outcome = dispatcher
            .fetch_result(make_finst_id(1, 2), 0)
            .expect("fetch");

        let FetchOutcome::Ready(chunk) = outcome else {
            panic!("expected ready chunk");
        };
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(chunk.len(), 1);
    }

    #[test]
    fn remote_dispatcher_cancel_is_sent() {
        let state = Arc::new(MockState::default());
        let addr = spawn_mock_server(Arc::clone(&state));
        let dispatcher = RemoteDispatcher::new(addr);

        dispatcher.cancel_fragments(&[make_finst_id(1, 2)]);

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
        let dispatcher = RemoteDispatcher::new(addr);

        let start = std::time::Instant::now();
        dispatcher.cancel_fragments(&[make_finst_id(7, 8)]);
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
    fn fetch_unknown_finst_returns_eof() {
        let dispatcher = InProcessDispatcher::default();
        let finst_id = make_finst_id(999, 888);
        let outcome = dispatcher.fetch_result(finst_id, 10).unwrap();
        assert!(
            matches!(outcome, FetchOutcome::Eof),
            "expected Eof for unknown finst_id"
        );
    }

    #[test]
    fn cancel_is_idempotent() {
        let dispatcher = InProcessDispatcher::default();
        let ids = vec![make_finst_id(100, 200), make_finst_id(101, 201)];
        // Calling cancel twice must not panic.
        dispatcher.cancel_fragments(&ids);
        dispatcher.cancel_fragments(&ids);
    }

    #[test]
    fn compute_pipeline_dop_returns_positive() {
        let dop = compute_pipeline_dop();
        assert!(dop > 0, "dop must be positive, got {dop}");
        assert!(dop <= 4, "dop must be at most 4 in test env, got {dop}");
    }

    #[test]
    fn is_result_sink_detects_noop_correctly() {
        let params = make_noop_sink_params(1, 2);
        assert!(
            !is_result_sink(&params),
            "NOOP_SINK should not be detected as result sink"
        );
    }

    #[test]
    fn is_result_sink_detects_result_sink() {
        use crate::partitions;
        let result_sink = data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::RESULT_SINK,
            None::<data_sinks::TDataStreamSink>,
            Some(data_sinks::TResultSink::default()),
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
        let fragment = crate::planner::TPlanFragment::new(
            None::<crate::plan_nodes::TPlan>,
            None::<Vec<crate::exprs::TExpr>>,
            Some(result_sink),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            None::<i64>,
            None::<i64>,
            None::<Vec<crate::data::TGlobalDict>>,
            None::<Vec<crate::data::TGlobalDict>>,
            None::<crate::planner::TCacheParam>,
            None::<std::collections::BTreeMap<i32, crate::exprs::TExpr>>,
            None::<crate::planner::TGroupExecutionParam>,
        );
        let mut params = make_noop_sink_params(1, 2);
        params.fragment = Some(fragment);
        assert!(
            is_result_sink(&params),
            "RESULT_SINK should be detected as result sink"
        );
    }

    #[test]
    fn submitted_ids_snapshot_reads_current_ids_at_error_time() {
        let dispatcher = InProcessDispatcher::default();
        {
            let mut ids = dispatcher
                .state
                .submitted_ids
                .lock()
                .expect("submitted_ids lock");
            ids.push((1, 10));
        }
        let stale_snapshot = vec![(1, 10)];
        {
            let mut ids = dispatcher
                .state
                .submitted_ids
                .lock()
                .expect("submitted_ids lock");
            ids.push((1, 11));
        }

        let current = submitted_ids_snapshot(&dispatcher.state);
        assert_ne!(
            current, stale_snapshot,
            "helper must read current state, not a submit-time snapshot"
        );
        assert_eq!(current, vec![(1, 10), (1, 11)]);
    }

    #[test]
    fn cancel_fragment_instance_cancels_result_buffer_and_exchange() {
        let finst_id = crate::common::types::UniqueId { hi: 42, lo: 420 };
        crate::runtime::result_buffer::create_sender(finst_id);

        cancel_fragment_instance(finst_id.hi, finst_id.lo);

        let crate::runtime::result_buffer::TryFetchResult::Error(err) =
            crate::runtime::result_buffer::try_fetch(finst_id)
        else {
            panic!("expected result buffer cancellation to be observable");
        };
        assert!(matches!(
            err.kind,
            crate::runtime::result_buffer::FetchErrorKind::Cancelled
        ));
    }

    #[test]
    fn root_pipeline_dop_uses_request_value_when_present() {
        let mut params = make_noop_sink_params(1, 2);
        params.pipeline_dop = Some(7);
        assert_eq!(resolve_root_pipeline_dop(&params), 7);
    }

    #[test]
    fn fragment_error_message_includes_error_text() {
        let message = format_fragment_error((9, 99), "lowering failed");
        assert!(message.contains("9/99"), "message should include finst id");
        assert!(
            message.contains("lowering failed"),
            "message should include original error"
        );
    }
}
