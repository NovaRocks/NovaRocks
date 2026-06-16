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
//! `std::thread::spawn`; `RemoteDispatcher` talks to one or more remote BEs
//! over gRPC by index; `FragmentScheduler` (PR-3/PR-4) will choose which
//! backend each fragment instance lands on.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(test)]
use arrow::array::{ArrayRef, BinaryBuilder};
#[cfg(test)]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use thrift::protocol::{TBinaryOutputProtocol, TSerializable};
use thrift::transport::{TBufferChannel, TIoChannel};

#[cfg(test)]
use crate::common::ids::SlotId;
#[cfg(test)]
use crate::common::thrift::thrift_binary_deserialize;
use crate::common::types::UniqueId;
use crate::exec::chunk::Chunk;
use crate::exec::chunk::ChunkSchemaRef;
#[cfg(test)]
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::internal_service;
use crate::lower::layout::{build_tuple_slot_order, reorder_tuple_slots};
use crate::lower::thrift::lower_plan;
use crate::runtime::profile::Profiler;
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

    /// Whether this dispatcher can return per-fragment profilers after a
    /// coordinated execution. Remote dispatchers do not yet surface profiles.
    fn supports_profile_collection(&self) -> bool {
        false
    }

    /// Drain fragment profilers collected by in-process execution. Remote
    /// dispatchers do not currently surface profiles through this path.
    fn take_profiles(&self) -> Vec<Profiler> {
        Vec::new()
    }
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

#[cfg(test)]
fn empty_chunk() -> Result<Chunk, String> {
    Chunk::try_new_with_chunk_schema(
        RecordBatch::new_empty(Arc::new(Schema::empty())),
        Arc::new(ChunkSchema::empty()),
    )
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

type QueryKey = (i64, i64);
type FinstKey = (i64, i64);

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
        if matches!(*guard, RootSlotState::Running) {
            *guard = RootSlotState::Done(VecDeque::from(chunks));
            self.notify.notify_all();
        }
    }

    fn set_error(&self, msg: String) {
        let mut guard = self.state.lock().expect("root slot lock");
        if matches!(*guard, RootSlotState::Running) {
            *guard = RootSlotState::Error(msg);
            self.notify.notify_all();
        }
    }
}

#[derive(Clone)]
struct RootSlotEntry {
    query_key: QueryKey,
    slot: Arc<RootSlot>,
}

// ---------------------------------------------------------------------------
// InProcessDispatcher
// ---------------------------------------------------------------------------

struct InProcessState {
    /// Root fragment result slots (keyed by finst (hi, lo)).
    root_slots: Mutex<HashMap<FinstKey, RootSlotEntry>>,
    /// All submitted fragment instance IDs, used for bulk cancel.
    submitted_ids: Mutex<Vec<FinstKey>>,
    /// First non-root fragment error by query. If it happens before the root
    /// slot is installed, that query's root slot picks it up during root
    /// submission.
    fragment_errors: Mutex<HashMap<QueryKey, String>>,
    fragment_profilers: Mutex<HashMap<FinstKey, Profiler>>,
    fragment_threads: Mutex<Vec<JoinHandle<()>>>,
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
    /// Create an `InProcessDispatcher`.
    ///
    /// Fragments run in-process and exchange through the local gRPC server, so
    /// no exchange endpoint is stored here: the scheduler now fills every
    /// `TPlanFragmentDestination` with the concrete backend address.
    pub fn new() -> Self {
        Self {
            state: Arc::new(InProcessState {
                root_slots: Mutex::new(HashMap::new()),
                submitted_ids: Mutex::new(Vec::new()),
                fragment_errors: Mutex::new(HashMap::new()),
                fragment_profilers: Mutex::new(HashMap::new()),
                fragment_threads: Mutex::new(Vec::new()),
            }),
        }
    }
}

impl Default for InProcessDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

fn submitted_ids_snapshot(state: &InProcessState) -> Vec<FinstKey> {
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

fn register_in_process_report_instance(
    params: &internal_service::TExecPlanFragmentParams,
    finst_id: UniqueId,
    query_id: QueryId,
) {
    let backend_num = params.backend_num;
    if let (Some(report_addr), Some(backend_num)) =
        (params.novarocks_report_addr.clone(), backend_num)
    {
        crate::service::fe_report::register_novarocks_instance(
            finst_id,
            query_id,
            report_addr,
            backend_num,
            false,
            None,
            None,
            None,
            None,
        );
    } else if let (Some(coord), Some(backend_num)) = (params.coord.clone(), backend_num) {
        crate::service::fe_report::register_instance(
            finst_id,
            query_id,
            coord,
            backend_num,
            false,
            None,
            None,
            None,
            None,
        );
    }
}

fn record_fragment_error(state: &InProcessState, query_key: QueryKey, msg: String) {
    let msg = {
        let mut guard = state.fragment_errors.lock().expect("fragment_errors lock");
        guard.entry(query_key).or_insert(msg).clone()
    };
    let slots: Vec<Arc<RootSlot>> = state
        .root_slots
        .lock()
        .expect("root_slots lock")
        .values()
        .filter(|entry| entry.query_key == query_key)
        .map(|entry| Arc::clone(&entry.slot))
        .collect();
    for slot in slots {
        slot.set_error(msg.clone());
    }
}

fn pending_fragment_error(state: &InProcessState, query_key: QueryKey) -> Option<String> {
    state
        .fragment_errors
        .lock()
        .expect("fragment_errors lock")
        .get(&query_key)
        .cloned()
}

fn record_fragment_profiler(
    state: &InProcessState,
    finst_key: FinstKey,
    profiler: Option<Profiler>,
) {
    if let Some(profiler) = profiler {
        state
            .fragment_profilers
            .lock()
            .expect("fragment_profilers lock")
            .insert(finst_key, profiler);
    }
}

fn record_fragment_thread(state: &InProcessState, handle: JoinHandle<()>) {
    state
        .fragment_threads
        .lock()
        .expect("fragment_threads lock")
        .push(handle);
}

fn wait_for_fragment_threads(state: &InProcessState) {
    let handles = std::mem::take(
        &mut *state
            .fragment_threads
            .lock()
            .expect("fragment_threads lock"),
    );
    for handle in handles {
        if handle.join().is_err() {
            warn!("in-process fragment thread panicked while collecting profiles");
        }
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
    fn submit_fragment(
        &self,
        backend_idx: usize,
        params: internal_service::TExecPlanFragmentParams,
    ) -> Result<(), String> {
        if backend_idx != 0 {
            return Err(format!(
                "InProcessDispatcher only supports backend_idx=0, got {backend_idx}"
            ));
        }
        let exec_params = params
            .params
            .as_ref()
            .ok_or_else(|| "submit_fragment: missing exec params".to_string())?;
        let finst_key = (
            exec_params.fragment_instance_id.hi,
            exec_params.fragment_instance_id.lo,
        );
        let query_key = (exec_params.query_id.hi, exec_params.query_id.lo);

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
                slots.insert(
                    finst_key,
                    RootSlotEntry {
                        query_key,
                        slot: Arc::clone(&slot),
                    },
                );
            }
            if let Some(msg) = pending_fragment_error(&self.state, query_key) {
                slot.set_error(msg);
            }

            let state = Arc::clone(&self.state);
            let handle = std::thread::spawn(move || {
                finish_root_fragment_in_process(finst_key, state, slot, || {
                    run_root_fragment_in_process(params)
                });
            });
            record_fragment_thread(self.state.as_ref(), handle);
        } else {
            // Non-root fragment path: execute_plan_fragment_sync in a thread.
            let state = Arc::clone(&self.state);
            let handle = std::thread::spawn(move || {
                let finst_id = UniqueId {
                    hi: finst_key.0,
                    lo: finst_key.1,
                };
                let query_id = QueryId {
                    hi: query_key.0,
                    lo: query_key.1,
                };
                register_in_process_report_instance(&params, finst_id, query_id);
                let result = crate::service::internal_service::execute_plan_fragment_sync(params);
                if let Ok(result) = result.as_ref() {
                    record_fragment_profiler(&state, finst_key, result.profiler.clone());
                }
                let report_error = result.as_ref().err().cloned();
                crate::service::fe_report::report_fragment_done(finst_id, report_error.clone());
                if let Err(e) = result {
                    warn!("{}", format_fragment_error(finst_key, &e));
                    record_fragment_error(&state, query_key, e);
                    // Cancel all exchanges so blocked receivers (including root) unblock.
                    cancel_all_submitted(&state);
                }
            });
            record_fragment_thread(self.state.as_ref(), handle);
        }

        Ok(())
    }

    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: types::TUniqueId,
        max_wait_ms: i64,
        _expected_chunk_schema: Option<&ChunkSchemaRef>,
    ) -> Result<FetchOutcome, String> {
        if backend_idx != 0 {
            return Err(format!(
                "InProcessDispatcher only supports backend_idx=0, got {backend_idx}"
            ));
        }
        let key = (finst_id.hi, finst_id.lo);
        let slot = {
            let slots = self.state.root_slots.lock().expect("root_slots lock");
            match slots.get(&key) {
                Some(entry) => Arc::clone(&entry.slot),
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

    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[types::TUniqueId]) {
        debug_assert_eq!(
            backend_idx, 0,
            "InProcessDispatcher only supports backend_idx=0"
        );
        for fid in finst_ids {
            cancel_fragment_instance(fid.hi, fid.lo);
        }
    }

    fn backend_count(&self) -> usize {
        1
    }

    fn supports_profile_collection(&self) -> bool {
        true
    }

    fn take_profiles(&self) -> Vec<Profiler> {
        wait_for_fragment_threads(self.state.as_ref());
        std::mem::take(
            &mut *self
                .state
                .fragment_profilers
                .lock()
                .expect("fragment_profilers lock"),
        )
        .into_values()
        .collect()
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

    /// The address of `backend_idx`, if in range.
    ///
    /// PR-4 destination wiring: `FragmentScheduler` calls this to embed the
    /// correct backend address into `TPlanFragmentDestination` entries when
    /// assigning fragment instances to specific backends.
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

/// Run the root (RESULT_SINK) fragment in-process and return result chunks.
///
/// Mirrors the root-fragment execution path from `ExecutionCoordinator` but
/// operates on the pre-built `TExecPlanFragmentParams` produced by
/// `build_exec_plan_fragment_params`.
struct RootFragmentOutput {
    chunks: Vec<Chunk>,
    profiler: Option<Profiler>,
}

fn run_root_fragment_in_process(
    params: internal_service::TExecPlanFragmentParams,
) -> Result<RootFragmentOutput, String> {
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
    let profile_name = plan
        .nodes
        .first()
        .map(|node| format!("execute_fragment (plan_node_id={})", node.node_id))
        .unwrap_or_else(|| "execute_fragment".to_string());
    let profiler = query_opts
        .and_then(|opts| opts.enable_profile)
        .unwrap_or(false)
        .then(|| Profiler::new(profile_name));

    let mut tuple_slots = build_tuple_slot_order(desc_tbl);
    reorder_tuple_slots(&mut tuple_slots, desc_tbl);
    let layout_hints = tuple_slots.clone();

    let mut arena = ExprArena::default();
    let connectors = crate::connector::ConnectorRegistry::default();
    let lowered = {
        let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
        lower_plan(
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
        )?
    };

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

    // Root fragment is the FE-assigned instance whose index (backend_num) the
    // pipeline threads into RuntimeState; the data_stream_sink derives its
    // be_number from it. The root is always instance 0 in the scheduling plan.
    let backend_num = params.backend_num;

    let runtime_state = Arc::new(RuntimeState::new(
        query_opts.cloned(),
        None, // cache_options
        query_id,
        exec_p.runtime_filter_params.clone(),
        finst_id,
        backend_num,
        None, // mem_tracker
        None, // spill_config
        None, // spill_manager
    ));

    let dop = resolve_root_pipeline_dop(&params);

    let _exec_timer = profiler
        .as_ref()
        .map(|p| p.scoped_timer("PipelineExecuteTime"));
    execute_plan_with_pipeline(
        exec_plan,
        false,
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(handle.clone())),
        exchange_finst_id,
        profiler.clone(),
        dop,
        runtime_state,
        query_id,
        None, // fe_addr
        backend_num,
    )?;

    Ok(RootFragmentOutput {
        chunks: handle.take_chunks(),
        profiler,
    })
}

fn finish_root_fragment_in_process<F>(
    finst_key: (i64, i64),
    state: Arc<InProcessState>,
    slot: Arc<RootSlot>,
    run: F,
) where
    F: FnOnce() -> Result<RootFragmentOutput, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(Ok(output)) => {
            record_fragment_profiler(&state, finst_key, output.profiler);
            slot.set_done(output.chunks);
        }
        Ok(Err(msg)) => {
            warn!("{}", format_fragment_error(finst_key, &msg));
            cancel_all_submitted(&state);
            slot.set_error(msg);
        }
        Err(payload) => {
            let msg = format!(
                "root fragment thread panicked: {}",
                panic_payload_message(payload.as_ref())
            );
            warn!("{}", format_fragment_error(finst_key, &msg));
            cancel_all_submitted(&state);
            slot.set_error(msg);
        }
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
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
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};

    use crate::common::thrift::thrift_binary_serialize;
    use crate::service::grpc_proto as proto;
    use arrow::array::{Array, Int32Array};
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
            None::<i32>,
            None::<types::TNetworkAddress>,
            None::<bool>,
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
                fetch_batch: Mutex::new(Vec::new()),
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
                result_batch_thrift: self.0.fetch_batch.lock().expect("fetch batch lock").clone(),
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
    fn fetch_unknown_finst_returns_eof() {
        let dispatcher = InProcessDispatcher::default();
        let finst_id = make_finst_id(999, 888);
        let outcome = dispatcher.fetch_result(0, finst_id, 10, None).unwrap();
        assert!(
            matches!(outcome, FetchOutcome::Eof),
            "expected Eof for unknown finst_id"
        );
    }

    #[test]
    fn root_fragment_panic_sets_slot_error() {
        let dispatcher = InProcessDispatcher::default();
        let finst_key = (33, 44);
        let slot = RootSlot::new();
        dispatcher
            .state
            .root_slots
            .lock()
            .expect("root_slots lock")
            .insert(
                finst_key,
                RootSlotEntry {
                    query_key: finst_key,
                    slot: Arc::clone(&slot),
                },
            );

        finish_root_fragment_in_process(finst_key, Arc::clone(&dispatcher.state), slot, || {
            panic!("root fragment panic for test")
        });

        let outcome = dispatcher
            .fetch_result(0, make_finst_id(finst_key.0, finst_key.1), 1, None)
            .expect("fetch root slot");
        let FetchOutcome::Err(message) = outcome else {
            panic!("expected root fragment panic to surface as error");
        };
        assert!(
            message.contains("panicked") && message.contains("root fragment panic for test"),
            "unexpected panic error message: {message}"
        );
    }

    #[test]
    fn root_slot_preserves_first_fragment_error() {
        let slot = RootSlot::new();
        slot.set_error("Mem usage has exceed the limit of BE: BE:10004".to_string());
        slot.set_error("exchange canceled".to_string());

        let guard = slot.state.lock().expect("root slot lock");
        let RootSlotState::Error(message) = &*guard else {
            panic!("expected root slot error");
        };
        assert!(
            message.contains("Mem usage has exceed the limit of BE: BE:10004"),
            "unexpected root slot error: {message}"
        );
    }

    #[test]
    fn fragment_error_is_scoped_to_query() {
        let dispatcher = InProcessDispatcher::default();
        let query_a = (10, 1);
        let query_b = (20, 1);
        let slot_a = RootSlot::new();
        let slot_b = RootSlot::new();
        {
            let mut slots = dispatcher.state.root_slots.lock().expect("root_slots lock");
            slots.insert(
                (10, 2),
                RootSlotEntry {
                    query_key: query_a,
                    slot: Arc::clone(&slot_a),
                },
            );
            slots.insert(
                (20, 2),
                RootSlotEntry {
                    query_key: query_b,
                    slot: Arc::clone(&slot_b),
                },
            );
        }

        record_fragment_error(&dispatcher.state, query_a, "first query failed".to_string());

        let guard_a = slot_a.state.lock().expect("slot a lock");
        assert!(
            matches!(&*guard_a, RootSlotState::Error(message) if message == "first query failed"),
            "query A root slot should receive its fragment error"
        );
        drop(guard_a);
        let guard_b = slot_b.state.lock().expect("slot b lock");
        assert!(
            matches!(&*guard_b, RootSlotState::Running),
            "query B root slot must not inherit query A's fragment error"
        );
    }

    #[test]
    fn cancel_is_idempotent() {
        let dispatcher = InProcessDispatcher::default();
        let ids = vec![make_finst_id(100, 200), make_finst_id(101, 201)];
        // Calling cancel twice must not panic.
        dispatcher.cancel_fragments(0, &ids);
        dispatcher.cancel_fragments(0, &ids);
    }

    #[test]
    fn in_process_dispatcher_rejects_nonzero_backend_idx() {
        let d = InProcessDispatcher::default();
        let err = d
            .submit_fragment(1, make_noop_sink_params(1, 2))
            .expect_err("idx!=0 must err");
        assert!(err.contains("backend_idx") || err.contains("InProcess"));
    }

    #[test]
    fn in_process_dispatcher_backend_count_is_one() {
        assert_eq!(InProcessDispatcher::default().backend_count(), 1);
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
    fn in_process_non_result_fragment_reports_done() {
        let dispatcher = InProcessDispatcher::default();
        let finst_id = crate::common::types::UniqueId { hi: 701, lo: 801 };
        crate::runtime::sink_commit::register(finst_id);

        dispatcher
            .submit_fragment(0, make_noop_sink_params(finst_id.hi, finst_id.lo))
            .expect("submit noop fragment");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while crate::runtime::sink_commit::contains(finst_id) {
            assert!(
                std::time::Instant::now() < deadline,
                "non-result fragment completion must report done and clear sink commit state"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
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
