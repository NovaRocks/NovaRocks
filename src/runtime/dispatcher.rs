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
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::exec::chunk::Chunk;
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::internal_service;
use crate::lower::layout::{build_tuple_slot_order, reorder_tuple_slots};
use crate::lower::thrift::lower_plan;
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_state::RuntimeState;
use crate::{data_sinks, types};
use tracing::warn;

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

fn cancel_all_submitted(state: &InProcessState) {
    for (hi, lo) in submitted_ids_snapshot(state) {
        crate::runtime::exchange::cancel_fragment(hi, lo);
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
            crate::runtime::result_buffer::cancel(crate::common::types::UniqueId {
                hi: fid.hi,
                lo: fid.lo,
            });
            crate::runtime::exchange::cancel_fragment(fid.hi, fid.lo);
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
    fn cancel_all_submitted_reads_ids_at_error_time() {
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
