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
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};

use novarocks::novarocks_logging::{error, info, warn};

use novarocks::common::app_config;
use novarocks::common::config::debug_exec_batch_plan_json;
use novarocks::protocol::starrocks::thrift_codec::{thrift_binary_deserialize, thrift_named_json};

use novarocks::cache::CacheOptions;
use novarocks::common::types::UniqueId;
use novarocks::protocol::FieldPath;
use novarocks::protocol::starrocks::compat::endpoint::destination_address;
use novarocks::protocol::starrocks::compat::request::backfill_per_node_scan_ranges;
use novarocks::protocol::starrocks::decode::{
    StarRocksDecodeInput, StarRocksFragmentDraft, StarRocksReportDestination,
    StarRocksSubmissionMetadata, decode_incremental_scan_ranges, decode_runtime_endpoint,
    finish_fragment_submission, prepare_fragment_submission, snapshot_decode_facts,
};
use novarocks::runtime::exchange;
use novarocks::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentLookupClient, FragmentResultWriter,
    SyncFragmentExecutor,
};
use novarocks::runtime::fragment::{
    DormantFragmentHandle, FragmentCancelReason, FragmentOutcome, RunningFragmentHandle,
    prepare_fragment,
};
use novarocks::runtime::mem_tracker::MemTracker;
use novarocks::runtime::profile::{ProfileUnit, Profiler};
use novarocks::runtime::query_context::{LookupFetcherLifecycle, QueryId};
use novarocks::runtime::query_options::query_expire_durations;
use novarocks::runtime::starrocks_fragment_query::{
    StarRocksFragmentExecution, StarRocksFragmentHandoff, StarRocksFragmentPreStartHandoff,
    StarRocksFragmentQueryRuntime,
};
use novarocks::service::fe_report;
use novarocks::thrift::{data_sinks, descriptors, internal_service, planner, types};

use crate::fragment::admission::{
    DescriptorPreparation, DescriptorTransportCache, PrelaunchCancellationToken, PrelaunchGuard,
    PrelaunchRegistry,
};
use crate::fragment::dependency::resolve_dependencies;

pub struct CompatFragmentService {
    queries: StarRocksFragmentQueryRuntime,
    controls: Arc<CompatFragmentControls>,
    prelaunch: Arc<PrelaunchRegistry>,
    descriptor_cache: Arc<DescriptorTransportCache>,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    event_sink: Arc<dyn FragmentEventSink>,
}

impl CompatFragmentService {
    pub fn new(
        queries: StarRocksFragmentQueryRuntime,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            queries,
            controls: Arc::new(CompatFragmentControls::default()),
            prelaunch: Arc::new(PrelaunchRegistry::default()),
            descriptor_cache: Arc::new(DescriptorTransportCache::default()),
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
        }
    }

    pub fn submit_exec_batch_plan_fragments(&self, thrift_bytes: &[u8]) -> Result<usize, String> {
        submit_exec_batch_plan_fragments_with(self, thrift_bytes)
    }

    pub fn submit_exec_plan_fragment(&self, thrift_bytes: &[u8]) -> Result<(), String> {
        submit_exec_plan_fragment_with(self, thrift_bytes)
    }

    pub fn execute_plan_fragment_sync(
        &self,
        request: internal_service::TExecPlanFragmentParams,
    ) -> Result<UniqueId, String> {
        execute_plan_fragment_sync_with(self, request)
    }

    pub fn cancel_fragment(&self, finst_id: UniqueId) {
        let reason = format!("query canceled by FE: finst={finst_id}");
        if self.prelaunch.cancel_or_run(finst_id, || {
            self.controls.cancel_fragment(finst_id, &reason);
            novarocks::service::fragment_control::cancel_runtime_fragment(finst_id);
        }) {
            info!(
                target: "novarocks::exec",
                finst_id = %finst_id,
                "cancel request marked StarRocks fragment preparation"
            );
        }
    }
}

impl SyncFragmentExecutor for CompatFragmentService {
    fn execute_encoded(&self, payload: &[u8]) -> Result<UniqueId, String> {
        let request = thrift_binary_deserialize(payload)?;
        self.execute_plan_fragment_sync(request)
    }
}

#[cfg(test)]
static TEST_FRAGMENT_LAUNCH_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdapterFailureStage {
    Prepare,
    ReportRegistration,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct AdapterFailurePlan {
    query_id: QueryId,
    stage: AdapterFailureStage,
    index: usize,
}

#[cfg(test)]
static TEST_ADAPTER_FAILURE: Mutex<Option<AdapterFailurePlan>> = Mutex::new(None);

#[cfg(test)]
static TEST_FRAGMENT_LAUNCHES_BY_QUERY: OnceLock<Mutex<HashMap<QueryId, usize>>> = OnceLock::new();

#[cfg(test)]
static TEST_REGISTERED_REPORTS: OnceLock<Mutex<HashMap<QueryId, Vec<UniqueId>>>> = OnceLock::new();

#[cfg(test)]
struct AdapterPausePlan {
    query_id: QueryId,
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}

#[cfg(test)]
static TEST_AFTER_HANDOFF_PAUSE: Mutex<Option<AdapterPausePlan>> = Mutex::new(None);

#[cfg(test)]
fn pause_after_handoff_before_start(query_id: QueryId) {
    let plan = {
        let mut pause = TEST_AFTER_HANDOFF_PAUSE
            .lock()
            .expect("adapter handoff pause lock");
        if pause.as_ref().is_some_and(|plan| plan.query_id == query_id) {
            pause.take()
        } else {
            None
        }
    };
    if let Some(plan) = plan {
        plan.entered.send(()).expect("signal handoff pause");
        plan.release.recv().expect("release handoff pause");
    }
}

#[cfg(not(test))]
fn pause_after_handoff_before_start(_query_id: QueryId) {}

#[cfg(test)]
fn set_adapter_failure(plan: Option<AdapterFailurePlan>) {
    *TEST_ADAPTER_FAILURE
        .lock()
        .expect("adapter failure plan lock") = plan;
}

#[cfg(test)]
fn injected_adapter_failure(query_id: QueryId, stage: AdapterFailureStage, index: usize) -> bool {
    TEST_ADAPTER_FAILURE
        .lock()
        .expect("adapter failure plan lock")
        .is_some_and(|plan| plan.query_id == query_id && plan.stage == stage && plan.index == index)
}

#[cfg(not(test))]
fn injected_adapter_failure(
    _query_id: QueryId,
    _stage: AdapterFailureStage,
    _index: usize,
) -> bool {
    false
}

#[cfg(test)]
fn record_fragment_launch(query_id: QueryId) {
    TEST_FRAGMENT_LAUNCH_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    *TEST_FRAGMENT_LAUNCHES_BY_QUERY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("fragment launch count lock")
        .entry(query_id)
        .or_default() += 1;
}

#[cfg(not(test))]
fn record_fragment_launch(_query_id: QueryId) {}

#[cfg(test)]
fn fragment_launch_count_for_query(query_id: QueryId) -> usize {
    TEST_FRAGMENT_LAUNCHES_BY_QUERY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("fragment launch count lock")
        .get(&query_id)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
fn record_report_registration(query_id: QueryId, finst_id: UniqueId) {
    TEST_REGISTERED_REPORTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("registered reports lock")
        .entry(query_id)
        .or_default()
        .push(finst_id);
}

#[cfg(not(test))]
fn record_report_registration(_query_id: QueryId, _finst_id: UniqueId) {}

#[cfg(test)]
fn record_report_unregistration(query_id: QueryId, finst_id: UniqueId) {
    let mut reports = TEST_REGISTERED_REPORTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("registered reports lock");
    if let Some(ids) = reports.get_mut(&query_id) {
        ids.retain(|current| *current != finst_id);
        if ids.is_empty() {
            reports.remove(&query_id);
        }
    }
}

#[cfg(not(test))]
fn record_report_unregistration(_query_id: QueryId, _finst_id: UniqueId) {}

#[cfg(test)]
fn registered_reports_for_query(query_id: QueryId) -> Vec<UniqueId> {
    TEST_REGISTERED_REPORTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("registered reports lock")
        .get(&query_id)
        .cloned()
        .unwrap_or_default()
}

fn choose_nonempty_str<'a>(primary: Option<&'a str>, fallback: Option<&'a str>) -> Option<&'a str> {
    match primary {
        Some(s) if !s.is_empty() => Some(s),
        _ => match fallback {
            Some(s) if !s.is_empty() => Some(s),
            _ => None,
        },
    }
}

fn validate_network_address(
    addr: Option<&types::TNetworkAddress>,
    missing_msg: &str,
    field_name: &str,
) -> Result<(), String> {
    let addr = addr.ok_or_else(|| missing_msg.to_string())?;
    if addr.hostname.is_empty() {
        return Err(format!("{field_name} hostname is empty"));
    }
    if addr.port <= 0 {
        return Err(format!("{field_name} port must be positive"));
    }
    Ok(())
}

fn validate_nodes_info(
    nodes_info: &descriptors::TNodesInfo,
    field_name: &str,
) -> Result<(), String> {
    for (idx, node) in nodes_info.nodes.iter().enumerate() {
        if node.host.is_empty() {
            return Err(format!("{field_name}[{idx}] host is empty"));
        }
        if node.async_internal_port <= 0 {
            return Err(format!(
                "{field_name}[{idx}] async_internal_port must be positive"
            ));
        }
    }
    Ok(())
}

fn validate_destinations(
    dests: &[data_sinks::TPlanFragmentDestination],
    field_name: &str,
) -> Result<(), String> {
    for (idx, dest) in dests.iter().enumerate() {
        validate_network_address(
            destination_address(dest),
            "missing destination address",
            &format!("{field_name}[{idx}]"),
        )?;
    }
    Ok(())
}

fn validate_internal_addresses(
    exec_params: &internal_service::TPlanFragmentExecParams,
    fragment: Option<&planner::TPlanFragment>,
) -> Result<(), String> {
    if let Some(dests) = exec_params.destinations.as_ref() {
        validate_destinations(dests, "destinations")?;
    }
    if let Some(fragment) = fragment {
        if let Some(plan) = fragment.plan.as_ref() {
            for node in &plan.nodes {
                if let Some(fetch) = node.fetch_node.as_ref()
                    && let Some(nodes_info) = fetch.nodes_info.as_ref()
                {
                    validate_nodes_info(nodes_info, "fetch.nodes_info")?;
                }
            }
        }
        if let Some(sink) = fragment.output_sink.as_ref() {
            match sink.type_ {
                data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                    let Some(multi) = sink.multi_cast_stream_sink.as_ref() else {
                        return Err(
                            "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload"
                                .to_string(),
                        );
                    };
                    for (idx, dests) in multi.destinations.iter().enumerate() {
                        validate_destinations(
                            dests,
                            &format!("multi_cast_stream_sink.destinations[{idx}]"),
                        )?;
                    }
                }
                data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
                    let Some(split) = sink.split_stream_sink.as_ref() else {
                        return Err(
                            "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload".to_string()
                        );
                    };
                    if let Some(destinations) = split.destinations.as_ref() {
                        for (idx, dests) in destinations.iter().enumerate() {
                            validate_destinations(
                                dests,
                                &format!("split_stream_sink.destinations[{idx}]"),
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn append_incremental_scan_ranges(
    queries: &StarRocksFragmentQueryRuntime,
    exec_params: &mut internal_service::TPlanFragmentExecParams,
) -> Result<(), String> {
    backfill_per_node_scan_ranges(exec_params);
    let finst_id = UniqueId {
        hi: exec_params.fragment_instance_id.hi,
        lo: exec_params.fragment_instance_id.lo,
    };
    let mut decoded_updates = Vec::new();
    for (node_id, scan_ranges) in &exec_params.per_node_scan_ranges {
        if scan_ranges.is_empty() {
            continue;
        }
        let change_op_slot = queries.incremental_change_op_slot(finst_id, *node_id)?;
        let decoded = decode_incremental_scan_ranges(*node_id, scan_ranges, change_op_slot)
            .map_err(|error| error.to_string())?;
        decoded_updates.push((*node_id, decoded));
    }
    for (node_id, scan_ranges) in decoded_updates {
        queries.append_incremental_scan_ranges(finst_id, node_id, scan_ranges)?;
    }
    Ok(())
}

fn add_exchange_sender_counts(counts: &mut HashMap<i32, usize>, fragment: &planner::TPlanFragment) {
    let Some(sink) = fragment.output_sink.as_ref() else {
        return;
    };
    match sink.type_ {
        data_sinks::TDataSinkType::DATA_STREAM_SINK => {
            if let Some(stream_sink) = sink.stream_sink.as_ref() {
                *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
            } else {
                warn!(
                    target: "novarocks::exec",
                    "DATA_STREAM_SINK missing stream_sink payload while collecting senders"
                );
            }
        }
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            if let Some(multi) = sink.multi_cast_stream_sink.as_ref() {
                for stream_sink in &multi.sinks {
                    *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
                }
            } else {
                warn!(
                    target: "novarocks::exec",
                    "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload while collecting senders"
                );
            }
        }
        data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            if let Some(split) = sink.split_stream_sink.as_ref() {
                if let Some(sinks) = split.sinks.as_ref() {
                    for stream_sink in sinks {
                        *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
                    }
                } else {
                    warn!(
                        target: "novarocks::exec",
                        "SPLIT_DATA_STREAM_SINK missing sinks while collecting senders"
                    );
                }
            } else {
                warn!(
                    target: "novarocks::exec",
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload while collecting senders"
                );
            }
        }
        _ => {}
    }
}

fn collect_exchange_sender_counts(
    common: Option<&internal_service::TExecPlanFragmentParams>,
    unique: &[internal_service::TExecPlanFragmentParams],
) -> HashMap<i32, usize> {
    let mut counts = HashMap::new();
    if unique.is_empty() {
        if let Some(fragment) = common.and_then(|c| c.fragment.as_ref()) {
            add_exchange_sender_counts(&mut counts, fragment);
        }
        return counts;
    }

    for one in unique {
        let fragment = one
            .fragment
            .as_ref()
            .or_else(|| common.and_then(|c| c.fragment.as_ref()));
        if let Some(fragment) = fragment {
            add_exchange_sender_counts(&mut counts, fragment);
        }
    }
    counts
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LookupCloseTarget {
    lookup_node_id: i32,
    host: String,
    port: i32,
}

struct LookupCloseGuard {
    query_id: QueryId,
    targets: Vec<LookupCloseTarget>,
}

#[derive(Debug)]
struct LookupCloseTask {
    query_id: QueryId,
    target: LookupCloseTarget,
}

struct LookupCloseDispatcher {
    sender: mpsc::SyncSender<LookupCloseTask>,
}

const LOOKUP_CLOSE_WORKERS: usize = 4;
const LOOKUP_CLOSE_QUEUE_CAPACITY: usize = 256;

impl LookupCloseDispatcher {
    fn start() -> Result<Self, String> {
        let (sender, receiver) = mpsc::sync_channel(LOOKUP_CLOSE_QUEUE_CAPACITY);
        let receiver = Arc::new(std::sync::Mutex::new(receiver));
        for index in 0..LOOKUP_CLOSE_WORKERS {
            let receiver = Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("lookup-close-{index}"))
                .spawn(move || lookup_close_worker(receiver))
                .map_err(|error| format!("failed to start lookup_close worker {index}: {error}"))?;
        }
        Ok(Self { sender })
    }

    fn try_dispatch(&self, task: LookupCloseTask) -> Result<(), String> {
        self.sender.try_send(task).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => "lookup_close queue is full".to_string(),
            mpsc::TrySendError::Disconnected(_) => {
                "lookup_close dispatcher is disconnected".to_string()
            }
        })
    }
}

fn lookup_close_dispatcher() -> Result<&'static LookupCloseDispatcher, String> {
    static DISPATCHER: OnceLock<Result<LookupCloseDispatcher, String>> = OnceLock::new();
    DISPATCHER
        .get_or_init(LookupCloseDispatcher::start)
        .as_ref()
        .map_err(Clone::clone)
}

fn lookup_close_worker(receiver: Arc<std::sync::Mutex<mpsc::Receiver<LookupCloseTask>>>) {
    loop {
        let task = {
            let receiver = receiver
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            receiver.recv()
        };
        let Ok(task) = task else {
            return;
        };
        let port = match u16::try_from(task.target.port) {
            Ok(port) => port,
            Err(_) => {
                warn!(
                    target: "novarocks::rpc",
                    query_id = %task.query_id,
                    lookup_node_id = task.target.lookup_node_id,
                    host = %task.target.host,
                    port = task.target.port,
                    "lookup_close skipped: async_internal_port out of u16 range"
                );
                continue;
            }
        };
        if let Err(err) = crate::internal_rpc_client::lookup_close(
            &task.target.host,
            port,
            task.query_id,
            task.target.lookup_node_id,
        ) {
            warn!(
                target: "novarocks::rpc",
                query_id = %task.query_id,
                lookup_node_id = task.target.lookup_node_id,
                host = %task.target.host,
                port,
                error = %err,
                "lookup_close failed"
            );
        }
    }
}

impl Drop for LookupCloseGuard {
    fn drop(&mut self) {
        let dispatcher = match lookup_close_dispatcher() {
            Ok(dispatcher) => dispatcher,
            Err(error) => {
                warn!(
                    target: "novarocks::rpc",
                    query_id = %self.query_id,
                    error = %error,
                    "lookup_close dispatch unavailable"
                );
                return;
            }
        };
        for target in self.targets.drain(..) {
            let lookup_node_id = target.lookup_node_id;
            let host = target.host.clone();
            let port = target.port;
            if let Err(error) = dispatcher.try_dispatch(LookupCloseTask {
                query_id: self.query_id,
                target,
            }) {
                warn!(
                    target: "novarocks::rpc",
                    query_id = %self.query_id,
                    lookup_node_id,
                    host = %host,
                    port,
                    error = %error,
                    "lookup_close dispatch rejected"
                );
            }
        }
    }
}

struct PreparedStarRocksFragment {
    submission: novarocks::runtime::fragment::FragmentSubmission,
    metadata: StarRocksSubmissionMetadata,
    total_fragments: Option<usize>,
}

struct StarRocksFragmentDraftEnvelope {
    draft: StarRocksFragmentDraft,
    total_fragments: Option<usize>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_starrocks_draft(
    fragment: &planner::TPlanFragment,
    descriptor: Option<&descriptors::TDescriptorTable>,
    params: &internal_service::TPlanFragmentExecParams,
    query_opts: Option<&internal_service::TQueryOptions>,
    query_globals: Option<&internal_service::TQueryGlobals>,
    db_name: Option<&str>,
    coord: Option<&types::TNetworkAddress>,
    novarocks_report_addr: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    pipeline_dop: i32,
    group_execution_scan_dop: Option<i32>,
    batch_exchange_sender_counts: &HashMap<i32, usize>,
    typed_result_sink: bool,
) -> Result<StarRocksFragmentDraftEnvelope, String> {
    validate_internal_addresses(params, Some(fragment))?;
    let facts = snapshot_decode_facts(params)?;
    let novarocks_report_endpoint = novarocks_report_addr
        .map(|address| {
            decode_runtime_endpoint(
                address,
                FieldPath::root("exec_plan_fragment").field("novarocks_report_addr"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    let draft = prepare_fragment_submission(StarRocksDecodeInput {
        fragment,
        descriptors: descriptor,
        params,
        query_options: query_opts,
        query_globals,
        db_name,
        coord,
        novarocks_report_endpoint: novarocks_report_endpoint.as_ref(),
        backend_num,
        pipeline_dop,
        group_execution_scan_dop,
        batch_exchange_sender_counts,
        typed_result_sink,
        facts: &facts,
    })
    .map_err(|error| error.to_string())?;
    Ok(StarRocksFragmentDraftEnvelope {
        draft,
        total_fragments: params.instances_number.map(|value| value.max(0) as usize),
    })
}

fn resolve_starrocks_draft(
    draft: &StarRocksFragmentDraftEnvelope,
    token: &PrelaunchCancellationToken,
) -> Result<novarocks::protocol::starrocks::decode::StarRocksResolvedDependencies, String> {
    resolve_dependencies(draft.draft.external_dependencies(), token)
        .map_err(|error| error.to_string())
}

fn finish_starrocks_draft(
    draft: StarRocksFragmentDraftEnvelope,
    resolved: novarocks::protocol::starrocks::decode::StarRocksResolvedDependencies,
) -> Result<PreparedStarRocksFragment, String> {
    let decoded =
        finish_fragment_submission(draft.draft, resolved).map_err(|error| error.to_string())?;
    let (submission, metadata) = decoded.into_parts();
    Ok(PreparedStarRocksFragment {
        submission,
        metadata,
        total_fragments: draft.total_fragments,
    })
}

struct PreparedLaunchResources {
    finst_id: UniqueId,
    profiler: Option<Profiler>,
    query_mem_tracker: Arc<MemTracker>,
    fragment_mem_tracker: Arc<MemTracker>,
    backend_num: i32,
    enable_profile: bool,
    report_interval_ns: Option<i64>,
    report_destination: Option<StarRocksReportDestination>,
    lookup_close_targets: Vec<LookupCloseTarget>,
    dormant: DormantFragmentHandle,
}

fn same_row_position_descriptor(
    left: &novarocks::exec::row_position::RowPositionDescriptor,
    right: &novarocks::exec::row_position::RowPositionDescriptor,
) -> bool {
    left.row_position_type == right.row_position_type
        && left.row_source_slot == right.row_source_slot
        && left.fetch_ref_slots == right.fetch_ref_slots
        && left.lookup_ref_slots == right.lookup_ref_slots
}

fn prepare_query_handoff(
    prepared: &[PreparedStarRocksFragment],
    generation: u64,
) -> Result<StarRocksFragmentHandoff, String> {
    let first = prepared
        .first()
        .ok_or_else(|| "StarRocks handoff requires at least one fragment".to_string())?;
    let query_id = first.submission.query_id();
    let query_options = first.submission.query_options();
    let cache_options = CacheOptions::from_query_options(Some(query_options))?;
    let (delivery_expire, query_expire) = query_expire_durations(Some(query_options));
    let mut descriptor_snapshot = None;
    let mut total_fragments = None;
    let mut row_pos_descs = HashMap::new();
    let mut lookup_fetchers = HashMap::new();
    let mut instances = Vec::with_capacity(prepared.len());

    for item in prepared {
        if item.submission.query_id() != query_id {
            return Err("mixed query_id in prepared StarRocks batch".to_string());
        }
        let incoming_cache =
            CacheOptions::from_query_options(Some(item.submission.query_options()))?;
        if incoming_cache != cache_options {
            return Err("cache options mismatch for query".to_string());
        }
        if let Some(snapshot) = item.metadata.descriptor_snapshot() {
            descriptor_snapshot = Some(Arc::new(snapshot.clone()));
        }
        if let Some(incoming_total) = item.total_fragments {
            total_fragments = Some(
                total_fragments
                    .map_or(incoming_total, |current: usize| current.max(incoming_total)),
            );
        }
        for (tuple_id, incoming) in item.metadata.row_position_descriptors() {
            if let Some(existing) = row_pos_descs.get(tuple_id)
                && !same_row_position_descriptor(existing, incoming)
            {
                return Err(format!(
                    "conflicting row position descriptor for tuple_id={tuple_id}"
                ));
            }
            row_pos_descs
                .entry(*tuple_id)
                .or_insert_with(|| incoming.clone());
        }
        for (node_id, incoming) in item.metadata.lookup_fetcher_lifecycles() {
            lookup_fetchers
                .entry(*node_id)
                .and_modify(|existing| {
                    *existing = match (*existing, *incoming) {
                        (
                            LookupFetcherLifecycle::Exact(current),
                            LookupFetcherLifecycle::Exact(new),
                        ) => LookupFetcherLifecycle::Exact(current.max(new)),
                        (LookupFetcherLifecycle::Unknown, LookupFetcherLifecycle::Exact(new)) => {
                            LookupFetcherLifecycle::Exact(new)
                        }
                        (
                            LookupFetcherLifecycle::Exact(current),
                            LookupFetcherLifecycle::Unknown,
                        ) => LookupFetcherLifecycle::Exact(current),
                        (LookupFetcherLifecycle::Unknown, LookupFetcherLifecycle::Unknown) => {
                            LookupFetcherLifecycle::Unknown
                        }
                    };
                })
                .or_insert(*incoming);
        }
        instances.push((
            item.submission.fragment_instance_id(),
            item.submission.incremental_scan_contracts(),
        ));
    }

    StarRocksFragmentHandoff::new(
        query_id,
        generation,
        delivery_expire,
        query_expire,
        cache_options,
        descriptor_snapshot,
        total_fragments,
        row_pos_descs,
        lookup_fetchers,
        instances,
    )
}

fn profile_report_interval_ns(
    enable_profile: bool,
    query_options: &novarocks::runtime::query_options::QueryOptions,
) -> Option<i64> {
    if !enable_profile {
        return None;
    }
    query_options
        .runtime_profile_report_interval()
        .filter(|value| *value > 0)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .or_else(|| {
            app_config::config()
                .ok()
                .map(|config| config.runtime.profile_report_interval.max(1) * 1_000_000_000)
        })
}

fn launch_prepared_fragments(
    service: &CompatFragmentService,
    prepared: Vec<PreparedStarRocksFragment>,
    descriptor_preparation: DescriptorPreparation,
    guard: PrelaunchGuard,
) -> Result<usize, String> {
    if prepared.is_empty() {
        return Ok(0);
    }
    let query_id = prepared[0].submission.query_id();
    if prepared
        .iter()
        .any(|item| item.submission.query_id() != query_id)
    {
        return Err("mixed query_id in prepared StarRocks batch".to_string());
    }
    let handoff = prepare_query_handoff(&prepared, descriptor_preparation.generation())?;
    let execution = handoff.execution();
    let queries = service.queries.clone();
    let admission = queries.prepare_admission(
        handoff.query_id(),
        handoff.delivery_expire(),
        handoff.query_expire(),
        handoff.cache_options(),
    )?;
    let mut launches = Vec::with_capacity(prepared.len());
    for (prepare_index, prepared) in prepared.into_iter().enumerate() {
        let finst_id = prepared.submission.fragment_instance_id();
        if injected_adapter_failure(query_id, AdapterFailureStage::Prepare, prepare_index + 1) {
            for launch in launches.drain(..).rev() {
                drop(launch);
            }
            return Err(format!(
                "injected StarRocks fragment prepare failure at index {}",
                prepare_index + 1
            ));
        }
        let query_options = prepared.submission.query_options().clone();
        let backend_num = prepared.submission.backend_num();
        let fragment_mem_tracker = admission.fragment_mem_tracker(finst_id);
        let query_mem_tracker = admission.query_mem_tracker();
        let profiler = query_options.enable_profile().then(|| {
            Profiler::new(format!(
                "execute_fragment (plan_node_id={})",
                prepared.submission.root_plan_node_id()
            ))
        });
        let report_interval_ns =
            profile_report_interval_ns(query_options.enable_profile(), &query_options);
        let report_destination = prepared.metadata.report_destination().cloned();
        let lookup_close_targets = prepared
            .metadata
            .lookup_close_targets()
            .iter()
            .map(|target| LookupCloseTarget {
                lookup_node_id: target.lookup_node_id(),
                host: target.host().to_string(),
                port: i32::from(target.port()),
            })
            .collect();
        if prepared.submission.uses_split_data_stream_sink() {
            eprintln!("compat_fragment_sink sink=SPLIT_DATA_STREAM_SINK stage=materialized");
        }
        let prepare_context = prepared.metadata.into_prepare_context(
            profiler.clone(),
            Some(Arc::clone(&fragment_mem_tracker)),
            Arc::clone(&service.exchange_transmitter),
            Arc::clone(&service.lookup_client),
            Arc::clone(&service.result_writer),
            Arc::clone(&service.event_sink),
            finst_id,
        );
        let dormant = match prepare_fragment(prepared.submission, prepare_context) {
            Ok(dormant) => dormant,
            Err(error) => {
                for launch in launches.drain(..).rev() {
                    drop(launch);
                }
                return Err(error.to_string());
            }
        };
        launches.push(PreparedLaunchResources {
            finst_id,
            profiler,
            query_mem_tracker,
            fragment_mem_tracker,
            backend_num,
            enable_profile: query_options.enable_profile(),
            report_interval_ns,
            report_destination,
            lookup_close_targets,
            dormant,
        });
    }
    let created = launches.len();
    let start_gate = Arc::new(BatchStartGate::default());
    let workers = spawn_dormant_workers(
        launches,
        execution,
        queries.clone(),
        Arc::clone(&service.controls),
        Arc::clone(&start_gate),
    )?;
    let worker_finst_ids = workers
        .iter()
        .map(|worker| worker.finst_id)
        .collect::<Vec<_>>();
    let pending_routes = match service.controls.register_pending(
        query_id,
        &worker_finst_ids,
        Arc::clone(&start_gate),
    ) {
        Ok(routes) => routes,
        Err(error) => {
            start_gate.abort(FragmentCancelReason::new(error.clone()));
            abort_dormant_workers(workers);
            return Err(error);
        }
    };
    let committed_handoff = match guard.handoff(|| {
        service
            .descriptor_cache
            .commit_handoff(&descriptor_preparation, |lease_factory| {
                queries.commit_handoff(handoff, || {
                    lease_factory.map(|factory| factory.into_cleanup_lease())
                })
            })
    }) {
        Ok(tracker) => tracker,
        Err(error) => {
            start_gate.abort(FragmentCancelReason::new(error.clone()));
            drop(pending_routes);
            abort_dormant_workers(workers);
            return Err(error);
        }
    };
    let committed_query_mem_tracker = committed_handoff.query_mem_tracker();
    let mut pre_start = Some(committed_handoff.into_pre_start());
    debug_assert!(Arc::ptr_eq(
        &committed_query_mem_tracker,
        &admission.query_mem_tracker()
    ));
    let registered_reports = match register_fragment_reports(&workers, execution) {
        Ok(registered) => registered,
        Err((error, registered)) => {
            rollback_committed_launch(
                workers,
                &start_gate,
                pre_start.take().expect("committed pre-start handoff"),
                pending_routes,
                &registered,
            );
            return Err(error);
        }
    };
    pause_after_handoff_before_start(query_id);
    if !start_gate.start(|| {
        pre_start
            .take()
            .expect("committed pre-start handoff")
            .start();
    }) {
        rollback_committed_launch(
            workers,
            &start_gate,
            pre_start.take().expect("aborted pre-start handoff"),
            pending_routes,
            &registered_reports,
        );
        return Err("StarRocks fragment batch was cancelled before start".to_string());
    }
    pending_routes.handoff();
    for worker in workers {
        worker.detach();
    }
    drop(admission);
    Ok(created)
}

fn register_fragment_reports(
    workers: &[DormantWorker],
    execution: StarRocksFragmentExecution,
) -> Result<Vec<UniqueId>, (String, Vec<UniqueId>)> {
    let query_id = execution.query_id();
    let mut registered = Vec::new();
    for (registration_index, worker) in workers.iter().enumerate() {
        if injected_adapter_failure(
            query_id,
            AdapterFailureStage::ReportRegistration,
            registration_index + 1,
        ) {
            return Err((
                format!(
                    "injected StarRocks report registration failure at index {}",
                    registration_index + 1
                ),
                registered,
            ));
        }
        match worker.report_destination.as_ref() {
            Some(StarRocksReportDestination::NovaRocks(endpoint)) => {
                fe_report::register_novarocks_instance(
                    worker.finst_id,
                    query_id,
                    endpoint.clone(),
                    worker.backend_num,
                    worker.enable_profile,
                    worker.profiler.clone(),
                    Some(Arc::clone(&worker.fragment_mem_tracker)),
                    Some(Arc::clone(&worker.query_mem_tracker)),
                    worker.report_interval_ns,
                );
                registered.push(worker.finst_id);
                record_report_registration(query_id, worker.finst_id);
            }
            Some(StarRocksReportDestination::Coordinator(endpoint)) => {
                fe_report::register_instance(
                    worker.finst_id,
                    query_id,
                    types::TNetworkAddress::new(endpoint.host().to_string(), endpoint.port()),
                    worker.backend_num,
                    worker.enable_profile,
                    worker.profiler.clone(),
                    Some(Arc::clone(&worker.fragment_mem_tracker)),
                    Some(Arc::clone(&worker.query_mem_tracker)),
                    worker.report_interval_ns,
                );
                registered.push(worker.finst_id);
                record_report_registration(query_id, worker.finst_id);
            }
            None => warn!(
                target: "novarocks::report",
                finst_id = %worker.finst_id,
                "missing report destination for reportExecStatus"
            ),
        }
    }
    Ok(registered)
}

fn unregister_fragment_reports(query_id: QueryId, registered: &[UniqueId]) {
    for finst_id in registered.iter().rev() {
        fe_report::unregister_instance(*finst_id);
        record_report_unregistration(query_id, *finst_id);
    }
}

fn rollback_committed_launch(
    workers: Vec<DormantWorker>,
    start_gate: &BatchStartGate,
    pre_start: StarRocksFragmentPreStartHandoff,
    pending_routes: PendingFragmentRoutes,
    registered_reports: &[UniqueId],
) {
    unregister_fragment_reports(pre_start.execution().query_id(), registered_reports);
    let rolled_back = pre_start.rollback();
    debug_assert!(
        rolled_back,
        "committed StarRocks handoff must be rollbackable before batch start"
    );
    start_gate.abort(FragmentCancelReason::new(
        "StarRocks fragment batch rolled back before start",
    ));
    drop(pending_routes);
    join_dormant_workers(workers);
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum BatchStartState {
    #[default]
    Pending,
    Started,
    Aborted,
}

#[derive(Debug, Default)]
struct BatchStartGate {
    state: Mutex<BatchStartGateState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct BatchStartGateState {
    phase: BatchStartState,
    cancel_reason: Option<FragmentCancelReason>,
}

impl BatchStartGate {
    fn wait(&self) -> BatchStartState {
        let mut state = self.state.lock().expect("batch start gate lock");
        while state.phase == BatchStartState::Pending {
            state = self
                .changed
                .wait(state)
                .expect("batch start gate wait lock");
        }
        state.phase
    }

    fn start(&self, seal_pre_start: impl FnOnce()) -> bool {
        let mut state = self.state.lock().expect("batch start gate lock");
        if state.phase != BatchStartState::Pending {
            return false;
        }
        seal_pre_start();
        state.phase = BatchStartState::Started;
        self.changed.notify_all();
        true
    }

    fn abort(&self, reason: FragmentCancelReason) {
        let mut state = self.state.lock().expect("batch start gate lock");
        if state.cancel_reason.is_none() {
            state.cancel_reason = Some(reason);
        }
        if state.phase == BatchStartState::Pending {
            state.phase = BatchStartState::Aborted;
            self.changed.notify_all();
        }
    }

    fn cancellation_reason(&self) -> Option<FragmentCancelReason> {
        self.state
            .lock()
            .expect("batch start gate lock")
            .cancel_reason
            .clone()
    }
}

struct DormantWorker {
    finst_id: UniqueId,
    profiler: Option<Profiler>,
    query_mem_tracker: Arc<MemTracker>,
    fragment_mem_tracker: Arc<MemTracker>,
    backend_num: i32,
    enable_profile: bool,
    report_interval_ns: Option<i64>,
    report_destination: Option<StarRocksReportDestination>,
    join: std::thread::JoinHandle<()>,
}

impl DormantWorker {
    fn detach(self) {
        drop(self.join);
    }
}

#[derive(Default)]
struct CompatFragmentControls {
    routes: std::sync::Mutex<HashMap<UniqueId, CompatFragmentRoute>>,
}

enum CompatFragmentRoute {
    Pending {
        query_id: QueryId,
        gate: Arc<BatchStartGate>,
    },
    Running {
        query_id: QueryId,
        handle: RunningFragmentHandle,
    },
}

impl CompatFragmentControls {
    #[cfg(test)]
    fn has_running_route(&self, query_id: QueryId, finst_id: UniqueId) -> bool {
        self.routes
            .lock()
            .expect("compat fragment controls lock")
            .get(&finst_id)
            .is_some_and(|route| {
                matches!(
                    route,
                    CompatFragmentRoute::Running {
                        query_id: current,
                        ..
                    } if *current == query_id
                )
            })
    }

    fn register_pending(
        self: &Arc<Self>,
        query_id: QueryId,
        finst_ids: &[UniqueId],
        gate: Arc<BatchStartGate>,
    ) -> Result<PendingFragmentRoutes, String> {
        let mut routes = self.routes.lock().expect("compat fragment controls lock");
        if let Some(finst_id) = finst_ids
            .iter()
            .find(|finst_id| routes.contains_key(finst_id))
        {
            return Err(format!(
                "compat fragment cancellation route already exists: finst_id={finst_id}"
            ));
        }
        for finst_id in finst_ids {
            routes.insert(
                *finst_id,
                CompatFragmentRoute::Pending {
                    query_id,
                    gate: Arc::clone(&gate),
                },
            );
        }
        Ok(PendingFragmentRoutes {
            controls: Arc::clone(self),
            query_id,
            finst_ids: finst_ids.to_vec(),
            gate,
            active: true,
        })
    }

    fn publish(
        &self,
        query_id: QueryId,
        finst_id: UniqueId,
        gate: &Arc<BatchStartGate>,
        handle: RunningFragmentHandle,
    ) {
        let published = {
            let mut routes = self.routes.lock().expect("compat fragment controls lock");
            let matches_pending = routes.get(&finst_id).is_some_and(|route| {
                matches!(
                    route,
                    CompatFragmentRoute::Pending {
                        query_id: current,
                        gate: current_gate,
                    } if *current == query_id && Arc::ptr_eq(current_gate, gate)
                )
            });
            if matches_pending {
                routes.insert(
                    finst_id,
                    CompatFragmentRoute::Running {
                        query_id,
                        handle: handle.clone(),
                    },
                );
            }
            matches_pending
        };
        if published && let Some(reason) = gate.cancellation_reason() {
            handle.cancel(reason);
        }
    }

    fn remove(&self, query_id: QueryId, finst_id: UniqueId) {
        let mut routes = self.routes.lock().expect("compat fragment controls lock");
        if routes.get(&finst_id).is_some_and(|route| match route {
            CompatFragmentRoute::Pending {
                query_id: current, ..
            }
            | CompatFragmentRoute::Running {
                query_id: current, ..
            } => *current == query_id,
        }) {
            routes.remove(&finst_id);
        }
    }

    fn cancel_query(&self, query_id: QueryId, reason: &str) {
        let (gates, handles) = {
            let routes = self.routes.lock().expect("compat fragment controls lock");
            let mut gates = Vec::new();
            let mut handles = Vec::new();
            for route in routes.values() {
                match route {
                    CompatFragmentRoute::Pending {
                        query_id: current,
                        gate,
                    } if *current == query_id => {
                        if !gates.iter().any(|current| Arc::ptr_eq(current, gate)) {
                            gates.push(Arc::clone(gate));
                        }
                    }
                    CompatFragmentRoute::Running {
                        query_id: current,
                        handle,
                    } if *current == query_id => handles.push(handle.clone()),
                    _ => {}
                }
            }
            (gates, handles)
        };
        let reason = FragmentCancelReason::new(reason);
        for gate in gates {
            gate.abort(reason.clone());
        }
        for handle in handles {
            handle.cancel(reason.clone());
        }
    }

    fn cancel_fragment(&self, finst_id: UniqueId, reason: &str) {
        let query_id = self
            .routes
            .lock()
            .expect("compat fragment controls lock")
            .get(&finst_id)
            .map(|route| match route {
                CompatFragmentRoute::Pending { query_id, .. }
                | CompatFragmentRoute::Running { query_id, .. } => *query_id,
            });
        if let Some(query_id) = query_id {
            self.cancel_query(query_id, reason);
        }
    }
}

struct PendingFragmentRoutes {
    controls: Arc<CompatFragmentControls>,
    query_id: QueryId,
    finst_ids: Vec<UniqueId>,
    gate: Arc<BatchStartGate>,
    active: bool,
}

impl PendingFragmentRoutes {
    fn handoff(mut self) {
        self.active = false;
    }
}

impl Drop for PendingFragmentRoutes {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut routes = self
            .controls
            .routes
            .lock()
            .expect("compat fragment controls lock");
        for finst_id in self.finst_ids.iter().rev() {
            let owned = routes.get(finst_id).is_some_and(|route| {
                matches!(
                    route,
                    CompatFragmentRoute::Pending {
                        query_id,
                        gate,
                    } if *query_id == self.query_id && Arc::ptr_eq(gate, &self.gate)
                )
            });
            if owned {
                routes.remove(finst_id);
            }
        }
    }
}

fn spawn_dormant_workers(
    launches: Vec<PreparedLaunchResources>,
    execution: StarRocksFragmentExecution,
    queries: StarRocksFragmentQueryRuntime,
    controls: Arc<CompatFragmentControls>,
    start_gate: Arc<BatchStartGate>,
) -> Result<Vec<DormantWorker>, String> {
    let mut workers: Vec<DormantWorker> = Vec::with_capacity(launches.len());
    for launch in launches {
        let finst_id = launch.finst_id;
        let query_id = execution.query_id();
        let profiler_for_wall = launch.profiler.clone();
        let queries = queries.clone();
        let controls = Arc::clone(&controls);
        let worker_start_gate = Arc::clone(&start_gate);
        let join = match std::thread::Builder::new()
            .name(format!(
                "compat-fragment-{:x}-{:x}",
                finst_id.hi, finst_id.lo
            ))
            .spawn(move || {
                if worker_start_gate.wait() == BatchStartState::Aborted {
                    return;
                }
                record_fragment_launch(query_id);
                let wall_start = std::time::Instant::now();
                let completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _lookup_close_guard = LookupCloseGuard {
                        query_id,
                        targets: launch.lookup_close_targets,
                    };
                    let running = launch.dormant.start();
                    controls.publish(query_id, finst_id, &worker_start_gate, running.clone());
                    let fact = running.join();
                    controls.remove(query_id, finst_id);
                    (running, fact)
                }));
                let (running, report_error) = match completion {
                    Ok((running, fact)) => {
                        let error = match fact.outcome() {
                            FragmentOutcome::Succeeded => {
                                running.handoff_sink_commit();
                                None
                            }
                            FragmentOutcome::Failed(error) => Some(error.to_string()),
                            FragmentOutcome::Cancelled { reason } => {
                                Some(format!("fragment cancelled: {}", reason.detail()))
                            }
                        };
                        (Some(running), error)
                    }
                    Err(payload) => {
                        controls.remove(query_id, finst_id);
                        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = payload.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic payload".to_string()
                        };
                        (None, Some(format!("panic in fragment execution: {msg}")))
                    }
                };
                if let Some(profiler) = profiler_for_wall.as_ref() {
                    let elapsed_ns = novarocks::runtime::profile::clamp_u128_to_i64(
                        wall_start.elapsed().as_nanos(),
                    );
                    profiler.counter_set("QueryExecutionWallTime", ProfileUnit::TimeNs, elapsed_ns);
                }
                if let Some(error_message) = report_error.as_ref() {
                    error!(
                        target: "novarocks::exec",
                        finst_id = %finst_id,
                        error = %error_message,
                        "exec_plan_fragment failed"
                    );
                }
                run_async_cleanup_sequence(
                    report_error,
                    |error| {
                        controls.cancel_query(query_id, error);
                        for id in queries.cancel_query(execution, error.to_string()) {
                            exchange::cancel_fragment(id.hi, id.lo);
                        }
                    },
                    || queries.finish_fragment_for_report(execution),
                    |error, decision| {
                        fe_report::report_fragment_done(
                            finst_id,
                            error,
                            decision.include_runtime_filter_profile(),
                        );
                    },
                    || exchange::remove_fragment(finst_id.hi, finst_id.lo),
                    || queries.unregister_fragment(finst_id, execution),
                    |decision| queries.cleanup_after_fragment_report(query_id, decision),
                );
                drop(running);
            }) {
            Ok(join) => join,
            Err(error) => {
                start_gate.abort(FragmentCancelReason::new(format!(
                    "spawn StarRocks fragment adapter worker failed: {error}"
                )));
                join_dormant_workers(workers);
                return Err(format!(
                    "spawn StarRocks fragment adapter worker failed: {error}"
                ));
            }
        };
        workers.push(DormantWorker {
            finst_id,
            profiler: launch.profiler,
            query_mem_tracker: launch.query_mem_tracker,
            fragment_mem_tracker: launch.fragment_mem_tracker,
            backend_num: launch.backend_num,
            enable_profile: launch.enable_profile,
            report_interval_ns: launch.report_interval_ns,
            report_destination: launch.report_destination,
            join,
        });
    }
    Ok(workers)
}

fn abort_dormant_workers(workers: Vec<DormantWorker>) {
    join_dormant_workers(workers);
}

fn join_dormant_workers(workers: Vec<DormantWorker>) {
    for worker in workers.into_iter().rev() {
        let _ = worker.join.join();
    }
}

fn run_async_cleanup_sequence<T>(
    report_error: Option<String>,
    cancel: impl FnOnce(&str),
    finish_for_report: impl FnOnce() -> T,
    report_done: impl FnOnce(Option<String>, &T),
    remove_exchange: impl FnOnce(),
    unregister_finst: impl FnOnce(),
    cleanup_after_report: impl FnOnce(T),
) {
    if let Some(error) = report_error.as_deref() {
        cancel(error);
    }
    let decision = finish_for_report();
    report_done(report_error, &decision);
    remove_exchange();
    unregister_finst();
    cleanup_after_report(decision);
}

fn submit_exec_batch_plan_fragments_with(
    service: &CompatFragmentService,
    thrift_bytes: &[u8],
) -> Result<usize, String> {
    let batch: internal_service::TExecBatchPlanFragmentsParams =
        thrift_binary_deserialize(thrift_bytes)?;
    if debug_exec_batch_plan_json() {
        match thrift_named_json(&batch) {
            Ok(json) => info!(
                target: "novarocks::rpc",
                rpc = "exec_batch_plan_fragments",
                named_json = %json,
                "named_json"
            ),
            Err(e) => warn!(
                target: "novarocks::rpc",
                rpc = "exec_batch_plan_fragments",
                error = %e,
                "named_json_failed"
            ),
        }
    }
    let common = batch.common_param.as_ref();
    let unique = batch.unique_param_per_instance.unwrap_or_default();
    if unique.is_empty() {
        return Ok(0);
    }
    let sender_counts = collect_exchange_sender_counts(common, &unique);
    let common_desc_tbl = common.and_then(|value| value.desc_tbl.as_ref());
    let mut envelopes = Vec::with_capacity(unique.len());
    let mut finst_ids = Vec::with_capacity(unique.len());
    let mut query_id_for_batch = None;
    for one in &unique {
        let params = one
            .params
            .as_ref()
            .or_else(|| common.and_then(|c| c.params.as_ref()));
        let fragment = one
            .fragment
            .as_ref()
            .or_else(|| common.and_then(|c| c.fragment.as_ref()));
        let coord = one
            .coord
            .as_ref()
            .or_else(|| common.and_then(|c| c.coord.as_ref()));
        let novarocks_report_addr = one
            .novarocks_report_addr
            .clone()
            .or_else(|| common.and_then(|c| c.novarocks_report_addr.clone()));
        let typed_result_sink = one
            .novarocks_typed_result_sink
            .or_else(|| common.and_then(|c| c.novarocks_typed_result_sink))
            .unwrap_or(false);
        let backend_num = one
            .backend_num
            .or_else(|| common.and_then(|c| c.backend_num));
        // NOTE: backend_num must match FE's instance index (ExecutionDAG index).
        // If this value is wrong, FE will treat reportExecStatus as "unknown backend number"
        // and drop sink_commit_infos, causing Iceberg commit to be skipped.
        let db_name = choose_nonempty_str(
            one.db_name.as_deref(),
            common.and_then(|c| c.db_name.as_deref()),
        );
        let query_opts = one
            .query_options
            .as_ref()
            .or(common.and_then(|c| c.query_options.as_ref()));
        let query_globals = one
            .query_globals
            .as_ref()
            .or_else(|| common.and_then(|c| c.query_globals.as_ref()));
        let exec_params = params.ok_or_else(|| {
            "missing params in exec_batch_plan_fragments unique instance".to_string()
        })?;
        let fragment = fragment.ok_or_else(|| {
            "missing fragment in exec_batch_plan_fragments unique instance".to_string()
        })?;

        let query_id = QueryId::new(exec_params.query_id.hi, exec_params.query_id.lo);
        if let Some(existing) = query_id_for_batch {
            if existing != query_id {
                return Err("mixed query_id in exec_batch_plan_fragments".to_string());
            }
        } else {
            query_id_for_batch = Some(query_id);
        }

        let finst_id = UniqueId {
            hi: exec_params.fragment_instance_id.hi,
            lo: exec_params.fragment_instance_id.lo,
        };
        let mut exec_params = exec_params.clone();
        backfill_per_node_scan_ranges(&mut exec_params);
        finst_ids.push(finst_id);
        envelopes.push((
            fragment,
            exec_params,
            query_opts,
            query_globals,
            db_name,
            coord,
            novarocks_report_addr,
            backend_num,
            resolve_pipeline_dop(one),
            one.group_execution_scan_dop,
            typed_result_sink,
            one.desc_tbl.as_ref(),
        ));
    }
    let query_id = query_id_for_batch.expect("non-empty batch has query id");
    let unique_descriptors = envelopes.iter().map(|entry| entry.11).collect::<Vec<_>>();
    let descriptor_preparation =
        service
            .descriptor_cache
            .prepare_batch(query_id, common_desc_tbl, &unique_descriptors)?;
    let generation = descriptor_preparation.generation();
    let mut guard = service
        .prelaunch
        .install(query_id, generation, finst_ids.clone())?;
    let frontend_endpoint = envelopes
        .first()
        .and_then(|entry| entry.5)
        .map(|address| {
            decode_runtime_endpoint(
                address,
                FieldPath::root("exec_plan_fragment").field("coord"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    guard.set_frontend_endpoint(frontend_endpoint);
    let token = guard.cancellation_token();
    let mut drafts = Vec::with_capacity(envelopes.len());
    for entry in envelopes {
        drafts.push(prepare_starrocks_draft(
            entry.0,
            descriptor_preparation.descriptor(),
            &entry.1,
            entry.2,
            entry.3,
            entry.4,
            entry.5,
            entry.6.as_ref(),
            entry.7,
            entry.8,
            entry.9,
            &sender_counts,
            entry.10,
        )?);
    }
    token.check(0).map_err(|error| error.to_string())?;
    let resolutions = drafts
        .iter()
        .map(|draft| resolve_starrocks_draft(draft, &token))
        .collect::<Result<Vec<_>, _>>()?;
    token.check(0).map_err(|error| error.to_string())?;
    let prepared = drafts
        .into_iter()
        .zip(resolutions)
        .map(|(draft, resolved)| finish_starrocks_draft(draft, resolved))
        .collect::<Result<Vec<_>, _>>()?;
    launch_prepared_fragments(service, prepared, descriptor_preparation, guard)
}

fn submit_exec_plan_fragment_with(
    service: &CompatFragmentService,
    thrift_bytes: &[u8],
) -> Result<(), String> {
    let one: internal_service::TExecPlanFragmentParams = thrift_binary_deserialize(thrift_bytes)?;
    if debug_exec_batch_plan_json() {
        match thrift_named_json(&one) {
            Ok(json) => info!(
                target: "novarocks::rpc",
                rpc = "exec_plan_fragment",
                named_json = %json,
                "named_json"
            ),
            Err(e) => warn!(
                target: "novarocks::rpc",
                rpc = "exec_plan_fragment",
                error = %e,
                "named_json_failed"
            ),
        }
    }
    let Some(params) = one.params.as_ref() else {
        return Err("missing params in TExecPlanFragmentParams".to_string());
    };
    if one.fragment.is_none() {
        let mut params = params.clone();
        append_incremental_scan_ranges(&service.queries, &mut params)?;
        return Ok(());
    }
    let fragment = one.fragment.as_ref().expect("checked above");
    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId::new(params.query_id.hi, params.query_id.lo);
    let mut params = params.clone();
    backfill_per_node_scan_ranges(&mut params);
    let descriptor_preparation =
        service
            .descriptor_cache
            .prepare(query_id, one.desc_tbl.as_ref(), None)?;
    let mut guard =
        service
            .prelaunch
            .install(query_id, descriptor_preparation.generation(), [finst_id])?;
    guard.set_frontend_endpoint(
        one.coord
            .as_ref()
            .map(|address| {
                decode_runtime_endpoint(
                    address,
                    FieldPath::root("exec_plan_fragment").field("coord"),
                )
                .map_err(|error| error.to_string())
            })
            .transpose()?,
    );
    let token = guard.cancellation_token();
    let draft = prepare_starrocks_draft(
        fragment,
        descriptor_preparation.descriptor(),
        &params,
        one.query_options.as_ref(),
        one.query_globals.as_ref(),
        one.db_name.as_deref(),
        one.coord.as_ref(),
        one.novarocks_report_addr.as_ref(),
        one.backend_num,
        resolve_pipeline_dop(&one),
        one.group_execution_scan_dop,
        &HashMap::new(),
        one.novarocks_typed_result_sink.unwrap_or(false),
    )?;
    let resolved = resolve_starrocks_draft(&draft, &token)?;
    let prepared = finish_starrocks_draft(draft, resolved)?;
    launch_prepared_fragments(service, vec![prepared], descriptor_preparation, guard)?;
    Ok(())
}

fn execute_plan_fragment_sync_with(
    service: &CompatFragmentService,
    one: internal_service::TExecPlanFragmentParams,
) -> Result<UniqueId, String> {
    let Some(params) = one.params.as_ref() else {
        return Err("missing params in TExecPlanFragmentParams".to_string());
    };
    let Some(fragment) = one.fragment.as_ref() else {
        return Err("missing fragment in TExecPlanFragmentParams".to_string());
    };

    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId::new(params.query_id.hi, params.query_id.lo);

    let mut params = params.clone();
    backfill_per_node_scan_ranges(&mut params);
    let descriptor_preparation =
        service
            .descriptor_cache
            .prepare(query_id, one.desc_tbl.as_ref(), None)?;
    let mut guard =
        service
            .prelaunch
            .install(query_id, descriptor_preparation.generation(), [finst_id])?;
    guard.set_frontend_endpoint(
        one.coord
            .as_ref()
            .map(|address| {
                decode_runtime_endpoint(
                    address,
                    FieldPath::root("exec_plan_fragment").field("coord"),
                )
                .map_err(|error| error.to_string())
            })
            .transpose()?,
    );
    let token = guard.cancellation_token();
    let draft = prepare_starrocks_draft(
        fragment,
        descriptor_preparation.descriptor(),
        &params,
        one.query_options.as_ref(),
        one.query_globals.as_ref(),
        one.db_name.as_deref(),
        one.coord.as_ref(),
        one.novarocks_report_addr.as_ref(),
        one.backend_num,
        resolve_pipeline_dop(&one),
        one.group_execution_scan_dop,
        &HashMap::new(),
        one.novarocks_typed_result_sink.unwrap_or(false),
    )?;
    let resolved = resolve_starrocks_draft(&draft, &token)?;
    let prepared = finish_starrocks_draft(draft, resolved)?;

    let lookup_close_targets = prepared
        .metadata
        .lookup_close_targets()
        .iter()
        .map(|target| LookupCloseTarget {
            lookup_node_id: target.lookup_node_id(),
            host: target.host().to_string(),
            port: i32::from(target.port()),
        })
        .collect();

    let handoff = prepare_query_handoff(
        std::slice::from_ref(&prepared),
        descriptor_preparation.generation(),
    )?;
    let execution = handoff.execution();
    let queries = service.queries.clone();
    let admission = queries.prepare_admission(
        handoff.query_id(),
        handoff.delivery_expire(),
        handoff.query_expire(),
        handoff.cache_options(),
    )?;
    let fragment_mem_tracker = admission.fragment_mem_tracker(finst_id);
    let prepare_context = prepared.metadata.into_prepare_context(
        None,
        Some(Arc::clone(&fragment_mem_tracker)),
        Arc::clone(&service.exchange_transmitter),
        Arc::clone(&service.lookup_client),
        Arc::clone(&service.result_writer),
        Arc::clone(&service.event_sink),
        finst_id,
    );
    let dormant = prepare_fragment(prepared.submission, prepare_context)
        .map_err(|error| error.to_string())?;
    let start_gate = Arc::new(BatchStartGate::default());
    let pending_routes =
        service
            .controls
            .register_pending(query_id, &[finst_id], Arc::clone(&start_gate))?;
    let committed_handoff = guard.handoff(|| {
        service
            .descriptor_cache
            .commit_handoff(&descriptor_preparation, |lease_factory| {
                queries.commit_handoff(handoff, || {
                    lease_factory.map(|factory| factory.into_cleanup_lease())
                })
            })
    })?;
    let query_mem_tracker = committed_handoff.query_mem_tracker();
    debug_assert!(Arc::ptr_eq(
        &query_mem_tracker,
        &admission.query_mem_tracker()
    ));
    let mut pre_start = Some(committed_handoff.into_pre_start());
    if !start_gate.start(|| {
        pre_start
            .take()
            .expect("committed sync pre-start handoff")
            .start();
    }) {
        let rolled_back = pre_start
            .take()
            .expect("aborted sync pre-start handoff")
            .rollback();
        debug_assert!(rolled_back, "sync pre-start handoff must be rollbackable");
        drop(pending_routes);
        return Err("StarRocks sync fragment was cancelled before start".to_string());
    }
    let execution_result = {
        let _lookup_close_guard = LookupCloseGuard {
            query_id,
            targets: lookup_close_targets,
        };
        let running = dormant.start();
        service
            .controls
            .publish(query_id, finst_id, &start_gate, running.clone());
        pending_routes.handoff();
        let fact = running.join();
        service.controls.remove(query_id, finst_id);
        let result = match fact.outcome() {
            FragmentOutcome::Succeeded => {
                running.handoff_sink_commit();
                Ok(())
            }
            FragmentOutcome::Failed(error) => Err(error.to_string()),
            FragmentOutcome::Cancelled { reason } => {
                Err(format!("fragment cancelled: {}", reason.detail()))
            }
        };
        drop(running);
        result
    };
    exchange::remove_fragment(finst_id.hi, finst_id.lo);
    queries.unregister_fragment(finst_id, execution);
    queries.finish_fragment(execution);
    drop(admission);

    match execution_result {
        Ok(()) => Ok(finst_id),
        Err(error) => Err(error),
    }
}

fn resolve_pipeline_dop(request: &internal_service::TExecPlanFragmentParams) -> i32 {
    // Align with StarRocks: pipeline_dop is a per-fragment-instance (unique request) parameter.
    novarocks::runtime::exec_env::calc_pipeline_dop(request.pipeline_dop.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use novarocks::common::types::UniqueId;
    use novarocks::protocol::starrocks::thrift_codec::{
        thrift_binary_deserialize, thrift_binary_serialize,
    };
    use novarocks::runtime::fragment::io::SyncFragmentExecutor;
    use novarocks::runtime::query_context::QueryId;
    use novarocks::thrift::{
        data_sinks, descriptors, internal_service, partitions, plan_nodes, planner, types,
    };

    use super::{
        AdapterFailurePlan, AdapterFailureStage, AdapterPausePlan, CompatFragmentService,
        TEST_AFTER_HANDOFF_PAUSE, TEST_FRAGMENT_LAUNCH_COUNT, fragment_launch_count_for_query,
        registered_reports_for_query, run_async_cleanup_sequence, set_adapter_failure,
    };

    static FAILURE_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct AdapterFailureReset;

    impl Drop for AdapterFailureReset {
        fn drop(&mut self) {
            set_adapter_failure(None);
        }
    }

    fn inject_failure(
        query: UniqueId,
        stage: AdapterFailureStage,
        index: usize,
    ) -> AdapterFailureReset {
        set_adapter_failure(Some(AdapterFailurePlan {
            query_id: runtime_query_id(query),
            stage,
            index,
        }));
        AdapterFailureReset
    }

    fn runtime_query_id(query: UniqueId) -> QueryId {
        QueryId::new(query.hi, query.lo)
    }

    fn fragment_service() -> CompatFragmentService {
        CompatFragmentService::new(
            novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime::new(),
            crate::fragment::brpc_exchange_transmitter(),
            crate::fragment::brpc_fragment_lookup_client(),
            crate::fragment::compat_result_writer(),
            crate::fragment::compat_fragment_event_sink(),
        )
    }

    fn empty_set_node() -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode::new(
            11,
            plan_nodes::TPlanNodeType::EMPTY_SET_NODE,
            0,
            -1,
            vec![],
            vec![],
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn noop_sink() -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::NOOP_SINK,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn fragment(plan: Option<plan_nodes::TPlan>) -> planner::TPlanFragment {
        planner::TPlanFragment::new(
            plan,
            None,
            noop_sink(),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None,
                None,
                None,
            ),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn params(query: UniqueId, finst: UniqueId) -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId::new(query.hi, query.lo),
            types::TUniqueId::new(finst.hi, finst.lo),
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn request(
        query: UniqueId,
        finst: UniqueId,
        fragment: planner::TPlanFragment,
    ) -> internal_service::TExecPlanFragmentParams {
        internal_service::TExecPlanFragmentParams::new(
            internal_service::InternalServiceVersion::V1,
            Some(fragment),
            None,
            Some(params(query, finst)),
            None,
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(true),
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn query_options_with_cache_probability(probability: i32) -> internal_service::TQueryOptions {
        let mut options: internal_service::TQueryOptions =
            thrift_binary_deserialize(&[0]).expect("empty query options");
        options.datacache_evict_probability = Some(probability);
        options
    }

    fn valid_batch(
        query: UniqueId,
        finst_ids: &[UniqueId],
        report_to_coordinator: bool,
    ) -> internal_service::TExecBatchPlanFragmentsParams {
        let requests = finst_ids
            .iter()
            .map(|finst_id| {
                let mut request = request(
                    query,
                    *finst_id,
                    fragment(Some(plan_nodes::TPlan::new(vec![empty_set_node()]))),
                );
                if report_to_coordinator {
                    request.coord =
                        Some(types::TNetworkAddress::new("127.0.0.1".to_string(), 65_000));
                }
                request
            })
            .collect();
        internal_service::TExecBatchPlanFragmentsParams::new(None, Some(requests))
    }

    fn blocking_exchange_request(
        query: UniqueId,
        finst: UniqueId,
    ) -> internal_service::TExecPlanFragmentParams {
        let mut exchange = empty_set_node();
        exchange.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
        exchange.exchange_node = Some(plan_nodes::TExchangeNode::new(
            vec![1],
            None,
            None,
            None,
            None,
            None,
        ));
        let mut request = request(
            query,
            finst,
            fragment(Some(plan_nodes::TPlan::new(vec![exchange]))),
        );
        request.desc_tbl = Some(descriptors::TDescriptorTable::new(
            Some(vec![]),
            vec![descriptors::TTupleDescriptor::new(
                Some(1),
                None,
                None,
                None,
                None,
            )],
            None,
            None,
        ));
        request
            .params
            .as_mut()
            .expect("fragment params")
            .per_exch_num_senders
            .insert(11, 1);
        request
    }

    fn wait_for_query_launches(query_id: UniqueId, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while fragment_launch_count_for_query(runtime_query_id(query_id)) < expected
            && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(
            fragment_launch_count_for_query(runtime_query_id(query_id)),
            expected,
            "timed out waiting for fragment launches"
        );
    }

    #[test]
    fn batch_start_gate_does_not_start_after_abort() {
        let gate = super::BatchStartGate::default();

        gate.abort(novarocks::runtime::fragment::FragmentCancelReason::new(
            "test cancellation",
        ));
        let _ = gate.start(|| {});

        assert_eq!(
            gate.wait(),
            super::BatchStartState::Aborted,
            "start must not overwrite a cancellation that already aborted the gate"
        );
    }

    #[test]
    fn cancel_after_handoff_aborts_the_pending_gate_before_any_driver_starts() {
        let _failure_lock = FAILURE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let query = UniqueId {
            hi: 85_031,
            lo: 85_032,
        };
        let finst = UniqueId {
            hi: 85_033,
            lo: 85_034,
        };
        let bytes = thrift_binary_serialize(&valid_batch(query, &[finst], false))
            .expect("serialize cancellation batch");
        let service = Arc::new(CompatFragmentService::new(
            novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime::new(),
            crate::fragment::brpc_exchange_transmitter(),
            crate::fragment::brpc_fragment_lookup_client(),
            crate::fragment::compat_result_writer(),
            crate::fragment::compat_fragment_event_sink(),
        ));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        *TEST_AFTER_HANDOFF_PAUSE
            .lock()
            .expect("adapter handoff pause lock") = Some(AdapterPausePlan {
            query_id: runtime_query_id(query),
            entered: entered_tx,
            release: release_rx,
        });
        let launch_service = Arc::clone(&service);
        let launch =
            std::thread::spawn(move || launch_service.submit_exec_batch_plan_fragments(&bytes));

        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handoff reached pending start gate");
        service.cancel_fragment(finst);
        release_tx.send(()).expect("release pending start gate");
        let result = launch.join().expect("launch thread");

        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("cancelled before start")),
            "cancel after handoff must reject gate release: {result:?}"
        );
        assert_eq!(
            fragment_launch_count_for_query(runtime_query_id(query)),
            0,
            "cancelled pending handoff must not launch a driver"
        );
    }

    #[test]
    fn cancel_fragment_reaches_a_running_sync_fragment() {
        let query = UniqueId {
            hi: 85_041,
            lo: 85_042,
        };
        let finst = UniqueId {
            hi: 85_043,
            lo: 85_044,
        };
        let service = Arc::new(CompatFragmentService::new(
            novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime::new(),
            crate::fragment::brpc_exchange_transmitter(),
            crate::fragment::brpc_fragment_lookup_client(),
            crate::fragment::compat_result_writer(),
            crate::fragment::compat_fragment_event_sink(),
        ));
        let execution_service = Arc::clone(&service);
        let payload = thrift_binary_serialize(&blocking_exchange_request(query, finst))
            .expect("serialize sync fragment request");
        let execution = std::thread::spawn(move || execution_service.execute_encoded(&payload));

        let query_id = runtime_query_id(query);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !service.controls.has_running_route(query_id, finst)
            && !execution.is_finished()
            && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        if execution.is_finished() {
            panic!(
                "sync fragment exited before publishing a running route: {:?}",
                execution.join().expect("sync fragment execution thread")
            );
        }
        assert!(
            service.controls.has_running_route(query_id, finst),
            "sync fragment must publish a cancellable running route"
        );
        service.cancel_fragment(finst);

        let result = execution.join().expect("sync fragment execution thread");
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("fragment cancelled")),
            "cancel must reach the running sync fragment: {result:?}"
        );
        assert!(
            !service.controls.has_running_route(query_id, finst),
            "sync cancellation route must be removed after execution finishes"
        );
    }

    #[test]
    fn malformed_fragment_fails_before_registration() {
        let query = UniqueId {
            hi: 85_001,
            lo: 85_002,
        };
        let finst = UniqueId {
            hi: 85_003,
            lo: 85_004,
        };
        let request = request(query, finst, fragment(None));
        let before = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);
        let service = fragment_service();

        let result = service.submit_exec_plan_fragment(
            &thrift_binary_serialize(&request).expect("serialize malformed request"),
        );
        let after = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);

        assert!(
            result.is_err(),
            "malformed fragment must fail synchronously"
        );
        assert_eq!(after, before, "decode failure must not launch");
    }

    #[test]
    fn batch_second_unique_malformed_launches_nothing() {
        let query = UniqueId {
            hi: 85_101,
            lo: 85_102,
        };
        let first_finst = UniqueId {
            hi: 85_103,
            lo: 85_104,
        };
        let second_finst = UniqueId {
            hi: 85_105,
            lo: 85_106,
        };
        let valid = request(
            query,
            first_finst,
            fragment(Some(plan_nodes::TPlan::new(vec![empty_set_node()]))),
        );
        let malformed = request(query, second_finst, fragment(None));
        let batch = internal_service::TExecBatchPlanFragmentsParams::new(
            None,
            Some(vec![valid, malformed]),
        );
        let before = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);
        let service = fragment_service();

        let result = service.submit_exec_batch_plan_fragments(
            &thrift_binary_serialize(&batch).expect("serialize malformed batch"),
        );
        let after = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);

        assert!(
            result.is_err(),
            "batch decode must reject the malformed unique"
        );
        assert_eq!(
            after, before,
            "batch must launch no fragment on decode failure"
        );
    }

    #[test]
    fn batch_second_fragment_cache_options_conflict_leaves_handoff_unpublished() {
        let query = UniqueId {
            hi: 85_111,
            lo: 85_112,
        };
        let first_finst = UniqueId {
            hi: 85_113,
            lo: 85_114,
        };
        let second_finst = UniqueId {
            hi: 85_115,
            lo: 85_116,
        };
        let plan = || fragment(Some(plan_nodes::TPlan::new(vec![empty_set_node()])));
        let mut first = request(query, first_finst, plan());
        first.query_options = Some(query_options_with_cache_probability(10));
        let mut second = request(query, second_finst, plan());
        second.query_options = Some(query_options_with_cache_probability(20));
        let batch =
            internal_service::TExecBatchPlanFragmentsParams::new(None, Some(vec![first, second]));
        let before = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);
        let service = fragment_service();

        let result = service.submit_exec_batch_plan_fragments(
            &thrift_binary_serialize(&batch).expect("serialize cache-conflict batch"),
        );
        let after = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);

        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("cache options mismatch")),
            "unexpected result: {result:?}"
        );
        assert_eq!(
            after, before,
            "handoff validation failure must not launch a driver"
        );
    }

    #[test]
    fn nth_prepare_failure_compensates_batch_and_allows_retry() {
        let _failure_lock = FAILURE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let query = UniqueId {
            hi: 85_201,
            lo: 85_202,
        };
        let finst_ids = [
            UniqueId {
                hi: 85_203,
                lo: 85_204,
            },
            UniqueId {
                hi: 85_205,
                lo: 85_206,
            },
        ];
        let bytes = thrift_binary_serialize(&valid_batch(query, &finst_ids, false))
            .expect("serialize prepare failure batch");
        let reset = inject_failure(query, AdapterFailureStage::Prepare, 2);
        let service = fragment_service();

        let result = service.submit_exec_batch_plan_fragments(&bytes);

        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("prepare failure at index 2")),
            "unexpected result: {result:?}"
        );
        assert_eq!(
            fragment_launch_count_for_query(runtime_query_id(query)),
            0,
            "prepare failure must not launch any driver"
        );
        drop(reset);

        assert_eq!(
            service.submit_exec_batch_plan_fragments(&bytes),
            Ok(2),
            "compensated prepare failure must leave the batch retryable"
        );
        wait_for_query_launches(query, 2);
    }

    #[test]
    fn nth_report_registration_failure_unregisters_reports_and_rolls_back_handoff() {
        let _failure_lock = FAILURE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let query = UniqueId {
            hi: 85_211,
            lo: 85_212,
        };
        let finst_ids = [
            UniqueId {
                hi: 85_213,
                lo: 85_214,
            },
            UniqueId {
                hi: 85_215,
                lo: 85_216,
            },
        ];
        let bytes = thrift_binary_serialize(&valid_batch(query, &finst_ids, true))
            .expect("serialize report registration failure batch");
        let reset = inject_failure(query, AdapterFailureStage::ReportRegistration, 2);
        let service = fragment_service();

        let result = service.submit_exec_batch_plan_fragments(&bytes);

        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("report registration failure at index 2")),
            "unexpected result: {result:?}"
        );
        assert_eq!(
            fragment_launch_count_for_query(runtime_query_id(query)),
            0,
            "report registration failure must not launch any driver"
        );
        assert!(
            registered_reports_for_query(runtime_query_id(query)).is_empty(),
            "partial report registration must be reversed"
        );

        drop(reset);

        assert_eq!(
            service.submit_exec_batch_plan_fragments(&bytes),
            Ok(2),
            "report-registration rollback must leave the batch retryable"
        );
        wait_for_query_launches(query, 2);
    }

    #[test]
    fn async_error_cleanup_preserves_report_and_release_order() {
        let events = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let record = |event: &'static str| {
            let events = std::rc::Rc::clone(&events);
            move || events.borrow_mut().push(event)
        };
        let cancel_events = std::rc::Rc::clone(&events);
        let report_events = std::rc::Rc::clone(&events);
        let cleanup_events = std::rc::Rc::clone(&events);

        run_async_cleanup_sequence(
            Some("execution failed".to_string()),
            move |error| {
                assert_eq!(error, "execution failed");
                cancel_events.borrow_mut().push("cancel-fanout");
            },
            || {
                record("finish-for-report")();
                7
            },
            move |error, decision| {
                assert_eq!(error.as_deref(), Some("execution failed"));
                assert_eq!(*decision, 7);
                report_events.borrow_mut().push("report-done");
            },
            record("exchange-remove"),
            record("finst-unregister"),
            move |decision| {
                assert_eq!(decision, 7);
                cleanup_events.borrow_mut().push("query-cleanup");
            },
        );

        assert_eq!(
            events.borrow().as_slice(),
            [
                "cancel-fanout",
                "finish-for-report",
                "report-done",
                "exchange-remove",
                "finst-unregister",
                "query-cleanup",
            ]
        );
    }
}
