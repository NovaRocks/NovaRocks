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
//! ReportExecStatus helpers.
//!
//! StarRocks FE does not actively trigger runtime profile pulls in production; profiles are
//! pushed by BE via reportExecStatus. novarocks therefore does not expose a trigger entry point
//! and keeps this module focused on BE-initiated reports.

use std::collections::HashMap;
#[cfg(feature = "compat")]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};

#[cfg(feature = "compat")]
use crate::cache::DataCacheManager;
#[cfg(feature = "compat")]
use crate::common::network;
use crate::common::types::UniqueId;
#[cfg(feature = "compat")]
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_logging::{debug, warn};
use crate::proto::{common, novarocks};
use crate::runtime::endpoint::RuntimeEndpoint;
#[cfg(feature = "compat")]
use crate::runtime::load_tracking;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{
    CounterAggregateType, CounterMergeType, CounterMinMaxType, CounterStrategy, ProfileCounter,
    ProfileNode, ProfileUnit, Profiler, RuntimeProfileTree, default_counter_strategy,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use crate::runtime::sink_commit;
#[cfg(feature = "compat")]
use crate::service::exec_state_reporter::{self, ExecStateReportTask};
#[cfg(feature = "compat")]
use crate::service::exec_status_report::{self, ExecStatusReportInput};
#[cfg(feature = "compat")]
use crate::service::frontend_rpc::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager};
use crate::service::report_worker;
use crate::service::standalone_exec_state_reporter::{self, StandaloneExecStateReportTask};
#[cfg(feature = "compat")]
use crate::thrift::data_cache;
#[cfg(feature = "compat")]
use crate::thrift::{frontend_service, types};
#[cfg(feature = "compat")]
use crate::thrift::{metrics, runtime_profile, status, status_code};

#[derive(Clone, Debug)]
enum ReportDestination {
    #[cfg(feature = "compat")]
    StarRocksFrontend(types::TNetworkAddress),
    NovaRocksCoordinator(RuntimeEndpoint),
}

#[derive(Clone, Debug)]
struct ReportInstance {
    destination: ReportDestination,
    backend_num: i32,
    query_id: QueryId,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
    fe_query_gone: bool,
}

static REPORT_REGISTRY: OnceLock<Mutex<HashMap<UniqueId, ReportInstance>>> = OnceLock::new();
#[cfg(test)]
static TEST_PROGRESS_REPORT_CALLS: OnceLock<Mutex<HashMap<UniqueId, usize>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<UniqueId, ReportInstance>> {
    REPORT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn progress_report_call_count_for_test(finst_id: UniqueId) -> usize {
    TEST_PROGRESS_REPORT_CALLS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("test progress report calls lock")
        .get(&finst_id)
        .copied()
        .unwrap_or(0)
}

#[cfg(feature = "compat")]
pub fn register_instance(
    finst_id: UniqueId,
    query_id: QueryId,
    coord: types::TNetworkAddress,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
) {
    register_instance_with_destination(
        finst_id,
        query_id,
        ReportDestination::StarRocksFrontend(coord),
        backend_num,
        enable_profile,
        profiler,
        mem_tracker,
        query_mem_tracker,
        report_interval_ns,
    );
}

pub fn unregister_instance(finst_id: UniqueId) {
    registry()
        .lock()
        .expect("report registry lock")
        .remove(&finst_id);
}

pub fn register_novarocks_instance(
    finst_id: UniqueId,
    query_id: QueryId,
    coord: RuntimeEndpoint,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
) {
    register_instance_with_destination(
        finst_id,
        query_id,
        ReportDestination::NovaRocksCoordinator(coord),
        backend_num,
        enable_profile,
        profiler,
        mem_tracker,
        query_mem_tracker,
        report_interval_ns,
    );
}

fn register_instance_with_destination(
    finst_id: UniqueId,
    query_id: QueryId,
    destination: ReportDestination,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
) {
    report_worker::ensure_started();
    match &destination {
        #[cfg(feature = "compat")]
        ReportDestination::StarRocksFrontend(_) => exec_state_reporter::ensure_started(),
        ReportDestination::NovaRocksCoordinator(_) => {
            standalone_exec_state_reporter::ensure_started()
        }
    }
    let mut guard = registry().lock().expect("report registry lock");
    guard.insert(
        finst_id,
        ReportInstance {
            destination,
            backend_num,
            query_id,
            enable_profile,
            profiler,
            mem_tracker,
            query_mem_tracker,
            report_interval_ns,
            fe_query_gone: false,
        },
    );
}

pub(crate) struct ReportInstanceSnapshot {
    pub(crate) enable_profile: bool,
    pub(crate) report_interval_ns: Option<i64>,
}

pub(crate) fn list_report_instances() -> Vec<(UniqueId, ReportInstanceSnapshot)> {
    let guard = registry().lock().expect("report registry lock");
    guard
        .iter()
        .filter(|(_, instance)| !instance.fe_query_gone)
        .map(|(id, instance)| {
            (
                *id,
                ReportInstanceSnapshot {
                    enable_profile: instance.enable_profile,
                    report_interval_ns: instance.report_interval_ns,
                },
            )
        })
        .collect()
}

pub(crate) fn mark_fe_query_gone(finst_id: UniqueId) {
    #[cfg(not(feature = "compat"))]
    {
        let _ = finst_id;
        return;
    }
    #[cfg(feature = "compat")]
    if let Ok(mut guard) = registry().lock()
        && let Some(instance) = guard.get_mut(&finst_id)
        && matches!(
            instance.destination,
            ReportDestination::StarRocksFrontend(_)
        )
    {
        instance.fe_query_gone = true;
    }
}

pub fn report_fragment_done(
    finst_id: UniqueId,
    error: Option<String>,
    include_runtime_filters: bool,
) {
    let instance = {
        let mut guard = registry().lock().expect("report registry lock");
        guard.remove(&finst_id)
    };
    let Some(instance) = instance else {
        debug!(
            target: "novarocks::report",
            finst_id = %finst_id,
            "report instance missing"
        );
        sink_commit::unregister(finst_id);
        return;
    };
    if instance.fe_query_gone {
        debug!(
            target: "novarocks::report",
            finst_id = %finst_id,
            query_id = %instance.query_id,
            "skip final reportExecStatus because FE query is already gone"
        );
        sink_commit::unregister(finst_id);
        return;
    }
    match instance.destination {
        #[cfg(feature = "compat")]
        ReportDestination::StarRocksFrontend(coord) => {
            let status = thrift_status_from_error(error.clone());
            let profile = build_profile_tree(
                instance.query_id,
                instance.enable_profile,
                instance.profiler.as_ref(),
                instance.mem_tracker.as_ref(),
                instance.query_mem_tracker.as_ref(),
                include_runtime_filters,
            );
            let load_datacache_metrics = build_load_datacache_metrics(profile.as_ref());
            let params = exec_status_report::build_report_params(ExecStatusReportInput {
                finst_id,
                query_id: instance.query_id,
                backend_num: instance.backend_num,
                status,
                profile,
                done: true,
                tracking_url: build_tracking_url(instance.query_id),
                load_channel_profile: None,
                load_datacache_metrics,
                native_profile: None,
            });
            enqueue_final_report(ExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
            });
        }
        ReportDestination::NovaRocksCoordinator(coord) => {
            let native_profile = build_native_profile_tree(
                instance.query_id,
                instance.enable_profile,
                instance.profiler.as_ref(),
                instance.mem_tracker.as_ref(),
                instance.query_mem_tracker.as_ref(),
                include_runtime_filters,
            );
            let report = build_native_report(NativeExecStatusReportInput {
                finst_id,
                query_id: instance.query_id,
                backend_num: instance.backend_num,
                status: native_status_from_error(error.clone()),
                done: true,
                native_profile,
            });
            enqueue_standalone_final_report(StandaloneExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                report,
            });
        }
    }
    sink_commit::unregister(finst_id);
}

/// Enqueues a best-effort progress report for a fragment instance.
///
/// Runtime I/O adapters decide whether to emit progress; this retains the
/// protocol-specific registry and queue lifecycle behind that adapter.
pub fn report_exec_state(finst_id: UniqueId) {
    #[cfg(test)]
    {
        let mut calls = TEST_PROGRESS_REPORT_CALLS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("test progress report calls lock");
        *calls.entry(finst_id).or_default() += 1;
    }
    let instance = {
        let guard = registry().lock().expect("report registry lock");
        guard.get(&finst_id).cloned()
    };
    let Some(instance) = instance else {
        debug!(
            target: "novarocks::report",
            finst_id = %finst_id,
            "report instance missing"
        );
        return;
    };
    if instance.fe_query_gone {
        debug!(
            target: "novarocks::report",
            finst_id = %finst_id,
            query_id = %instance.query_id,
            "skip periodic reportExecStatus because FE query is already gone"
        );
        return;
    }
    let enqueue_result = match instance.destination {
        #[cfg(feature = "compat")]
        ReportDestination::StarRocksFrontend(coord) => {
            let status = thrift_ok_status();
            let profile = build_profile_tree(
                instance.query_id,
                instance.enable_profile,
                instance.profiler.as_ref(),
                instance.mem_tracker.as_ref(),
                instance.query_mem_tracker.as_ref(),
                false,
            );
            let load_datacache_metrics = build_load_datacache_metrics(profile.as_ref());
            let params = exec_status_report::build_report_params(ExecStatusReportInput {
                finst_id,
                query_id: instance.query_id,
                backend_num: instance.backend_num,
                status,
                profile,
                done: false,
                tracking_url: build_tracking_url(instance.query_id),
                load_channel_profile: None,
                load_datacache_metrics,
                native_profile: None,
            });
            enqueue_non_final_report(ExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
            })
        }
        ReportDestination::NovaRocksCoordinator(coord) => {
            let native_profile = build_native_profile_tree(
                instance.query_id,
                instance.enable_profile,
                instance.profiler.as_ref(),
                instance.mem_tracker.as_ref(),
                instance.query_mem_tracker.as_ref(),
                false,
            );
            let report = build_native_report(NativeExecStatusReportInput {
                finst_id,
                query_id: instance.query_id,
                backend_num: instance.backend_num,
                status: native_ok_status(),
                done: false,
                native_profile,
            });
            enqueue_standalone_non_final_report(StandaloneExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                report,
            })
        }
    };
    if let Err(e) = enqueue_result {
        warn!(
            target: "novarocks::report",
            finst_id = %finst_id,
            error = %e,
            "failed to enqueue reportExecStatus"
        );
    }
}

#[cfg(feature = "compat")]
pub fn fetch_query_profile(
    coord: &types::TNetworkAddress,
    query_id: &str,
) -> Result<String, String> {
    let req = frontend_service::TGetProfileRequest::new(Some(vec![query_id.to_string()]));
    let result = FrontendRpcManager::shared()
        .call(FrontendRpcKind::SchemaQuery, coord, |client| {
            client
                .get_query_profile(req.clone())
                .map_err(FrontendRpcError::from_thrift)
        })
        .map_err(|e| format!("getQueryProfile RPC failed: {e}"))?;

    if let Some(status) = result.status
        && status.status_code != status_code::TStatusCode::OK
    {
        return Err(format!("FE returned error: {:?}", status));
    }
    let payload = result
        .query_result
        .and_then(|mut v| v.drain(..).next())
        .unwrap_or_default();
    Ok(payload)
}

#[cfg(feature = "compat")]
fn enqueue_final_report(task: ExecStateReportTask) {
    exec_state_reporter::enqueue_final(task);
}

#[cfg(feature = "compat")]
fn enqueue_non_final_report(task: ExecStateReportTask) -> Result<(), String> {
    exec_state_reporter::enqueue_non_final(task)
}

fn enqueue_standalone_final_report(task: StandaloneExecStateReportTask) {
    standalone_exec_state_reporter::enqueue_final(task);
}

fn enqueue_standalone_non_final_report(task: StandaloneExecStateReportTask) -> Result<(), String> {
    standalone_exec_state_reporter::enqueue_non_final(task)
}

#[cfg(feature = "compat")]
fn build_tracking_url(query_id: QueryId) -> Option<String> {
    if !load_tracking::has_tracking_log(query_id) {
        return None;
    }
    let cfg = novarocks_app_config().ok()?;
    let host = network::advertise_host().ok()?;
    let host = network::format_host_for_url(&host);
    Some(format!(
        "http://{host}:{}/api/_load_tracking/{}/{}",
        cfg.server.http_port, query_id.hi, query_id.lo
    ))
}

#[cfg(feature = "compat")]
fn sum_counter_from_profile_tree(
    profile: &runtime_profile::TRuntimeProfileTree,
    name: &str,
) -> i64 {
    profile
        .nodes
        .iter()
        .flat_map(|node| node.counters.iter())
        .filter(|counter| counter.name == name)
        .map(|counter| counter.value)
        .fold(0i64, |acc, value| acc.saturating_add(value))
}

#[cfg(feature = "compat")]
fn count_datacache_active_nodes(profile: &runtime_profile::TRuntimeProfileTree) -> i64 {
    profile
        .nodes
        .iter()
        .filter(|node| {
            let read_bytes = node
                .counters
                .iter()
                .filter(|counter| counter.name == "DataCacheReadBytes")
                .map(|counter| counter.value)
                .fold(0i64, |acc, value| acc.saturating_add(value));
            let write_bytes = node
                .counters
                .iter()
                .filter(|counter| counter.name == "DataCacheWriteBytes")
                .map(|counter| counter.value)
                .fold(0i64, |acc, value| acc.saturating_add(value));
            read_bytes > 0 || write_bytes > 0
        })
        .count() as i64
}

#[cfg(feature = "compat")]
fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(feature = "compat")]
fn build_datacache_runtime_metrics() -> Option<data_cache::TDataCacheMetrics> {
    let cache = DataCacheManager::instance().block_cache()?;
    Some(data_cache::TDataCacheMetrics::new(
        Some(data_cache::TDataCacheStatus::NORMAL),
        Option::<i64>::None,
        Option::<i64>::None,
        Some(clamp_u64_to_i64(cache.capacity_bytes())),
        Some(clamp_u64_to_i64(cache.used_bytes())),
    ))
}

#[cfg(feature = "compat")]
fn build_load_datacache_metrics(
    profile: Option<&runtime_profile::TRuntimeProfileTree>,
) -> Option<data_cache::TLoadDataCacheMetrics> {
    let profile = profile?;
    let read_bytes = sum_counter_from_profile_tree(profile, "DataCacheReadBytes");
    let read_time_ns = sum_counter_from_profile_tree(profile, "DataCacheReadTimer");
    let write_bytes = sum_counter_from_profile_tree(profile, "DataCacheWriteBytes");
    let write_time_ns = sum_counter_from_profile_tree(profile, "DataCacheWriteTimer");
    let mut count = count_datacache_active_nodes(profile);

    let has_activity = read_bytes > 0 || read_time_ns > 0 || write_bytes > 0 || write_time_ns > 0;
    if !has_activity {
        return None;
    }
    if count <= 0 {
        count = 1;
    }

    Some(data_cache::TLoadDataCacheMetrics::new(
        Some(read_bytes),
        Some(read_time_ns),
        Some(write_bytes),
        Some(write_time_ns),
        Some(count),
        build_datacache_runtime_metrics(),
    ))
}

struct NativeExecStatusReportInput {
    finst_id: UniqueId,
    query_id: QueryId,
    backend_num: i32,
    status: common::Status,
    done: bool,
    native_profile: Option<novarocks::RuntimeProfileTree>,
}

fn build_native_report(input: NativeExecStatusReportInput) -> novarocks::ExecStatusReport {
    let iceberg_commits = sink_commit::list_iceberg_commits(input.finst_id);
    let (loaded_rows, sink_load_bytes, filtered_rows) =
        load_stats_for_native_report(input.finst_id, &iceberg_commits);

    novarocks::ExecStatusReport {
        query_id: Some(crate::proto::common::UniqueId {
            hi: input.query_id.hi,
            lo: input.query_id.lo,
        }),
        fragment_instance_id: Some(crate::proto::common::UniqueId {
            hi: input.finst_id.hi,
            lo: input.finst_id.lo,
        }),
        backend_num: input.backend_num,
        status: Some(input.status),
        done: input.done,
        iceberg_commits,
        loaded_rows,
        sink_load_bytes,
        filtered_rows,
        profile: input.native_profile,
    }
}

fn load_stats_for_native_report(
    finst_id: UniqueId,
    iceberg_commits: &[novarocks::IcebergCommitInfo],
) -> (i64, i64, i64) {
    let state_stats = sink_commit::get_load_stats(finst_id);
    let mut normal_rows: i64 = state_stats.loaded_rows.max(0);
    let mut loaded_bytes: i64 = state_stats.loaded_bytes.max(0);
    let filtered_rows: i64 = state_stats.filtered_rows.max(0);

    for info in iceberg_commits {
        if let Some(file) = info.iceberg_data_file.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
    }

    (normal_rows, loaded_bytes, filtered_rows)
}

fn native_ok_status() -> common::Status {
    common::Status {
        code: 0,
        message: String::new(),
    }
}

fn native_status_from_error(error: Option<String>) -> common::Status {
    match error {
        Some(message) => common::Status { code: 1, message },
        None => native_ok_status(),
    }
}

#[cfg(feature = "compat")]
fn thrift_ok_status() -> status::TStatus {
    status::TStatus::new(status_code::TStatusCode::OK, None)
}

#[cfg(feature = "compat")]
fn thrift_status_from_error(error: Option<String>) -> status::TStatus {
    match error {
        Some(msg) => {
            status::TStatus::new(status_code::TStatusCode::INTERNAL_ERROR, Some(vec![msg]))
        }
        None => thrift_ok_status(),
    }
}

#[cfg(feature = "compat")]
pub(crate) fn profile_to_thrift_tree_for_fe(
    profiler: &Profiler,
) -> runtime_profile::TRuntimeProfileTree {
    runtime_profile_tree_to_thrift_for_fe(&profiler.to_native_tree())
        .expect("RuntimeProfile should always produce thrift-compatible profile units")
}

#[cfg(feature = "compat")]
pub(crate) fn runtime_profile_tree_to_thrift_for_fe(
    tree: &RuntimeProfileTree,
) -> Result<runtime_profile::TRuntimeProfileTree, String> {
    let mut nodes = Vec::new();
    native_profile_node_to_thrift_for_fe(&tree.root, &mut nodes)?;
    Ok(runtime_profile::TRuntimeProfileTree::new(nodes))
}

#[cfg(feature = "compat")]
pub(crate) fn runtime_profile_tree_from_thrift_for_fe(
    tree: &runtime_profile::TRuntimeProfileTree,
) -> Result<RuntimeProfileTree, String> {
    let Some((root, consumed)) = native_profile_node_from_thrift_for_fe(&tree.nodes, 0)? else {
        return Err("TRuntimeProfileTree missing root".to_string());
    };
    if consumed != tree.nodes.len() {
        return Err(format!(
            "TRuntimeProfileTree has trailing nodes: consumed {consumed} of {}",
            tree.nodes.len()
        ));
    }
    Ok(RuntimeProfileTree { root })
}

#[cfg(feature = "compat")]
fn native_profile_node_to_thrift_for_fe(
    node: &ProfileNode,
    out: &mut Vec<runtime_profile::TRuntimeProfileNode>,
) -> Result<(), String> {
    let mut counters = node
        .counters
        .iter()
        .map(native_counter_to_thrift_for_fe)
        .collect::<Result<Vec<_>, _>>()?;
    counters.sort_by(|left, right| left.name.cmp(&right.name));

    let mut child_counters_map = BTreeMap::<String, BTreeSet<String>>::new();
    for counter in &node.counters {
        child_counters_map
            .entry(counter.parent_name.clone())
            .or_default()
            .insert(counter.name.clone());
    }

    let info_strings = node.info_strings.clone();
    let info_strings_display_order = info_strings.keys().cloned().collect::<Vec<_>>();

    out.push(runtime_profile::TRuntimeProfileNode::new(
        node.name.clone(),
        node.children.len() as i32,
        counters,
        i64::from(node.node_id),
        false,
        info_strings,
        info_strings_display_order,
        child_counters_map,
        None,
    ));

    for child in &node.children {
        native_profile_node_to_thrift_for_fe(child, out)?;
    }
    Ok(())
}

#[cfg(feature = "compat")]
fn native_counter_to_thrift_for_fe(
    counter: &ProfileCounter,
) -> Result<runtime_profile::TCounter, String> {
    Ok(runtime_profile::TCounter::new(
        counter.name.clone(),
        profile_unit_to_thrift_for_fe(counter.unit),
        counter.value,
        Some(counter_strategy_to_thrift_for_fe(counter.strategy)),
        counter.min_value,
        counter.max_value,
    ))
}

#[cfg(feature = "compat")]
fn native_profile_node_from_thrift_for_fe(
    nodes: &[runtime_profile::TRuntimeProfileNode],
    idx: usize,
) -> Result<Option<(ProfileNode, usize)>, String> {
    let Some(node) = nodes.get(idx) else {
        return Ok(None);
    };
    if node.num_children < 0 {
        return Err(format!(
            "TRuntimeProfileTree node {} has negative num_children {}",
            node.name, node.num_children
        ));
    }

    let mut next = idx + 1;
    let mut children = Vec::new();
    for _ in 0..node.num_children {
        let Some((child, consumed)) = native_profile_node_from_thrift_for_fe(nodes, next)? else {
            return Err(format!(
                "TRuntimeProfileTree node {} declares more children than available nodes",
                node.name
            ));
        };
        children.push(child);
        next = consumed;
    }

    let counter_parents = node
        .child_counters_map
        .iter()
        .flat_map(|(parent, children)| {
            children
                .iter()
                .map(|child| (child.clone(), parent.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<BTreeMap<_, _>>();

    let counters = node
        .counters
        .iter()
        .map(|counter| {
            thrift_counter_to_native_for_fe(
                counter,
                counter_parents
                    .get(&counter.name)
                    .cloned()
                    .unwrap_or_default(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some((
        ProfileNode {
            name: node.name.clone(),
            node_id: metadata_to_native_node_id_for_fe(node.metadata),
            counters,
            info_strings: node.info_strings.clone(),
            children,
        },
        next,
    )))
}

#[cfg(feature = "compat")]
fn thrift_counter_to_native_for_fe(
    counter: &runtime_profile::TCounter,
    parent_name: String,
) -> Result<ProfileCounter, String> {
    let unit = profile_unit_from_thrift_for_fe(counter.type_)?;
    let strategy = counter
        .strategy
        .as_ref()
        .map(counter_strategy_from_thrift_for_fe)
        .transpose()?
        .unwrap_or_else(|| default_counter_strategy(unit));
    Ok(ProfileCounter {
        name: counter.name.clone(),
        parent_name,
        unit,
        strategy,
        value: counter.value,
        min_value: counter.min_value,
        max_value: counter.max_value,
    })
}

#[cfg(feature = "compat")]
fn metadata_to_native_node_id_for_fe(metadata: i64) -> i32 {
    match i32::try_from(metadata) {
        Ok(value) => value,
        Err(_) if metadata.is_negative() => i32::MIN,
        Err(_) => i32::MAX,
    }
}

#[cfg(feature = "compat")]
fn profile_unit_to_thrift_for_fe(unit: ProfileUnit) -> metrics::TUnit {
    match unit {
        ProfileUnit::Unit => metrics::TUnit::UNIT,
        ProfileUnit::CpuTicks => metrics::TUnit::CPU_TICKS,
        ProfileUnit::Bytes => metrics::TUnit::BYTES,
        ProfileUnit::TimeNs => metrics::TUnit::TIME_NS,
        ProfileUnit::TimeMs => metrics::TUnit::TIME_MS,
        ProfileUnit::TimeS => metrics::TUnit::TIME_S,
        ProfileUnit::None => metrics::TUnit::NONE,
    }
}

#[cfg(feature = "compat")]
fn profile_unit_from_thrift_for_fe(unit: metrics::TUnit) -> Result<ProfileUnit, String> {
    match unit {
        metrics::TUnit::UNIT => Ok(ProfileUnit::Unit),
        metrics::TUnit::CPU_TICKS => Ok(ProfileUnit::CpuTicks),
        metrics::TUnit::BYTES => Ok(ProfileUnit::Bytes),
        metrics::TUnit::TIME_NS => Ok(ProfileUnit::TimeNs),
        metrics::TUnit::TIME_MS => Ok(ProfileUnit::TimeMs),
        metrics::TUnit::TIME_S => Ok(ProfileUnit::TimeS),
        metrics::TUnit::NONE => Ok(ProfileUnit::None),
        other => Err(format!(
            "unsupported thrift profile unit for FE report: {other:?}"
        )),
    }
}

#[cfg(feature = "compat")]
fn counter_strategy_to_thrift_for_fe(
    strategy: CounterStrategy,
) -> runtime_profile::TCounterStrategy {
    let aggregate_type = match strategy.aggregate_type() {
        CounterAggregateType::Sum => runtime_profile::TCounterAggregateType::SUM,
        CounterAggregateType::Avg => runtime_profile::TCounterAggregateType::AVG,
        CounterAggregateType::SumAvg => runtime_profile::TCounterAggregateType::SUM_AVG,
        CounterAggregateType::AvgSum => runtime_profile::TCounterAggregateType::AVG_SUM,
    };
    let merge_type = match strategy.merge_type() {
        CounterMergeType::MergeAll => runtime_profile::TCounterMergeType::MERGE_ALL,
        CounterMergeType::SkipAll => runtime_profile::TCounterMergeType::SKIP_ALL,
        CounterMergeType::SkipFirstMerge => runtime_profile::TCounterMergeType::SKIP_FIRST_MERGE,
        CounterMergeType::SkipSecondMerge => runtime_profile::TCounterMergeType::SKIP_SECOND_MERGE,
    };
    let min_max_type = strategy
        .min_max_type()
        .map(|min_max_type| match min_max_type {
            CounterMinMaxType::MinMaxAll => runtime_profile::TCounterMinMaxType::MIN_MAX_ALL,
            CounterMinMaxType::SkipAll => runtime_profile::TCounterMinMaxType::SKIP_ALL,
        });
    runtime_profile::TCounterStrategy::new(
        aggregate_type,
        merge_type,
        strategy.display_threshold(),
        min_max_type,
    )
}

#[cfg(feature = "compat")]
fn counter_strategy_from_thrift_for_fe(
    strategy: &runtime_profile::TCounterStrategy,
) -> Result<CounterStrategy, String> {
    let aggregate_type = match strategy.aggregate_type {
        runtime_profile::TCounterAggregateType::SUM => CounterAggregateType::Sum,
        runtime_profile::TCounterAggregateType::AVG => CounterAggregateType::Avg,
        runtime_profile::TCounterAggregateType::SUM_AVG => CounterAggregateType::SumAvg,
        runtime_profile::TCounterAggregateType::AVG_SUM => CounterAggregateType::AvgSum,
        other => {
            return Err(format!(
                "unsupported thrift counter aggregate type: {other:?}"
            ));
        }
    };
    let merge_type = match strategy.merge_type {
        runtime_profile::TCounterMergeType::MERGE_ALL => CounterMergeType::MergeAll,
        runtime_profile::TCounterMergeType::SKIP_ALL => CounterMergeType::SkipAll,
        runtime_profile::TCounterMergeType::SKIP_FIRST_MERGE => CounterMergeType::SkipFirstMerge,
        runtime_profile::TCounterMergeType::SKIP_SECOND_MERGE => CounterMergeType::SkipSecondMerge,
        other => return Err(format!("unsupported thrift counter merge type: {other:?}")),
    };
    let min_max_type = strategy
        .min_max_type
        .map(|min_max_type| match min_max_type {
            runtime_profile::TCounterMinMaxType::MIN_MAX_ALL => Ok(CounterMinMaxType::MinMaxAll),
            runtime_profile::TCounterMinMaxType::SKIP_ALL => Ok(CounterMinMaxType::SkipAll),
            other => Err(format!(
                "unsupported thrift counter min/max type: {other:?}"
            )),
        });
    let min_max_type = min_max_type.transpose()?;
    Ok(CounterStrategy::custom(
        aggregate_type,
        merge_type,
        strategy.display_threshold,
        min_max_type,
    ))
}

#[cfg(feature = "compat")]
fn build_profile_tree(
    query_id: QueryId,
    enable_profile: bool,
    profiler: Option<&Profiler>,
    mem_tracker: Option<&Arc<MemTracker>>,
    query_mem_tracker: Option<&Arc<MemTracker>>,
    include_runtime_filters: bool,
) -> Option<runtime_profile::TRuntimeProfileTree> {
    let merged = build_merged_profile_for_report(
        query_id,
        enable_profile,
        profiler,
        mem_tracker,
        query_mem_tracker,
        include_runtime_filters,
    )?;
    let mut tree = profile_to_thrift_tree_for_fe(&merged);
    normalize_profile_tree_for_fe(&mut tree);
    Some(tree)
}

#[allow(dead_code)] // Task 3 wires native report requests to this helper.
pub(crate) fn build_native_profile_tree(
    query_id: QueryId,
    enable_profile: bool,
    profiler: Option<&Profiler>,
    mem_tracker: Option<&Arc<MemTracker>>,
    query_mem_tracker: Option<&Arc<MemTracker>>,
    include_runtime_filters: bool,
) -> Option<novarocks::RuntimeProfileTree> {
    build_merged_profile_for_report(
        query_id,
        enable_profile,
        profiler,
        mem_tracker,
        query_mem_tracker,
        include_runtime_filters,
    )
    .map(|merged| merged.to_proto())
}

fn build_merged_profile_for_report(
    query_id: QueryId,
    enable_profile: bool,
    profiler: Option<&Profiler>,
    mem_tracker: Option<&Arc<MemTracker>>,
    query_mem_tracker: Option<&Arc<MemTracker>>,
    include_runtime_filters: bool,
) -> Option<Profiler> {
    if !enable_profile {
        return None;
    }
    let profiler = profiler?;
    let merged = merge_pipeline_profiles_for_fe(profiler);
    if include_runtime_filters {
        RuntimeFilterLifecycleRegistry::global()
            .export_to_profile(QueryKey::from_hi_lo(query_id.hi, query_id.lo), &merged);
    }
    if let Some(tracker) = mem_tracker {
        merged.counter_set(
            "InstancePeakMemoryUsage",
            ProfileUnit::Bytes,
            tracker.peak(),
        );
        merged.counter_set(
            "InstanceAllocatedMemoryUsage",
            ProfileUnit::Bytes,
            tracker.allocated(),
        );
        merged.counter_set(
            "InstanceDeallocatedMemoryUsage",
            ProfileUnit::Bytes,
            tracker.deallocated(),
        );
    }
    if let Some(tracker) = query_mem_tracker {
        merged.counter_set("QueryPeakMemoryUsage", ProfileUnit::Bytes, tracker.peak());
    }
    Some(merged)
}

pub(crate) fn merge_pipeline_profiles_for_fe(profiler: &Profiler) -> Profiler {
    let merged = Profiler::new(profiler.name());
    merged.set_metadata(profiler.metadata());
    merged.copy_all_info_strings_from(profiler);
    merged.copy_all_counters_from(profiler);

    for child in profiler.children() {
        let name = child.name();
        if !name.starts_with("Pipeline (id=") {
            merged.add_child(child);
            continue;
        }

        let grand_children = child.children();
        let has_driver_level = grand_children
            .first()
            .map(|c| c.name().starts_with("PipelineDriver (id="))
            .unwrap_or(false);
        if !has_driver_level {
            merged.add_child(child);
            continue;
        }

        if grand_children.is_empty() {
            continue;
        }

        let merged_driver = Profiler::merge_isomorphic_profiles(&grand_children);
        merged_driver.set_name(child.name());
        merged_driver.copy_all_info_strings_from(&child);
        merged_driver.copy_all_counters_from(&child);
        merged.add_child(merged_driver);
    }

    merged
}

#[cfg(feature = "compat")]
fn normalize_profile_tree_for_fe(tree: &mut runtime_profile::TRuntimeProfileTree) {
    let mut stack: Vec<(String, i32, bool)> = Vec::new();
    for node in &mut tree.nodes {
        while let Some((_, remaining, _)) = stack.last() {
            if *remaining > 0 {
                break;
            }
            stack.pop();
        }

        if let Some((_, remaining, _)) = stack.last_mut() {
            *remaining -= 1;
        }

        let name = node.name.as_str();
        let mut skip_warn = stack.last().map(|(_, _, skip)| *skip).unwrap_or(false);
        if name.starts_with("MemTracker") {
            skip_warn = true;
        }
        if name == "RuntimeFilters" {
            skip_warn = true;
        }
        if name.starts_with("Pipeline (id=") || name.starts_with("PipelineDriver (id=") {
            if node.num_children > 0 {
                stack.push((node.name.clone(), node.num_children, skip_warn));
            }
            continue;
        }
        if !skip_warn
            && !name.contains("plan_node_id=")
            && !name.contains("(id=")
            && name != "Summary"
            && name != "CommonMetrics"
            && name != "UniqueMetrics"
        {
            let mut parts = Vec::with_capacity(stack.len() + 1);
            for (ancestor, _, _) in &stack {
                parts.push(ancestor.as_str());
            }
            parts.push(name);
            let path = parts.join(" > ");
            warn!(
                target: "novarocks::profile",
                "profile node name missing plan_node_id: path={} name={}",
                path,
                name,
            );
        }
        if name == "RESULT_SINK" {
            node.name = "RESULT_SINK (plan_node_id=-1)".to_string();
            if node.num_children > 0 {
                stack.push((node.name.clone(), node.num_children, skip_warn));
            }
            continue;
        }
        if node.name.contains("(id=") && !node.name.contains("plan_node_id=") {
            node.name = node.name.replace("(id=", "(plan_node_id=");
        }
        if node.num_children > 0 {
            stack.push((node.name.clone(), node.num_children, skip_warn));
        }
    }
}

#[cfg(feature = "compat")]
pub(crate) fn is_query_gone_status(status: &status::TStatus) -> bool {
    status.status_code == status_code::TStatusCode::NOT_FOUND
        && status
            .error_msgs
            .as_ref()
            .map(|msgs| {
                msgs.iter()
                    .any(|msg| msg.contains("query id") && msg.contains("not found"))
            })
            .unwrap_or(false)
}
