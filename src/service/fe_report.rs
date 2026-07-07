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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use crate::cache::DataCacheManager;
use crate::common::network;
use crate::common::types::UniqueId;
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_logging::{debug, warn};
use crate::proto::novarocks;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::load_tracking;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{
    CounterAggregateType, CounterMergeType, CounterMinMaxType, CounterStrategy, ProfileCounter,
    ProfileNode, ProfileUnit, Profiler, RuntimeProfileTree, default_counter_strategy,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use crate::runtime::sink_commit;
use crate::service::exec_state_reporter::{self, ExecStateReportTask};
use crate::service::exec_status_report::{self, ExecStatusReportInput};
use crate::service::frontend_rpc::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager};
use crate::service::report_worker;
use crate::service::standalone_exec_state_reporter::{self, StandaloneExecStateReportTask};
use crate::thrift::{
    data_cache, frontend_service, metrics, runtime_profile, status, status_code, types,
};

#[derive(Clone, Debug)]
enum ReportDestination {
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

fn registry() -> &'static Mutex<HashMap<UniqueId, ReportInstance>> {
    REPORT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_instance(
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

pub(crate) fn register_novarocks_instance(
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

pub(crate) fn report_fragment_done(
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
    let status = match error {
        Some(msg) => {
            status::TStatus::new(status_code::TStatusCode::INTERNAL_ERROR, Some(vec![msg]))
        }
        None => status::TStatus::new(status_code::TStatusCode::OK, None),
    };
    match instance.destination {
        ReportDestination::StarRocksFrontend(coord) => {
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
            let report = match exec_status_report::build_native_report(ExecStatusReportInput {
                finst_id,
                query_id: instance.query_id,
                backend_num: instance.backend_num,
                status: status.clone(),
                profile: None,
                done: true,
                tracking_url: None,
                load_channel_profile: None,
                load_datacache_metrics: None,
                native_profile,
            }) {
                Ok(report) => report,
                Err(err) => {
                    warn!(
                        target: "novarocks::report",
                        finst_id = %finst_id,
                        query_id = %instance.query_id,
                        error = %err,
                        "failed to build native final reportExecStatus"
                    );
                    native_error_report(
                        instance.query_id,
                        finst_id,
                        instance.backend_num,
                        true,
                        format!("failed to build native reportExecStatus: {err}"),
                    )
                }
            };
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

pub(crate) fn report_exec_state(finst_id: UniqueId) {
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
    let status = status::TStatus::new(status_code::TStatusCode::OK, None);
    let enqueue_result = match instance.destination {
        ReportDestination::StarRocksFrontend(coord) => {
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
            let report = match exec_status_report::build_native_report(ExecStatusReportInput {
                finst_id,
                query_id: instance.query_id,
                backend_num: instance.backend_num,
                status: status.clone(),
                profile: None,
                done: false,
                tracking_url: None,
                load_channel_profile: None,
                load_datacache_metrics: None,
                native_profile,
            }) {
                Ok(report) => report,
                Err(err) => {
                    warn!(
                        target: "novarocks::report",
                        finst_id = %finst_id,
                        query_id = %instance.query_id,
                        error = %err,
                        "failed to build native non-final reportExecStatus"
                    );
                    return;
                }
            };
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

pub(crate) fn fetch_query_profile(
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

fn enqueue_final_report(task: ExecStateReportTask) {
    #[cfg(test)]
    if test_capture_final_report(&task) {
        return;
    }

    exec_state_reporter::enqueue_final(task);
}

fn enqueue_non_final_report(task: ExecStateReportTask) -> Result<(), String> {
    #[cfg(test)]
    if test_capture_non_final_report(&task) {
        return Ok(());
    }

    exec_state_reporter::enqueue_non_final(task)
}

fn enqueue_standalone_final_report(task: StandaloneExecStateReportTask) {
    #[cfg(test)]
    if test_capture_standalone_final_report(&task) {
        return;
    }

    standalone_exec_state_reporter::enqueue_final(task);
}

fn enqueue_standalone_non_final_report(task: StandaloneExecStateReportTask) -> Result<(), String> {
    #[cfg(test)]
    if test_capture_standalone_non_final_report(&task) {
        return Ok(());
    }

    standalone_exec_state_reporter::enqueue_non_final(task)
}

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

fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

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

fn native_error_report(
    query_id: QueryId,
    finst_id: UniqueId,
    backend_num: i32,
    done: bool,
    message: String,
) -> novarocks::ExecStatusReport {
    novarocks::ExecStatusReport {
        query_id: Some(crate::proto::common::UniqueId {
            hi: query_id.hi,
            lo: query_id.lo,
        }),
        fragment_instance_id: Some(crate::proto::common::UniqueId {
            hi: finst_id.hi,
            lo: finst_id.lo,
        }),
        backend_num,
        status: Some(crate::proto::common::Status {
            code: status_code::TStatusCode::INTERNAL_ERROR.0,
            message,
        }),
        done,
        iceberg_commits: Vec::new(),
        loaded_rows: 0,
        sink_load_bytes: 0,
        filtered_rows: 0,
        profile: None,
    }
}

pub(crate) fn profile_to_thrift_tree_for_fe(
    profiler: &Profiler,
) -> runtime_profile::TRuntimeProfileTree {
    runtime_profile_tree_to_thrift_for_fe(&profiler.to_native_tree())
        .expect("RuntimeProfile should always produce thrift-compatible profile units")
}

pub(crate) fn runtime_profile_tree_to_thrift_for_fe(
    tree: &RuntimeProfileTree,
) -> Result<runtime_profile::TRuntimeProfileTree, String> {
    let mut nodes = Vec::new();
    native_profile_node_to_thrift_for_fe(&tree.root, &mut nodes)?;
    Ok(runtime_profile::TRuntimeProfileTree::new(nodes))
}

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

fn metadata_to_native_node_id_for_fe(metadata: i64) -> i32 {
    match i32::try_from(metadata) {
        Ok(value) => value,
        Err(_) if metadata.is_negative() => i32::MIN,
        Err(_) => i32::MAX,
    }
}

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

#[cfg(test)]
pub(crate) fn test_insert_report_instance(finst_id: UniqueId, query_id: QueryId) {
    let mut guard = registry().lock().expect("report registry lock");
    guard.insert(
        finst_id,
        ReportInstance {
            destination: ReportDestination::StarRocksFrontend(types::TNetworkAddress::new(
                "127.0.0.1".to_string(),
                0,
            )),
            backend_num: 1,
            query_id,
            enable_profile: false,
            profiler: None,
            mem_tracker: None,
            query_mem_tracker: None,
            report_interval_ns: None,
            fe_query_gone: false,
        },
    );
}

#[cfg(test)]
pub(crate) fn test_insert_standalone_report_instance(
    finst_id: UniqueId,
    query_id: QueryId,
    coord: RuntimeEndpoint,
    backend_num: i32,
) {
    let mut guard = registry().lock().expect("report registry lock");
    guard.insert(
        finst_id,
        ReportInstance {
            destination: ReportDestination::NovaRocksCoordinator(coord),
            backend_num,
            query_id,
            enable_profile: false,
            profiler: None,
            mem_tracker: None,
            query_mem_tracker: None,
            report_interval_ns: None,
            fe_query_gone: false,
        },
    );
}

#[cfg(test)]
pub(crate) fn test_reset_report_registry() {
    if let Ok(mut guard) = registry().lock() {
        guard.clear();
    }
}

#[cfg(test)]
pub(crate) fn test_is_fe_query_gone(finst_id: UniqueId) -> bool {
    registry()
        .lock()
        .ok()
        .and_then(|guard| guard.get(&finst_id).map(|instance| instance.fe_query_gone))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn test_report_registry_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
type FinalReportHook = Box<dyn Fn(&ExecStateReportTask) + Send + Sync + 'static>;
#[cfg(test)]
type NonFinalReportHook = Box<dyn Fn(&ExecStateReportTask) + Send + Sync + 'static>;
#[cfg(test)]
type StandaloneFinalReportHook =
    Box<dyn Fn(&StandaloneExecStateReportTask) + Send + Sync + 'static>;
#[cfg(test)]
type StandaloneNonFinalReportHook =
    Box<dyn Fn(&StandaloneExecStateReportTask) + Send + Sync + 'static>;

#[cfg(test)]
fn test_final_report_hook() -> &'static Mutex<Option<FinalReportHook>> {
    static HOOK: OnceLock<Mutex<Option<FinalReportHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_non_final_report_hook() -> &'static Mutex<Option<NonFinalReportHook>> {
    static HOOK: OnceLock<Mutex<Option<NonFinalReportHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_standalone_final_report_hook() -> &'static Mutex<Option<StandaloneFinalReportHook>> {
    static HOOK: OnceLock<Mutex<Option<StandaloneFinalReportHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_standalone_non_final_report_hook() -> &'static Mutex<Option<StandaloneNonFinalReportHook>> {
    static HOOK: OnceLock<Mutex<Option<StandaloneNonFinalReportHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_set_final_report_hook(hook: Option<FinalReportHook>) {
    *test_final_report_hook()
        .lock()
        .expect("final report hook lock") = hook;
}

#[cfg(test)]
fn test_set_non_final_report_hook(hook: Option<NonFinalReportHook>) {
    *test_non_final_report_hook()
        .lock()
        .expect("non-final report hook lock") = hook;
}

#[cfg(test)]
fn test_set_standalone_final_report_hook(hook: Option<StandaloneFinalReportHook>) {
    *test_standalone_final_report_hook()
        .lock()
        .expect("standalone final report hook lock") = hook;
}

#[cfg(test)]
fn test_set_standalone_non_final_report_hook(hook: Option<StandaloneNonFinalReportHook>) {
    *test_standalone_non_final_report_hook()
        .lock()
        .expect("standalone non-final report hook lock") = hook;
}

#[cfg(test)]
fn test_capture_final_report(task: &ExecStateReportTask) -> bool {
    let guard = test_final_report_hook()
        .lock()
        .expect("final report hook lock");
    let Some(hook) = guard.as_ref() else {
        return false;
    };
    hook(task);
    true
}

#[cfg(test)]
fn test_capture_non_final_report(task: &ExecStateReportTask) -> bool {
    let guard = test_non_final_report_hook()
        .lock()
        .expect("non-final report hook lock");
    let Some(hook) = guard.as_ref() else {
        return false;
    };
    hook(task);
    true
}

#[cfg(test)]
fn test_capture_standalone_final_report(task: &StandaloneExecStateReportTask) -> bool {
    let guard = test_standalone_final_report_hook()
        .lock()
        .expect("standalone final report hook lock");
    let Some(hook) = guard.as_ref() else {
        return false;
    };
    hook(task);
    true
}

#[cfg(test)]
fn test_capture_standalone_non_final_report(task: &StandaloneExecStateReportTask) -> bool {
    let guard = test_standalone_non_final_report_hook()
        .lock()
        .expect("standalone non-final report hook lock");
    let Some(hook) = guard.as_ref() else {
        return false;
    };
    hook(task);
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use super::{
        is_query_gone_status, mark_fe_query_gone, report_exec_state, report_fragment_done,
        test_insert_report_instance, test_insert_standalone_report_instance,
        test_reset_report_registry,
    };
    use crate::common::types::UniqueId;
    use crate::proto::novarocks;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::load_tracking;
    use crate::runtime::profile::{
        CounterAggregateType, CounterMergeType, CounterMinMaxType, CounterStrategy, ProfileCounter,
        ProfileNode, ProfileUnit, Profiler, RuntimeProfileTree, default_counter_strategy,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::service::exec_state_reporter;
    use crate::thrift::frontend_service;
    use crate::thrift::{metrics, runtime_profile, status, status_code};

    #[test]
    fn query_gone_status_is_treated_as_benign() {
        let status = status::TStatus::new(
            status_code::TStatusCode::NOT_FOUND,
            Some(vec!["query id abc not found".to_string()]),
        );
        assert!(is_query_gone_status(&status));
    }

    #[test]
    fn unrelated_not_found_status_is_not_query_gone() {
        let status = status::TStatus::new(
            status_code::TStatusCode::NOT_FOUND,
            Some(vec!["tablet not found".to_string()]),
        );
        assert!(!is_query_gone_status(&status));
    }

    #[test]
    fn fragment_done_is_skipped_when_query_is_already_gone() {
        let _guard = super::test_report_registry_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let finst_id = UniqueId { hi: 11, lo: 22 };
        let query_id = QueryId { hi: 33, lo: 44 };
        test_reset_report_registry();
        exec_state_reporter::test_clear_shared_queues();
        test_insert_report_instance(finst_id, query_id);

        mark_fe_query_gone(finst_id);
        report_fragment_done(finst_id, None, false);

        assert_eq!(exec_state_reporter::test_priority_queue_len(), 0);
        assert!(super::list_report_instances().is_empty());
        test_reset_report_registry();
    }

    struct ReportHookGuard;

    impl Drop for ReportHookGuard {
        fn drop(&mut self) {
            super::test_set_final_report_hook(None);
            super::test_set_non_final_report_hook(None);
            super::test_set_standalone_final_report_hook(None);
            super::test_set_standalone_non_final_report_hook(None);
        }
    }

    fn capture_final_report(
        captured: Arc<Mutex<Option<frontend_service::TReportExecStatusParams>>>,
    ) -> ReportHookGuard {
        super::test_set_final_report_hook(Some(Box::new(move |task| {
            *captured.lock().expect("capture final report params") = Some(task.params.clone());
        })));
        super::test_set_standalone_final_report_hook(Some(Box::new(|_| {
            panic!("standalone final reporter should not receive FE report");
        })));
        ReportHookGuard
    }

    type CapturedReport = Option<(RuntimeEndpoint, novarocks::ExecStatusReport)>;

    fn capture_standalone_final_report(captured: Arc<Mutex<CapturedReport>>) -> ReportHookGuard {
        super::test_set_final_report_hook(Some(Box::new(|_| {
            panic!("FE final reporter should not receive standalone report");
        })));
        super::test_set_standalone_final_report_hook(Some(Box::new(move |task| {
            *captured.lock().expect("capture standalone final report") =
                Some((task.coord.clone(), task.report.clone()));
        })));
        ReportHookGuard
    }

    fn capture_standalone_non_final_report(
        captured: Arc<Mutex<CapturedReport>>,
    ) -> ReportHookGuard {
        super::test_set_non_final_report_hook(Some(Box::new(|_| {
            panic!("FE non-final reporter should not receive standalone report");
        })));
        super::test_set_standalone_non_final_report_hook(Some(Box::new(move |task| {
            *captured
                .lock()
                .expect("capture standalone non-final report") =
                Some((task.coord.clone(), task.report.clone()));
        })));
        ReportHookGuard
    }

    fn empty_thrift_profile_node(
        name: &str,
        num_children: i32,
    ) -> runtime_profile::TRuntimeProfileNode {
        runtime_profile::TRuntimeProfileNode::new(
            name.to_string(),
            num_children,
            vec![],
            0,
            false,
            BTreeMap::new(),
            vec![],
            BTreeMap::new(),
            None,
        )
    }

    #[test]
    fn runtime_profile_tree_from_thrift_for_fe_rejects_negative_num_children() {
        let tree =
            runtime_profile::TRuntimeProfileTree::new(vec![empty_thrift_profile_node("Root", -1)]);

        let err = super::runtime_profile_tree_from_thrift_for_fe(&tree)
            .expect_err("negative child count should be rejected");

        assert!(
            err.contains("negative num_children"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn runtime_profile_tree_from_thrift_for_fe_rejects_missing_declared_children() {
        let tree = runtime_profile::TRuntimeProfileTree::new(vec![
            empty_thrift_profile_node("Root", 2),
            empty_thrift_profile_node("OnlyChild", 0),
        ]);

        let err = super::runtime_profile_tree_from_thrift_for_fe(&tree)
            .expect_err("missing declared child should be rejected");

        assert!(
            err.contains("declares more children than available nodes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn runtime_profile_tree_from_thrift_for_fe_rejects_trailing_nodes() {
        let tree = runtime_profile::TRuntimeProfileTree::new(vec![
            empty_thrift_profile_node("Root", 0),
            empty_thrift_profile_node("Trailing", 0),
        ]);

        let err = super::runtime_profile_tree_from_thrift_for_fe(&tree)
            .expect_err("trailing nodes should be rejected");

        assert!(err.contains("trailing nodes"), "unexpected error: {err}");
    }

    #[test]
    fn runtime_profile_tree_to_thrift_for_fe_preserves_counter_strategy_fields() {
        let profile = Profiler::new("strategy-profile");
        let counter = profile.add_counter_with_strategy(
            "CustomCounter",
            ProfileUnit::TimeMs,
            CounterStrategy::custom(
                CounterAggregateType::SumAvg,
                CounterMergeType::SkipFirstMerge,
                42,
                Some(CounterMinMaxType::SkipAll),
            ),
        );
        counter.set(99);
        counter.set_min(7);
        counter.set_max(123);

        let tree = super::profile_to_thrift_tree_for_fe(&profile);
        let thrift_counter = tree.nodes[0]
            .counters
            .iter()
            .find(|counter| counter.name == "CustomCounter")
            .expect("custom counter");
        let strategy = thrift_counter.strategy.as_ref().expect("counter strategy");

        assert_eq!(thrift_counter.type_, metrics::TUnit::TIME_MS);
        assert_eq!(thrift_counter.value, 99);
        assert_eq!(thrift_counter.min_value, Some(7));
        assert_eq!(thrift_counter.max_value, Some(123));
        assert_eq!(
            strategy.aggregate_type,
            runtime_profile::TCounterAggregateType::SUM_AVG
        );
        assert_eq!(
            strategy.merge_type,
            runtime_profile::TCounterMergeType::SKIP_FIRST_MERGE
        );
        assert_eq!(strategy.display_threshold, 42);
        assert_eq!(
            strategy.min_max_type,
            Some(runtime_profile::TCounterMinMaxType::SKIP_ALL)
        );
    }

    #[test]
    fn runtime_profile_tree_to_thrift_for_fe_reconstructs_flat_tree() {
        let native = RuntimeProfileTree {
            root: ProfileNode {
                name: "Root".to_string(),
                node_id: 10,
                counters: vec![
                    ProfileCounter {
                        name: "TotalTime".to_string(),
                        parent_name: String::new(),
                        unit: ProfileUnit::TimeNs,
                        strategy: default_counter_strategy(ProfileUnit::TimeNs),
                        value: 100,
                        min_value: Some(90),
                        max_value: Some(110),
                    },
                    ProfileCounter {
                        name: "ScanTime".to_string(),
                        parent_name: "TotalTime".to_string(),
                        unit: ProfileUnit::TimeNs,
                        strategy: default_counter_strategy(ProfileUnit::TimeNs),
                        value: 70,
                        min_value: None,
                        max_value: None,
                    },
                ],
                info_strings: BTreeMap::from([
                    ("z_key".to_string(), "last".to_string()),
                    ("a_key".to_string(), "first".to_string()),
                ]),
                children: vec![ProfileNode {
                    name: "Child".to_string(),
                    node_id: 20,
                    counters: vec![ProfileCounter {
                        name: "RowsRead".to_string(),
                        parent_name: String::new(),
                        unit: ProfileUnit::Unit,
                        strategy: default_counter_strategy(ProfileUnit::Unit),
                        value: 9,
                        min_value: None,
                        max_value: None,
                    }],
                    info_strings: BTreeMap::new(),
                    children: vec![],
                }],
            },
        };

        let thrift =
            super::runtime_profile_tree_to_thrift_for_fe(&native).expect("native profile encodes");

        assert_eq!(thrift.nodes.len(), 2);
        assert_eq!(thrift.nodes[0].name, "Root");
        assert_eq!(thrift.nodes[0].num_children, 1);
        assert_eq!(thrift.nodes[0].metadata, 10);
        assert_eq!(
            thrift.nodes[0].info_strings_display_order,
            vec!["a_key".to_string(), "z_key".to_string()]
        );
        assert_eq!(
            thrift.nodes[0]
                .child_counters_map
                .get("")
                .expect("root counters"),
            &["TotalTime".to_string()].into_iter().collect()
        );
        assert_eq!(
            thrift.nodes[0]
                .child_counters_map
                .get("TotalTime")
                .expect("child counters"),
            &["ScanTime".to_string()].into_iter().collect()
        );
        assert_eq!(thrift.nodes[1].name, "Child");
        assert_eq!(thrift.nodes[1].metadata, 20);

        let decoded = super::runtime_profile_tree_from_thrift_for_fe(&thrift)
            .expect("thrift profile decodes");
        let scan_time = decoded
            .root
            .counters
            .iter()
            .find(|counter| counter.name == "ScanTime")
            .expect("ScanTime counter");
        assert_eq!(scan_time.parent_name, "TotalTime");
        assert_eq!(
            scan_time.strategy.aggregate_type(),
            CounterAggregateType::Avg
        );
    }

    #[test]
    fn fragment_done_passes_tracking_url_to_enqueued_report() {
        let _guard = super::test_report_registry_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let finst_id = UniqueId { hi: 51, lo: 52 };
        let query_id = QueryId { hi: 61, lo: 62 };
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_final_report(Arc::clone(&captured));

        crate::novarocks_config::install_default_for_test();
        test_reset_report_registry();
        exec_state_reporter::test_clear_shared_queues();
        test_insert_report_instance(finst_id, query_id);
        load_tracking::append_logs(query_id, ["rejected row".to_string()]);

        report_fragment_done(finst_id, None, false);

        let params = captured
            .lock()
            .expect("inspect captured final report params")
            .clone()
            .expect("final report params were captured");
        assert_eq!(
            params.tracking_url.as_deref(),
            Some("http://127.0.0.1:8040/api/_load_tracking/61/62")
        );
        test_reset_report_registry();
    }

    #[test]
    fn standalone_fragment_done_routes_final_report_to_standalone_reporter() {
        let _guard = super::test_report_registry_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let finst_id = UniqueId { hi: 71, lo: 72 };
        let query_id = QueryId { hi: 81, lo: 82 };
        let report_addr = RuntimeEndpoint::new("127.0.0.1", 18040).expect("report endpoint");
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_standalone_final_report(Arc::clone(&captured));

        test_reset_report_registry();
        test_insert_standalone_report_instance(finst_id, query_id, report_addr.clone(), 3);

        report_fragment_done(finst_id, None, false);

        let (coord, report) = captured
            .lock()
            .expect("inspect standalone final report")
            .clone()
            .expect("standalone final report was captured");
        assert_eq!(coord, report_addr);
        assert!(report.done);
        assert_eq!(report.backend_num, 3);
        assert_eq!(
            report.fragment_instance_id,
            Some(crate::proto::common::UniqueId { hi: 71, lo: 72 })
        );
        test_reset_report_registry();
    }

    #[test]
    fn standalone_report_exec_state_routes_non_final_report_to_standalone_reporter() {
        let _guard = super::test_report_registry_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let finst_id = UniqueId { hi: 73, lo: 74 };
        let query_id = QueryId { hi: 83, lo: 84 };
        let report_addr = RuntimeEndpoint::new("127.0.0.1", 18041).expect("report endpoint");
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_standalone_non_final_report(Arc::clone(&captured));

        test_reset_report_registry();
        test_insert_standalone_report_instance(finst_id, query_id, report_addr.clone(), 4);

        report_exec_state(finst_id);

        let (coord, report) = captured
            .lock()
            .expect("inspect standalone non-final report")
            .clone()
            .expect("standalone non-final report was captured");
        assert_eq!(coord, report_addr);
        assert!(!report.done);
        assert_eq!(report.backend_num, 4);
        assert_eq!(
            report.fragment_instance_id,
            Some(crate::proto::common::UniqueId { hi: 73, lo: 74 })
        );
        test_reset_report_registry();
    }

    #[test]
    fn mark_fe_query_gone_does_not_suppress_standalone_destination() {
        let _guard = super::test_report_registry_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let finst_id = UniqueId { hi: 75, lo: 76 };
        let query_id = QueryId { hi: 85, lo: 86 };
        let report_addr = RuntimeEndpoint::new("127.0.0.1", 18042).expect("report endpoint");
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_standalone_final_report(Arc::clone(&captured));

        test_reset_report_registry();
        test_insert_standalone_report_instance(finst_id, query_id, report_addr.clone(), 5);
        mark_fe_query_gone(finst_id);

        report_fragment_done(finst_id, None, false);

        let (coord, report) = captured
            .lock()
            .expect("inspect standalone final report")
            .clone()
            .expect("standalone final report was captured");
        assert_eq!(coord, report_addr);
        assert!(report.done);
        assert_eq!(report.backend_num, 5);
        test_reset_report_registry();
    }

    #[test]
    fn build_profile_tree_exports_runtime_filter_lifecycle() {
        let query_id = QueryId { hi: 91, lo: 92 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).built(17, 5, 256);
        registry.recorder(query_key).applied(17, 100, 40, 1);

        let profiler = Profiler::new("Query");
        let tree = super::build_profile_tree(query_id, true, Some(&profiler), None, None, true)
            .expect("profile tree");

        let rf_node = tree
            .nodes
            .iter()
            .find(|node| node.name == "RuntimeFilters")
            .expect("runtime filters node");
        assert_eq!(rf_node.num_children, 1);
        let filter_node = tree
            .nodes
            .iter()
            .find(|node| node.name == "Filter17")
            .expect("filter node");
        let counter_value = |name: &str| {
            filter_node
                .counters
                .iter()
                .find(|counter| counter.name == name)
                .map(|counter| counter.value)
        };
        assert_eq!(counter_value("BuiltRows"), Some(5));
        assert_eq!(counter_value("BuiltBytes"), Some(256));
        assert_eq!(counter_value("AppliedInputRows"), Some(100));
        assert_eq!(counter_value("AppliedOutputRows"), Some(40));

        registry.remove_query(query_key);
    }
}
