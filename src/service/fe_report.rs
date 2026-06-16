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
use std::sync::{Arc, Mutex, OnceLock};

use crate::cache::DataCacheManager;
use crate::common::network;
use crate::common::types::UniqueId;
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_logging::{debug, warn};
use crate::runtime::load_tracking;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;
use crate::runtime::sink_commit;
use crate::service::exec_state_reporter::{self, ExecStateReportTask};
use crate::service::exec_status_report::{self, ExecStatusReportInput};
use crate::service::frontend_rpc::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager};
use crate::service::report_worker;
use crate::service::standalone_exec_state_reporter::{self, StandaloneExecStateReportTask};
use crate::{data_cache, frontend_service, metrics, runtime_profile, status, status_code, types};

#[derive(Clone, Debug)]
enum ReportDestination {
    StarRocksFrontend(types::TNetworkAddress),
    NovaRocksCoordinator(types::TNetworkAddress),
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

pub(crate) fn report_fragment_done(finst_id: UniqueId, error: Option<String>) {
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
    let profile = build_profile_tree(
        instance.enable_profile,
        instance.profiler.as_ref(),
        instance.mem_tracker.as_ref(),
        instance.query_mem_tracker.as_ref(),
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
    });
    match instance.destination {
        ReportDestination::StarRocksFrontend(coord) => {
            enqueue_final_report(ExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
            });
        }
        ReportDestination::NovaRocksCoordinator(coord) => {
            enqueue_standalone_final_report(StandaloneExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
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
    let profile = build_profile_tree(
        instance.enable_profile,
        instance.profiler.as_ref(),
        instance.mem_tracker.as_ref(),
        instance.query_mem_tracker.as_ref(),
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
    });
    let enqueue_result = match instance.destination {
        ReportDestination::StarRocksFrontend(coord) => {
            enqueue_non_final_report(ExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
            })
        }
        ReportDestination::NovaRocksCoordinator(coord) => {
            enqueue_standalone_non_final_report(StandaloneExecStateReportTask {
                finst_id,
                query_id: instance.query_id,
                coord,
                params,
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

fn build_profile_tree(
    enable_profile: bool,
    profiler: Option<&Profiler>,
    mem_tracker: Option<&Arc<MemTracker>>,
    query_mem_tracker: Option<&Arc<MemTracker>>,
) -> Option<runtime_profile::TRuntimeProfileTree> {
    if !enable_profile {
        return None;
    }
    let profiler = profiler?;
    let merged = merge_pipeline_profiles_for_fe(profiler);
    if let Some(tracker) = mem_tracker {
        merged.counter_set(
            "InstancePeakMemoryUsage",
            metrics::TUnit::BYTES,
            tracker.peak(),
        );
        merged.counter_set(
            "InstanceAllocatedMemoryUsage",
            metrics::TUnit::BYTES,
            tracker.allocated(),
        );
        merged.counter_set(
            "InstanceDeallocatedMemoryUsage",
            metrics::TUnit::BYTES,
            tracker.deallocated(),
        );
    }
    if let Some(tracker) = query_mem_tracker {
        merged.counter_set(
            "QueryPeakMemoryUsage",
            metrics::TUnit::BYTES,
            tracker.peak(),
        );
    }
    let mut tree = merged.to_thrift_tree();
    normalize_profile_tree_for_fe(&mut tree);
    Some(tree)
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
    coord: types::TNetworkAddress,
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
    use std::sync::{Arc, Mutex};

    use super::{
        is_query_gone_status, mark_fe_query_gone, report_exec_state, report_fragment_done,
        test_insert_report_instance, test_insert_standalone_report_instance,
        test_reset_report_registry,
    };
    use crate::common::types::UniqueId;
    use crate::frontend_service;
    use crate::runtime::load_tracking;
    use crate::runtime::query_context::QueryId;
    use crate::service::exec_state_reporter;
    use crate::{status, status_code, types};

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
        report_fragment_done(finst_id, None);

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

    type CapturedReport = Option<(
        types::TNetworkAddress,
        frontend_service::TReportExecStatusParams,
    )>;

    fn capture_standalone_final_report(captured: Arc<Mutex<CapturedReport>>) -> ReportHookGuard {
        super::test_set_final_report_hook(Some(Box::new(|_| {
            panic!("FE final reporter should not receive standalone report");
        })));
        super::test_set_standalone_final_report_hook(Some(Box::new(move |task| {
            *captured.lock().expect("capture standalone final report") =
                Some((task.coord.clone(), task.params.clone()));
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
                Some((task.coord.clone(), task.params.clone()));
        })));
        ReportHookGuard
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

        report_fragment_done(finst_id, None);

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
        let report_addr = types::TNetworkAddress::new("127.0.0.1".to_string(), 18040);
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_standalone_final_report(Arc::clone(&captured));

        test_reset_report_registry();
        test_insert_standalone_report_instance(finst_id, query_id, report_addr.clone(), 3);

        report_fragment_done(finst_id, None);

        let (coord, params) = captured
            .lock()
            .expect("inspect standalone final report")
            .clone()
            .expect("standalone final report was captured");
        assert_eq!(coord, report_addr);
        assert_eq!(params.done, Some(true));
        assert_eq!(params.backend_num, Some(3));
        assert_eq!(
            params.fragment_instance_id,
            Some(types::TUniqueId::new(71, 72))
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
        let report_addr = types::TNetworkAddress::new("127.0.0.1".to_string(), 18041);
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_standalone_non_final_report(Arc::clone(&captured));

        test_reset_report_registry();
        test_insert_standalone_report_instance(finst_id, query_id, report_addr.clone(), 4);

        report_exec_state(finst_id);

        let (coord, params) = captured
            .lock()
            .expect("inspect standalone non-final report")
            .clone()
            .expect("standalone non-final report was captured");
        assert_eq!(coord, report_addr);
        assert_eq!(params.done, Some(false));
        assert_eq!(params.backend_num, Some(4));
        assert_eq!(
            params.fragment_instance_id,
            Some(types::TUniqueId::new(73, 74))
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
        let report_addr = types::TNetworkAddress::new("127.0.0.1".to_string(), 18042);
        let captured = Arc::new(Mutex::new(None));
        let _hook = capture_standalone_final_report(Arc::clone(&captured));

        test_reset_report_registry();
        test_insert_standalone_report_instance(finst_id, query_id, report_addr.clone(), 5);
        mark_fe_query_gone(finst_id);

        report_fragment_done(finst_id, None);

        let (coord, params) = captured
            .lock()
            .expect("inspect standalone final report")
            .clone()
            .expect("standalone final report was captured");
        assert_eq!(coord, report_addr);
        assert_eq!(params.done, Some(true));
        assert_eq!(params.backend_num, Some(5));
        test_reset_report_registry();
    }
}
