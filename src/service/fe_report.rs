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

use std::collections::{BTreeMap, HashMap};
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
use crate::service::frontend_rpc::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager};
use crate::service::report_worker;
use crate::{
    data_cache, frontend_service, internal_service, metrics, runtime_profile, status, status_code,
    types,
};

#[derive(Clone, Debug)]
struct ReportInstance {
    coord: types::TNetworkAddress,
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
    report_worker::ensure_started();
    exec_state_reporter::ensure_started();
    let mut guard = registry().lock().expect("report registry lock");
    guard.insert(
        finst_id,
        ReportInstance {
            coord,
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
    let params = build_report_params(
        &instance,
        finst_id,
        status,
        true,
        profile,
        None,
        load_datacache_metrics,
    );
    exec_state_reporter::enqueue_final(ExecStateReportTask {
        finst_id,
        query_id: instance.query_id,
        coord: instance.coord.clone(),
        params,
    });
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
    let params = build_report_params(
        &instance,
        finst_id,
        status,
        false,
        profile,
        None,
        load_datacache_metrics,
    );
    if let Err(e) = exec_state_reporter::enqueue_non_final(ExecStateReportTask {
        finst_id,
        query_id: instance.query_id,
        coord: instance.coord.clone(),
        params,
    }) {
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

    if let Some(status) = result.status {
        if status.status_code != status_code::TStatusCode::OK {
            return Err(format!("FE returned error: {:?}", status));
        }
    }
    let payload = result
        .query_result
        .and_then(|mut v| v.drain(..).next())
        .unwrap_or_default();
    Ok(payload)
}

fn build_report_params(
    instance: &ReportInstance,
    finst_id: UniqueId,
    status: status::TStatus,
    done: bool,
    profile: Option<runtime_profile::TRuntimeProfileTree>,
    load_channel_profile: Option<runtime_profile::TRuntimeProfileTree>,
    load_datacache_metrics: Option<data_cache::TLoadDataCacheMetrics>,
) -> frontend_service::TReportExecStatusParams {
    let sink_commit_infos = sink_commit::list(finst_id);
    let tablet_commit_infos = sink_commit::list_tablet_commit_infos(finst_id);
    let tablet_fail_infos = sink_commit::list_tablet_fail_infos(finst_id);
    // NOTE: FE derives loadedRows from load_counters (dpp.norm.ALL). If we only
    // report sink/tablet commit infos without load_counters, FE may treat the
    // insert as "no rows" and skip first-load statistics collection.
    let state_stats = sink_commit::get_load_stats(finst_id);
    let mut normal_rows: i64 = state_stats.loaded_rows.max(0);
    let mut loaded_bytes: i64 = state_stats.loaded_bytes.max(0);
    let filtered_rows: i64 = state_stats.filtered_rows.max(0);
    for info in &sink_commit_infos {
        if let Some(file) = info.iceberg_data_file.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
        if let Some(file) = info.hive_file_info.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
    }
    // NOTE: Use the same counter keys FE expects (LoadEtlTask.DPP_NORMAL_ALL / DPP_ABNORMAL_ALL).
    // Missing or mismatched keys will make FE see loadedRows=0.
    let load_counters = if normal_rows > 0 || loaded_bytes > 0 || filtered_rows > 0 {
        let mut counters = BTreeMap::new();
        counters.insert("dpp.norm.ALL".to_string(), normal_rows.to_string());
        counters.insert("dpp.abnorm.ALL".to_string(), filtered_rows.to_string());
        if loaded_bytes > 0 {
            counters.insert("loaded.bytes".to_string(), loaded_bytes.to_string());
        }
        Some(counters)
    } else {
        None
    };
    let tracking_url = build_tracking_url(instance.query_id);
    debug!(
        target: "novarocks::sink_commit",
        finst_id = %finst_id,
        backend_num = instance.backend_num,
        query_id = %instance.query_id,
        tablet_commit_info_len = tablet_commit_infos.len(),
        tablet_fail_info_len = tablet_fail_infos.len(),
        commit_info_len = sink_commit_infos.len(),
        done = done,
        "reportExecStatus sink/tablet commit infos"
    );
    let tablet_commit_infos = if tablet_commit_infos.is_empty() {
        None
    } else {
        Some(tablet_commit_infos)
    };
    let sink_commit_infos = if sink_commit_infos.is_empty() {
        None
    } else {
        Some(sink_commit_infos)
    };
    let tablet_fail_infos = if tablet_fail_infos.is_empty() {
        None
    } else {
        Some(tablet_fail_infos)
    };
    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(types::TUniqueId {
            hi: instance.query_id.hi,
            lo: instance.query_id.lo,
        }),
        Some(instance.backend_num),
        Some(types::TUniqueId {
            hi: finst_id.hi,
            lo: finst_id.lo,
        }),
        Some(status),
        Some(done),
        profile,
        Option::<Vec<String>>::None,
        Option::<Vec<String>>::None,
        load_counters,
        tracking_url,
        Option::<Vec<String>>::None,
        tablet_commit_infos,
        (normal_rows > 0).then_some(normal_rows),
        Option::<i64>::None,
        (loaded_bytes > 0).then_some(loaded_bytes),
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<internal_service::TLoadJobType>::None,
        tablet_fail_infos,
        (filtered_rows > 0).then_some(filtered_rows),
        Option::<i64>::None,
        Option::<i64>::None,
        sink_commit_infos,
        Option::<String>::None,
        load_channel_profile,
        load_datacache_metrics,
    )
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
    let Some(profile) = profile else {
        return None;
    };
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

fn merge_pipeline_profiles_for_fe(profiler: &Profiler) -> Profiler {
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
            coord: types::TNetworkAddress::new("127.0.0.1".to_string(), 0),
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
mod tests {
    use super::{
        is_query_gone_status, mark_fe_query_gone, report_fragment_done,
        test_insert_report_instance, test_reset_report_registry,
    };
    use crate::common::types::UniqueId;
    use crate::runtime::query_context::QueryId;
    use crate::service::exec_state_reporter;
    use crate::{status, status_code};

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
}
