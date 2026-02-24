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
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol,
    TMessageIdentifier, TMessageType, TOutputProtocol, TSerializable, TStructIdentifier, TType,
};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::cache::DataCacheManager;
use crate::common::types::UniqueId;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;
use crate::runtime::sink_commit;
use crate::service::report_worker;
use crate::novarocks_logging::{debug, warn};
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
}

static REPORT_REGISTRY: OnceLock<Mutex<HashMap<UniqueId, ReportInstance>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<UniqueId, ReportInstance>> {
    REPORT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn read_frontend_result<T: TSerializable>(
    i_prot: &mut dyn TInputProtocol,
    call_name: &str,
) -> Result<T, String> {
    i_prot.read_struct_begin().map_err(|e| e.to_string())?;
    let mut value: Option<T> = None;
    loop {
        let field = i_prot.read_field_begin().map_err(|e| e.to_string())?;
        if field.field_type == TType::Stop {
            break;
        }
        match field.id {
            Some(0) => {
                let parsed = T::read_from_in_protocol(i_prot).map_err(|e| e.to_string())?;
                value = Some(parsed);
            }
            _ => {
                i_prot.skip(field.field_type).map_err(|e| e.to_string())?;
            }
        }
        i_prot.read_field_end().map_err(|e| e.to_string())?;
    }
    i_prot.read_struct_end().map_err(|e| e.to_string())?;
    value.ok_or_else(|| format!("missing {} result", call_name))
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

pub(crate) fn report_fragment_done(finst_id: UniqueId, error: Option<String>) {
    let instance = {
        let mut guard = registry().lock().expect("report registry lock");
        guard.remove(&finst_id)
    };
    let Some(instance) = instance else {
        debug!(
            target: "novarocks::report",
            finst_id_hi = finst_id.hi,
            finst_id_lo = finst_id.lo,
            "report instance missing"
        );
        return;
    };
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
    if let Err(e) = send_report(&instance.coord, params) {
        warn!(
            target: "novarocks::report",
            finst_id_hi = finst_id.hi,
            finst_id_lo = finst_id.lo,
            error = %e,
            "reportExecStatus failed"
        );
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
            finst_id_hi = finst_id.hi,
            finst_id_lo = finst_id.lo,
            "report instance missing"
        );
        return;
    };
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
    if let Err(e) = send_report(&instance.coord, params) {
        warn!(
            target: "novarocks::report",
            finst_id_hi = finst_id.hi,
            finst_id_lo = finst_id.lo,
            error = %e,
            "reportExecStatus failed"
        );
    }
}

pub(crate) fn fetch_query_profile(
    coord: &types::TNetworkAddress,
    query_id: &str,
) -> Result<String, String> {
    let addr: SocketAddr = format!("{}:{}", coord.hostname, coord.port)
        .parse()
        .map_err(|e| format!("invalid coord address: {e}"))?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| format!("connect FE failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel
        .split()
        .map_err(|e| format!("split thrift channel failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let mut i_prot = TBinaryInputProtocol::new(i_trans, true);
    let mut o_prot = TBinaryOutputProtocol::new(o_trans, true);

    let req = frontend_service::TGetProfileRequest::new(Some(vec![query_id.to_string()]));
    let seq_id = 0;
    o_prot
        .write_message_begin(&TMessageIdentifier::new(
            "getQueryProfile",
            TMessageType::Call,
            seq_id,
        ))
        .map_err(|e| e.to_string())?;
    o_prot
        .write_struct_begin(&TStructIdentifier::new("getQueryProfile_args"))
        .map_err(|e| e.to_string())?;
    o_prot
        .write_field_begin(&TFieldIdentifier::new("request", TType::Struct, 1))
        .map_err(|e| e.to_string())?;
    req.write_to_out_protocol(&mut o_prot)
        .map_err(|e| e.to_string())?;
    o_prot.write_field_end().map_err(|e| e.to_string())?;
    o_prot.write_field_stop().map_err(|e| e.to_string())?;
    o_prot.write_struct_end().map_err(|e| e.to_string())?;
    o_prot.write_message_end().map_err(|e| e.to_string())?;
    o_prot.flush().map_err(|e| e.to_string())?;

    let resp = i_prot.read_message_begin().map_err(|e| e.to_string())?;
    match resp.message_type {
        TMessageType::Reply => {}
        TMessageType::Exception => {
            let err = thrift::Error::read_application_error_from_in_protocol(&mut i_prot)
                .map_err(|e| format!("read thrift exception failed: {e}"))?;
            i_prot.read_message_end().map_err(|e| e.to_string())?;
            return Err(format!("thrift exception: {err}"));
        }
        other => {
            i_prot.read_message_end().map_err(|e| e.to_string())?;
            return Err(format!("unexpected response type: {:?}", other));
        }
    }
    if resp.name != "getQueryProfile" {
        i_prot.read_message_end().map_err(|e| e.to_string())?;
        return Err(format!("unexpected response name: {}", resp.name));
    }

    let result = read_frontend_result::<frontend_service::TGetProfileResponse>(
        &mut i_prot,
        "getQueryProfile",
    )?;
    i_prot.read_message_end().map_err(|e| e.to_string())?;

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
    let (state_rows, state_bytes) = sink_commit::get_load_counters(finst_id);
    let mut normal_rows: i64 = state_rows.max(0);
    let mut loaded_bytes: i64 = state_bytes.max(0);
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
    let load_counters = if normal_rows > 0 || loaded_bytes > 0 {
        let mut counters = BTreeMap::new();
        counters.insert("dpp.norm.ALL".to_string(), normal_rows.to_string());
        counters.insert("dpp.abnorm.ALL".to_string(), "0".to_string());
        if loaded_bytes > 0 {
            counters.insert("loaded.bytes".to_string(), loaded_bytes.to_string());
        }
        Some(counters)
    } else {
        None
    };
    debug!(
        target: "novarocks::sink_commit",
        finst_id_hi = finst_id.hi,
        finst_id_lo = finst_id.lo,
        backend_num = instance.backend_num,
        query_id_hi = instance.query_id.hi,
        query_id_lo = instance.query_id.lo,
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
        Option::<String>::None,
        Option::<Vec<String>>::None,
        tablet_commit_infos,
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<internal_service::TLoadJobType>::None,
        tablet_fail_infos,
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<i64>::None,
        sink_commit_infos,
        Option::<String>::None,
        load_channel_profile,
        load_datacache_metrics,
    )
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

fn send_report(
    coord: &types::TNetworkAddress,
    params: frontend_service::TReportExecStatusParams,
) -> Result<(), String> {
    let addr: SocketAddr = format!("{}:{}", coord.hostname, coord.port)
        .parse()
        .map_err(|e| format!("invalid coord address: {e}"))?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| format!("connect FE failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel
        .split()
        .map_err(|e| format!("split thrift channel failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let mut i_prot = TBinaryInputProtocol::new(i_trans, true);
    let mut o_prot = TBinaryOutputProtocol::new(o_trans, true);

    let seq_id = 0;
    o_prot
        .write_message_begin(&TMessageIdentifier::new(
            "reportExecStatus",
            TMessageType::Call,
            seq_id,
        ))
        .map_err(|e| e.to_string())?;
    o_prot
        .write_struct_begin(&TStructIdentifier::new("reportExecStatus_args"))
        .map_err(|e| e.to_string())?;
    o_prot
        .write_field_begin(&TFieldIdentifier::new("params", TType::Struct, 1))
        .map_err(|e| e.to_string())?;
    params
        .write_to_out_protocol(&mut o_prot)
        .map_err(|e| e.to_string())?;
    o_prot.write_field_end().map_err(|e| e.to_string())?;
    o_prot.write_field_stop().map_err(|e| e.to_string())?;
    o_prot.write_struct_end().map_err(|e| e.to_string())?;
    o_prot.write_message_end().map_err(|e| e.to_string())?;
    o_prot.flush().map_err(|e| e.to_string())?;

    let resp = i_prot.read_message_begin().map_err(|e| e.to_string())?;
    match resp.message_type {
        TMessageType::Reply => {}
        TMessageType::Exception => {
            let err = thrift::Error::read_application_error_from_in_protocol(&mut i_prot)
                .map_err(|e| format!("read thrift exception failed: {e}"))?;
            i_prot.read_message_end().map_err(|e| e.to_string())?;
            return Err(format!("thrift exception: {err}"));
        }
        other => {
            i_prot.read_message_end().map_err(|e| e.to_string())?;
            return Err(format!("unexpected response type: {:?}", other));
        }
    }
    if resp.name != "reportExecStatus" {
        i_prot.read_message_end().map_err(|e| e.to_string())?;
        return Err(format!("unexpected response name: {}", resp.name));
    }

    let result = read_frontend_result::<frontend_service::TReportExecStatusResult>(
        &mut i_prot,
        "reportExecStatus",
    )?;
    i_prot.read_message_end().map_err(|e| e.to_string())?;

    if let Some(status) = result.status {
        if status.status_code != status_code::TStatusCode::OK {
            return Err(format!("FE returned error: {:?}", status));
        }
    }
    Ok(())
}
