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
use std::sync::Arc;

use crate::cache::CacheOptions;
use crate::common::app_config;
use crate::common::types::UniqueId;
use crate::lower_native::execute_fragment_native;
use crate::novarocks_logging::{error, info, warn};
use crate::runtime::exchange;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::native_fragment_wire::{
    network_address_from_native, query_options_from_native, runtime_filter_params_from_native,
};
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{
    QueryContextManager, QueryId, query_context_manager, query_expire_durations,
};
use crate::runtime::result_buffer;
use crate::service::fe_report;

fn unique_id_from_native(src: &crate::proto::common::UniqueId) -> UniqueId {
    UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn query_id_from_native(src: &crate::proto::common::UniqueId) -> QueryId {
    QueryId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn profile_name_for_native_fragment(fragment: &crate::proto::plan::PlanFragment) -> String {
    let plan_node_id = fragment
        .root
        .as_ref()
        .map(|root| root.node_id)
        .unwrap_or(-1);
    if plan_node_id >= 0 {
        format!("execute_fragment_native (plan_node_id={plan_node_id})")
    } else {
        "execute_fragment_native".to_string()
    }
}

fn native_exchange_sender_counts(
    instance_params: &crate::proto::novarocks::InstanceParams,
) -> Result<HashMap<i32, usize>, String> {
    instance_params
        .per_exch_num_senders
        .iter()
        .map(|(node_id, count)| {
            if *count <= 0 {
                return Err(format!(
                    "native InstanceParams per_exch_num_senders node_id={} must be positive, got {}",
                    node_id, count
                ));
            }
            let count = usize::try_from(*count).map_err(|_| {
                format!(
                    "native InstanceParams per_exch_num_senders node_id={} cannot convert {} to usize",
                    node_id, count
                )
            })?;
            Ok((*node_id, count))
        })
        .collect()
}

fn native_fragment_uses_fetch_result_buffer(fragment: &crate::proto::plan::PlanFragment) -> bool {
    matches!(
        fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref()),
        Some(crate::proto::plan::data_sink::Kind::Result(true))
    )
}

fn prepare_native_result_buffer_if_needed(
    fragment: &crate::proto::plan::PlanFragment,
    finst_id: UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) {
    if !native_fragment_uses_fetch_result_buffer(fragment) {
        return;
    }
    if typed_result_sink {
        result_buffer::create_typed_sender(finst_id);
    } else {
        result_buffer::create_sender(finst_id);
    }
    if let Some(root) = mem_tracker {
        let label = format!("ResultBuffer: finst={}", finst_id);
        let tracker = MemTracker::new_child(label, root);
        result_buffer::set_mem_tracker(finst_id, tracker);
    }
}

fn spawn_exec_fragment_native(
    fragment: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
    pipeline_dop: i32,
    finst_id: UniqueId,
    query_id: QueryId,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    mgr: Arc<QueryContextManager>,
) {
    let uses_fetch_result_buffer = native_fragment_uses_fetch_result_buffer(&fragment);
    if uses_fetch_result_buffer {
        prepare_native_result_buffer_if_needed(
            &fragment,
            finst_id,
            instance_params.typed_result_sink,
            mem_tracker.as_ref(),
        );
    }
    mgr.register_finst(finst_id, query_id);
    std::thread::spawn(move || {
        let wall_start = std::time::Instant::now();
        let profiler_for_wall = profiler.clone();
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_fragment_native(
                &fragment,
                &instance_params,
                None,
                pipeline_dop,
                None,
                profiler,
                mem_tracker,
            )
        }))
        .unwrap_or_else(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic payload".to_string()
            };
            Err(format!("panic in native fragment execution: {msg}"))
        });
        if let Some(p) = profiler_for_wall.as_ref() {
            let elapsed_ns =
                crate::runtime::profile::clamp_u128_to_i64(wall_start.elapsed().as_nanos());
            p.counter_set(
                "QueryExecutionWallTime",
                crate::thrift::metrics::TUnit::TIME_NS,
                elapsed_ns,
            );
        }
        let mut report_error: Option<String> = None;
        if uses_fetch_result_buffer {
            match out {
                Ok(out) => {
                    if let Some(json) = out.profile_json.as_deref() {
                        info!(
                            target: "novarocks::profile",
                            finst_id = %finst_id,
                            profile_bytes = json.len(),
                            "native_fragment_profile"
                        );
                    }
                }
                Err(e) => {
                    report_error = Some(e.clone());
                    error!(
                        target: "novarocks::exec",
                        finst_id = %finst_id,
                        error = %e,
                        "exec_plan_fragment_native failed"
                    );
                    result_buffer::close_error(finst_id, e);
                }
            }
        } else if let Err(e) = out {
            report_error = Some(e.clone());
            error!(
                target: "novarocks::exec",
                finst_id = %finst_id,
                error = %e,
                "exec_plan_fragment_native failed"
            );
        }
        if let Some(ref err_msg) = report_error {
            let finsts = mgr.cancel_query(query_id, err_msg.clone());
            for id in finsts {
                result_buffer::close_error(id, err_msg.clone());
                exchange::cancel_fragment(id.hi, id.lo);
            }
        }
        let report_decision = mgr.finish_fragment_for_report(query_id);
        fe_report::report_fragment_done(
            finst_id,
            report_error,
            report_decision.include_runtime_filter_profile,
        );
        exchange::remove_fragment(finst_id.hi, finst_id.lo);
        mgr.unregister_finst(finst_id);
        mgr.cleanup_after_fragment_report(query_id, report_decision);
    });
}

pub fn submit_exec_plan_fragment_native(
    fragment: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
) -> Result<(), String> {
    let query_id = instance_params
        .query_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing query_id".to_string())
        .map(query_id_from_native)?;
    let finst_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())
        .map(unique_id_from_native)?;
    let query_opts = instance_params
        .query_options
        .as_ref()
        .map(query_options_from_native)
        .transpose()?;
    let (delivery_expire, query_expire) = query_expire_durations(query_opts.as_ref());
    let mgr = query_context_manager();
    mgr.get_or_register(query_id, false, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(query_opts.as_ref())?;
    mgr.set_cache_options(query_id, cache_options)?;

    let sender_counts = native_exchange_sender_counts(&instance_params)?;
    if !sender_counts.is_empty() {
        mgr.update_exchange_sender_counts(query_id, sender_counts)?;
    }
    if let Some(rf_params) = instance_params
        .runtime_filter_params
        .as_ref()
        .map(runtime_filter_params_from_native)
        .transpose()?
    {
        let _ = mgr.set_runtime_filter_params(query_id, rf_params);
    }

    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let enable_profile = query_opts
        .as_ref()
        .and_then(|opts| opts.enable_profile)
        .unwrap_or(false);
    let profiler = if enable_profile {
        Some(Profiler::new(profile_name_for_native_fragment(&fragment)))
    } else {
        None
    };
    let report_interval_ns = if enable_profile {
        app_config::config()
            .ok()
            .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
    } else {
        None
    };
    if let Some(report_addr) = instance_params
        .report_addr
        .as_deref()
        .filter(|addr| !addr.is_empty())
        .map(network_address_from_native)
        .transpose()?
    {
        fe_report::register_novarocks_instance(
            finst_id,
            query_id,
            report_addr,
            instance_params.backend_num,
            enable_profile,
            profiler.clone(),
            Some(Arc::clone(&fragment_mem_tracker)),
            Some(Arc::clone(&query_mem_tracker)),
            report_interval_ns,
        );
    } else {
        warn!(
            target: "novarocks::report",
            finst_id = %finst_id,
            "missing native report_addr for reportExecStatus"
        );
    }

    let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(
        query_opts
            .as_ref()
            .and_then(|opts| opts.pipeline_dop)
            .unwrap_or(0),
    );
    spawn_exec_fragment_native(
        fragment,
        instance_params,
        pipeline_dop,
        finst_id,
        query_id,
        profiler,
        Some(fragment_mem_tracker),
        Arc::clone(&mgr),
    );
    Ok(())
}
