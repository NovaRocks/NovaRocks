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
use std::time::Duration;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan, push_down_local_runtime_filters};
use crate::exec::row_position::RowPositionDescriptor;
use crate::exec::spill::{QuerySpillManager, SpillConfig};
use crate::novarocks_connectors::ConnectorRegistry;

use crate::cache::CacheOptions;
use crate::common::config::{
    debug_exec_node_output, runtime_filter_scan_wait_time_ms_override,
    runtime_filter_wait_timeout_ms_override,
};
use crate::common::types::UniqueId;
use crate::exec::operators::{
    DataStreamSinkFactory, IcebergTableSinkFactory, MultiCastDataStreamSinkFactory,
    NoopSinkFactory, OlapTableSinkFactory, ResultSinkFactory, ResultSinkHandle,
    SplitDataStreamSinkFactory,
};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::lower::layout::{build_tuple_slot_order, infer_tuple_slot_order, reorder_tuple_slots};
use crate::lower::{Lowered, lower_plan};
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::runtime_state::RuntimeState;
use crate::{data_sinks, descriptors, internal_service, planner, types};

#[derive(Clone, Debug)]
pub(crate) struct FragmentOutput {
    pub(crate) chunks: Vec<Chunk>,
    pub(crate) profile_json: Option<String>,
}

fn merge_row_pos_descs(
    target: &mut HashMap<i32, RowPositionDescriptor>,
    incoming: &HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    for (tuple_id, desc) in incoming {
        match target.get(tuple_id) {
            None => {
                target.insert(*tuple_id, desc.clone());
            }
            Some(existing) => {
                if existing.row_position_type != desc.row_position_type
                    || existing.row_source_slot != desc.row_source_slot
                    || existing.fetch_ref_slots != desc.fetch_ref_slots
                    || existing.lookup_ref_slots != desc.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={}",
                        tuple_id
                    ));
                }
            }
        }
    }
    Ok(())
}

fn collect_glm_metadata(
    node: &ExecNode,
    row_pos_descs: &mut HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    match &node.kind {
        ExecNodeKind::LookUp(lookup) => {
            merge_row_pos_descs(row_pos_descs, &lookup.row_pos_descs)?;
        }
        ExecNodeKind::Fetch(fetch) => {
            merge_row_pos_descs(row_pos_descs, &fetch.row_pos_descs)?;
            collect_glm_metadata(&fetch.input, row_pos_descs)?;
        }
        ExecNodeKind::AssertNumRows(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Project(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Filter(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Repeat(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::UnionAll(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::Limit(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ExchangeSource(_) => {}
        ExecNodeKind::Scan(_) => {}
        ExecNodeKind::Aggregate(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Join(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::NestedLoopJoin(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::Sort(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::TableFunction(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Analytic(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::SetOp(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::Values(_) => {}
    }
    Ok(())
}

pub(crate) fn execute_fragment(
    fragment: &planner::TPlanFragment,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    pipeline_dop: i32,
    _group_execution_scan_dop: Option<i32>,
    db_name: Option<&str>,
    profiler: Option<Profiler>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    mem_tracker: Option<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>>,
) -> Result<FragmentOutput, String> {
    let mut query_opts = query_opts.cloned();
    if let Some(opts) = query_opts.as_mut() {
        if let Some(ms) = runtime_filter_scan_wait_time_ms_override() {
            opts.runtime_filter_scan_wait_time_ms = Some(ms);
        }
        if let Some(ms) = runtime_filter_wait_timeout_ms_override() {
            let ms = i32::try_from(ms).unwrap_or(i32::MAX);
            opts.runtime_filter_wait_timeout_ms = Some(ms);
        }
    }

    let profile_name = fragment
        .plan
        .as_ref()
        .and_then(|plan| plan.nodes.first().map(|n| n.node_id))
        .filter(|id| *id >= 0)
        .map(|id| format!("execute_fragment (plan_node_id={id})"));
    let profiler = if profiler.is_some() {
        profiler
    } else if query_opts
        .as_ref()
        .and_then(|opts| opts.enable_profile)
        .unwrap_or(false)
    {
        Some(Profiler::new(
            profile_name.as_deref().unwrap_or("execute_fragment"),
        ))
    } else {
        None
    };

    let query_id = exec_params.map(|params| QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    });
    let runtime_filter_params = exec_params.and_then(|params| params.runtime_filter_params.clone());
    let fragment_instance_id = exec_params.map(|params| UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    });
    let cache_options = CacheOptions::from_query_options(query_opts.as_ref())?;
    let spill_config = SpillConfig::from_query_options(query_opts.as_ref())?;
    let spill_manager = spill_config
        .as_ref()
        .map(|config| Arc::new(QuerySpillManager::new(config.clone(), profiler.as_ref())));
    let runtime_state = Arc::new(RuntimeState::new(
        query_opts.clone(),
        Some(cache_options),
        query_id,
        runtime_filter_params,
        fragment_instance_id,
        backend_num,
        mem_tracker,
        spill_config,
        spill_manager,
    ));

    if let Some(plan) = fragment.plan.as_ref() {
        let mut tuple_slots = build_tuple_slot_order(desc_tbl);
        let inferred = infer_tuple_slot_order(fragment);
        if tuple_slots.is_empty() {
            tuple_slots = inferred.clone();
        } else {
            for (tuple_id, slots) in &inferred {
                if tuple_slots.contains_key(tuple_id) {
                    continue;
                }
                tuple_slots.insert(*tuple_id, slots.clone());
            }
        }
        reorder_tuple_slots(&mut tuple_slots, desc_tbl);
        let mut arena = ExprArena::default();
        let allow_throw_exception = query_opts
            .as_ref()
            .map(|opts| {
                opts.allow_throw_exception.unwrap_or(false)
                    || matches!(
                        opts.overflow_mode,
                        Some(mode) if mode == internal_service::TOverflowMode::REPORT_ERROR
                    )
            })
            .unwrap_or(false);
        arena.set_allow_throw_exception(allow_throw_exception);
        // Layout hints are used by scan nodes to decide which columns to materialize.
        //
        // For exchange fragments, pruning only by "local usage" is not correct because downstream
        // fragments may require additional columns that do not appear in this fragment's exprs.
        // The descriptor table already encodes the materialized slots for each tuple, so we use it
        // as the source of truth to avoid producing mismatched layouts at runtime.
        let layout_hints = tuple_slots.clone();
        let connectors = ConnectorRegistry::default();
        let lowered: Lowered = {
            let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
            lower_plan(
                plan,
                &mut arena,
                &tuple_slots,
                desc_tbl,
                fragment.query_global_dicts.as_deref(),
                fragment.query_global_dict_exprs.as_ref(),
                exec_params,
                query_opts.as_ref(),
                db_name,
                &connectors,
                &layout_hints,
                last_query_id,
                fe_addr,
            )?
        };

        // PlanFragment must have a sink
        let sink = fragment
            .output_sink
            .as_ref()
            .ok_or_else(|| "PlanFragment must have output_sink field".to_string())?;

        let mut exec_plan = ExecPlan {
            arena,
            root: lowered.node,
        };
        if let Some(query_id) = query_id {
            let mut row_pos_descs = HashMap::new();
            collect_glm_metadata(&exec_plan.root, &mut row_pos_descs)?;
            if !row_pos_descs.is_empty() {
                query_context_manager().register_row_pos_descs(query_id, row_pos_descs)?;
            }
        }
        push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);
        let root_plan_node_id = plan.nodes.first().map(|n| n.node_id).unwrap_or(-1);

        let mut chunks = Vec::new();
        match sink.type_ {
            data_sinks::TDataSinkType::DATA_STREAM_SINK => {
                let stream_sink = sink
                    .stream_sink
                    .as_ref()
                    .ok_or_else(|| "DATA_STREAM_SINK missing stream_sink payload".to_string())?;
                let exec_params = exec_params
                    .ok_or_else(|| "DATA_STREAM_SINK requires exec_params".to_string())?;

                let sink_factory = DataStreamSinkFactory::new(
                    stream_sink.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                );
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                let multi_cast_stream_sink =
                    sink.multi_cast_stream_sink.as_ref().ok_or_else(|| {
                        "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload"
                            .to_string()
                    })?;
                let exec_params = exec_params.ok_or_else(|| {
                    "MULTI_CAST_DATA_STREAM_SINK requires exec_params".to_string()
                })?;

                let sink_factory = MultiCastDataStreamSinkFactory::new(
                    multi_cast_stream_sink.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                );
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
                let split_stream_sink = sink.split_stream_sink.as_ref().ok_or_else(|| {
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload".to_string()
                })?;
                let exec_params = exec_params
                    .ok_or_else(|| "SPLIT_DATA_STREAM_SINK requires exec_params".to_string())?;
                let split_exprs = split_stream_sink.split_exprs.as_ref().ok_or_else(|| {
                    "SPLIT_DATA_STREAM_SINK missing split_exprs payload".to_string()
                })?;

                let mut split_expr_ids = Vec::with_capacity(split_exprs.len());
                for expr in split_exprs {
                    split_expr_ids.push(crate::lower::expr::lower_t_expr(
                        expr,
                        &mut exec_plan.arena,
                        &lowered.layout,
                        last_query_id,
                        fe_addr,
                    )?);
                }

                let sink_factory = SplitDataStreamSinkFactory::new(
                    split_stream_sink.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                    Arc::new(exec_plan.arena.clone()),
                    split_expr_ids,
                );
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::RESULT_SINK => {
                let result_sink_handle = ResultSinkHandle::new();
                let sink_factory = ResultSinkFactory::new(result_sink_handle.clone());
                let exchange_finst_id = exec_params.map(|params| {
                    (
                        params.fragment_instance_id.hi,
                        params.fragment_instance_id.lo,
                    )
                });
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
                chunks = result_sink_handle.take_chunks();
            }
            data_sinks::TDataSinkType::NOOP_SINK => {
                let sink_factory = NoopSinkFactory::new();
                let exchange_finst_id = exec_params.map(|params| {
                    (
                        params.fragment_instance_id.hi,
                        params.fragment_instance_id.lo,
                    )
                });
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::SCHEMA_TABLE_SINK => {
                // SCHEMA_TABLE_SINK statements (for example information_schema config updates)
                // only need side effects in FE metadata path; compute fragment output is discarded.
                let sink_factory = NoopSinkFactory::new();
                let exchange_finst_id = exec_params.map(|params| {
                    (
                        params.fragment_instance_id.hi,
                        params.fragment_instance_id.lo,
                    )
                });
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK => {
                let iceberg_sink = sink.iceberg_table_sink.as_ref().ok_or_else(|| {
                    "ICEBERG_TABLE_SINK missing iceberg_table_sink payload".to_string()
                })?;
                let output_exprs = fragment
                    .output_exprs
                    .as_ref()
                    .ok_or_else(|| "ICEBERG_TABLE_SINK missing output_exprs".to_string())?;
                let desc_tbl = desc_tbl
                    .ok_or_else(|| "ICEBERG_TABLE_SINK requires descriptor table".to_string())?;

                let sink_factory = IcebergTableSinkFactory::try_new(
                    iceberg_sink.clone(),
                    output_exprs,
                    &lowered.layout,
                    desc_tbl,
                    last_query_id,
                    fe_addr,
                )?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    None,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::OLAP_TABLE_SINK => {
                let olap_sink = sink
                    .olap_table_sink
                    .as_ref()
                    .ok_or_else(|| "OLAP_TABLE_SINK missing olap_table_sink payload".to_string())?;
                let sink_factory = OlapTableSinkFactory::try_new(
                    olap_sink.clone(),
                    fragment.output_exprs.as_deref(),
                    Some(&exec_plan),
                    Some(&lowered.layout),
                    last_query_id,
                    fe_addr,
                )?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    None,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            other => {
                return Err(format!(
                    "unsupported sink type: {:?}. Only DATA_STREAM_SINK, MULTI_CAST_DATA_STREAM_SINK, SPLIT_DATA_STREAM_SINK, RESULT_SINK, NOOP_SINK, SCHEMA_TABLE_SINK, ICEBERG_TABLE_SINK, and OLAP_TABLE_SINK are supported",
                    other
                ));
            }
        }
        return Ok(FragmentOutput {
            chunks,
            profile_json: None,
        });
    }

    Err("unsupported fragment: missing plan".to_string())
}
