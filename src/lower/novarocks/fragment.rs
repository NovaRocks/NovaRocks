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

//! Proto fragment lowering.

use std::sync::Arc;
use std::time::Duration;

use super::expr::lower_proto_expr;
use super::node::{NodeLoweringContext, lower_proto_node};
use crate::common::config::debug_exec_node_output;
use crate::common::types::UniqueId;
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{
    DataStreamSinkFactory, IcebergChangeStreamRouterSinkFactory, IcebergTableSinkFactory,
    MultiCastDataStreamSinkFactory, NoopSinkFactory, ResultBufferSinkFactory,
};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::lower::common::fragment_runtime::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::native_fragment_wire as native_wire;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::result_buffer;
use crate::service::result_batch_wire::ResultSinkConfig;
use crate::{connector, proto};

pub(crate) fn execute_fragment_native(
    fragment: &proto::plan::PlanFragment,
    instance_params: &proto::novarocks::InstanceParams,
    session_time_zone: Option<&str>,
    pipeline_dop: i32,
    _db_name: Option<&str>,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
) -> Result<FragmentOutput, String> {
    let query_options = instance_params
        .query_options
        .as_ref()
        .map(native_wire::query_options_from_native)
        .transpose()?;
    let query_options = apply_query_option_overrides(query_options);
    let query_id = instance_params
        .query_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing query_id".to_string())
        .map(query_id_from_native)?;
    let fragment_instance_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())
        .map(unique_id_from_native)?;
    let runtime_filter_params = instance_params
        .runtime_filter_params
        .as_ref()
        .map(native_wire::runtime_filter_params_from_native)
        .transpose()?;
    let result_buffer_tracker = mem_tracker.clone();
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: query_options.clone(),
            query_id: Some(query_id),
            runtime_filter_params,
            fragment_instance_id: Some(fragment_instance_id),
            backend_num: Some(instance_params.backend_num),
            mem_tracker,
        },
        profiler.as_ref(),
    )?;

    let root = fragment
        .root
        .as_ref()
        .ok_or_else(|| "native PlanFragment missing root".to_string())?;
    let sink = fragment
        .sink
        .as_ref()
        .ok_or_else(|| "native PlanFragment missing sink".to_string())?;

    let mut arena = ExprArena::default();
    let allow_throw_exception = query_options
        .as_ref()
        .map(|opts| opts.allow_throw_exception)
        .unwrap_or(false);
    arena.set_allow_throw_exception(allow_throw_exception);
    arena.set_session_time_zone(session_time_zone.map(str::to_string));

    let ctx = node_context_from_instance_params(
        instance_params,
        query_options.clone(),
        fragment_instance_id,
    )?;
    let lowered = {
        let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
        lower_proto_node(root, &mut arena, &ctx)?
    };

    let mut exec_plan = ExecPlan {
        arena,
        root: lowered.node,
    };
    push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);

    prepare_result_buffer_for_native_sink(
        sink,
        fragment_instance_id,
        instance_params.typed_result_sink,
        result_buffer_tracker.as_ref(),
    )?;
    let exchange_finst_id = Some((fragment_instance_id.hi, fragment_instance_id.lo));
    let sink_factory = sink_factory_from_native(
        fragment,
        sink,
        instance_params,
        instance_params.typed_result_sink,
        &lowered.layout,
    )?;
    let _exec_timer = profiler
        .as_ref()
        .map(|p| p.scoped_timer("PipelineExecuteTime"));
    execute_plan_with_pipeline(
        exec_plan,
        debug_exec_node_output(),
        Duration::from_millis(50),
        sink_factory,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        Some(query_id),
        None,
        Some(instance_params.backend_num),
    )?;

    Ok(FragmentOutput { profile_json: None })
}

fn unique_id_from_native(src: &proto::common::UniqueId) -> UniqueId {
    UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn query_id_from_native(src: &proto::common::UniqueId) -> QueryId {
    QueryId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn node_context_from_instance_params(
    instance_params: &proto::novarocks::InstanceParams,
    query_options: Option<QueryOptions>,
    fragment_instance_id: UniqueId,
) -> Result<NodeLoweringContext, String> {
    let mut ctx = NodeLoweringContext::default()
        .with_connector_registry(Arc::new(connector::ConnectorRegistry::default()))
        .with_query_options(query_options)
        .with_fragment_instance_id(fragment_instance_id.hi, fragment_instance_id.lo);
    for (node_id, ranges) in &instance_params.per_node_scan_ranges {
        ctx = ctx.with_scan_ranges(*node_id, ranges.ranges.clone());
    }
    for (node_id, sender_count) in &instance_params.per_exch_num_senders {
        if *sender_count <= 0 {
            return Err(format!(
                "native InstanceParams per_exch_num_senders node_id={} must be positive, got {}",
                node_id, sender_count
            ));
        }
        ctx = ctx.with_exchange_sender_count(
            crate::runtime::exchange::ExchangeKey {
                finst_id_hi: fragment_instance_id.hi,
                finst_id_lo: fragment_instance_id.lo,
                node_id: *node_id,
            },
            usize::try_from(*sender_count).map_err(|_| {
                format!(
                    "native InstanceParams per_exch_num_senders node_id={} cannot convert {} to usize",
                    node_id, sender_count
                )
            })?,
        );
    }
    Ok(ctx)
}

fn prepare_result_buffer_for_native_sink(
    sink: &proto::plan::DataSink,
    finst_id: UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) -> Result<(), String> {
    let uses_fetch_result_buffer = matches!(
        sink.kind.as_ref(),
        Some(proto::plan::data_sink::Kind::Result(true))
    );
    if !uses_fetch_result_buffer {
        return Ok(());
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
    Ok(())
}

fn sink_factory_from_native(
    fragment: &proto::plan::PlanFragment,
    sink: &proto::plan::DataSink,
    instance_params: &proto::novarocks::InstanceParams,
    typed_result_sink: bool,
    layout: &super::layout::Layout,
) -> Result<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>, String> {
    let kind = sink
        .kind
        .as_ref()
        .ok_or_else(|| "native PlanFragment sink kind missing".to_string())?;
    match kind {
        proto::plan::data_sink::Kind::Result(true) => {
            if !fragment.output_exprs.is_empty() {
                return Err(
                    "native RESULT sink does not support fragment output_exprs yet".to_string(),
                );
            }
            Ok(Box::new(ResultBufferSinkFactory::new(
                None,
                ResultSinkConfig::mysql(),
                None,
                typed_result_sink,
            )))
        }
        proto::plan::data_sink::Kind::Noop(true) => Ok(Box::new(NoopSinkFactory::new())),
        proto::plan::data_sink::Kind::Result(false) => {
            Err("native RESULT sink marker must be true".to_string())
        }
        proto::plan::data_sink::Kind::Noop(false) => {
            Err("native NOOP sink marker must be true".to_string())
        }
        proto::plan::data_sink::Kind::DataStream(stream) => {
            let mut partition_arena = ExprArena::default();
            let partition_exprs = stream
                .output_partition
                .as_ref()
                .ok_or_else(|| "native DATA_STREAM_SINK missing output_partition".to_string())?
                .exprs
                .iter()
                .enumerate()
                .map(|(idx, expr)| {
                    lower_proto_expr(expr, &mut partition_arena, layout).map_err(|err| {
                        format!("native DATA_STREAM_SINK partition expr[{idx}]: {err}")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let stream_sink = native_wire::data_stream_sink_from_native(stream)?;
            let destinations =
                native_wire::destinations_from_native(&instance_params.destinations)?;
            let exec_params = native_wire::exec_params_from_native(instance_params, destinations)?
                .to_compat_exec_params()?;
            let root_plan_node_id = fragment
                .root
                .as_ref()
                .map(|node| node.node_id)
                .unwrap_or(-1);
            Ok(Box::new(
                DataStreamSinkFactory::new_with_pre_lowered_partition(
                    stream_sink,
                    exec_params,
                    root_plan_node_id,
                    partition_arena,
                    partition_exprs,
                    None,
                    None,
                ),
            ))
        }
        proto::plan::data_sink::Kind::MultiCastDataStream(multi_cast) => {
            let pre_lowered_partitions = multi_cast
                .sinks
                .iter()
                .enumerate()
                .map(|(sink_idx, stream)| {
                    let mut partition_arena = ExprArena::default();
                    let partition_exprs = stream
                        .output_partition
                        .as_ref()
                        .ok_or_else(|| {
                            format!(
                                "native MULTI_CAST_DATA_STREAM_SINK sink[{sink_idx}] missing output_partition"
                            )
                        })?
                        .exprs
                        .iter()
                        .enumerate()
                        .map(|(expr_idx, expr)| {
                            lower_proto_expr(expr, &mut partition_arena, layout).map_err(|err| {
                                format!(
                                    "native MULTI_CAST_DATA_STREAM_SINK sink[{sink_idx}] partition expr[{expr_idx}]: {err}"
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((partition_arena, partition_exprs))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let multi_cast_sink = native_wire::multi_cast_data_stream_sink_from_native(multi_cast)?;
            let exec_params = native_wire::exec_params_from_native(instance_params, Vec::new())?
                .to_compat_exec_params()?;
            let root_plan_node_id = fragment
                .root
                .as_ref()
                .map(|node| node.node_id)
                .unwrap_or(-1);
            Ok(Box::new(
                MultiCastDataStreamSinkFactory::new_with_pre_lowered_partitions(
                    multi_cast_sink,
                    exec_params,
                    pre_lowered_partitions,
                    root_plan_node_id,
                    None,
                    None,
                ),
            ))
        }
        proto::plan::data_sink::Kind::IcebergWrite(iceberg) => {
            let (sink_input, _sink_mode) = super::sink::lower_iceberg_write_sink_factory_input(
                iceberg,
                &fragment.output_exprs,
                &fragment.output_columns,
                layout,
            )?;
            Ok(Box::new(IcebergTableSinkFactory::try_new(sink_input)?))
        }
        proto::plan::data_sink::Kind::IcebergChangeStreamRouter(router) => {
            let (router_sink, pre_lowered_partitions) =
                lower_iceberg_change_stream_router_sink_from_native(
                    router,
                    &fragment.output_exprs,
                    &fragment.output_columns,
                    layout,
                )?;
            let exec_params = native_wire::exec_params_from_native(instance_params, Vec::new())?
                .to_compat_exec_params()?;
            let root_plan_node_id = fragment
                .root
                .as_ref()
                .map(|node| node.node_id)
                .unwrap_or(-1);
            Ok(Box::new(
                IcebergChangeStreamRouterSinkFactory::try_new_with_pre_lowered_partitions(
                    router_sink,
                    exec_params,
                    pre_lowered_partitions,
                    root_plan_node_id,
                    None,
                    None,
                )?,
            ))
        }
    }
}

fn lower_iceberg_change_stream_router_sink_from_native(
    router: &proto::plan::IcebergChangeStreamRouterSink,
    output_exprs: &[proto::expr::Expr],
    output_columns: &[proto::common::OutputColumn],
    layout: &super::layout::Layout,
) -> Result<
    (
        native_wire::IcebergChangeStreamRouterSink,
        Vec<(ExprArena, Vec<crate::exec::expr::ExprId>)>,
    ),
    String,
> {
    let change_op_slot_id =
        output_slot_id_for_ordinal(output_columns, router.change_op_output_ordinal, "change_op")?;
    let data_route_slot_id = router
        .data_route_output_ordinal
        .map(|ordinal| output_slot_id_for_ordinal(output_columns, ordinal, "data_route"))
        .transpose()?;
    let mut branches = Vec::with_capacity(router.branches.len());
    let mut pre_lowered_partitions = Vec::with_capacity(router.branches.len());
    for (branch_idx, branch) in router.branches.iter().enumerate() {
        let partition = branch_partition_from_native(branch, output_exprs)?;
        let mut partition_arena = ExprArena::default();
        let partition_exprs = partition
            .exprs
            .iter()
            .enumerate()
            .map(|(expr_idx, expr)| {
                lower_proto_expr(expr, &mut partition_arena, layout).map_err(|err| {
                    format!(
                        "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{branch_idx}] partition expr[{expr_idx}]: {err}"
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output_columns = branch
            .output_ordinals
            .iter()
            .map(|ordinal| {
                output_slot_id_for_ordinal(
                    output_columns,
                    *ordinal,
                    &format!("branch[{branch_idx}] output"),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stream_sink = native_wire::DataStreamSink::new(
            branch.target_exchange_node_id,
            native_wire::data_partition_without_exprs(&partition)?,
            None::<bool>,
            None::<bool>,
            None::<i32>,
            Some(output_columns),
            None::<i64>,
        );
        let destinations = branch
            .destinations
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{branch_idx}] missing destinations"
                )
            })
            .and_then(native_wire::stream_destinations_from_native)?;
        let branch_destinations = destinations
            .into_iter()
            .map(crate::runtime::fragment_exec_params::compat_destination_from_runtime)
            .collect();
        branches.push(native_wire::IcebergChangeStreamRouterBranch::new(
            branch.branch_id,
            native_wire::iceberg_change_stream_branch_kind_from_native(branch.branch_kind)?,
            stream_sink,
            branch_destinations,
        ));
        pre_lowered_partitions.push((partition_arena, partition_exprs));
    }
    Ok((
        native_wire::IcebergChangeStreamRouterSink::new(
            change_op_slot_id,
            data_route_slot_id,
            branches,
        ),
        pre_lowered_partitions,
    ))
}

fn branch_partition_from_native(
    branch: &proto::plan::IcebergChangeStreamBranchRoute,
    output_exprs: &[proto::expr::Expr],
) -> Result<proto::plan::DataPartition, String> {
    if let Some(partition) = branch.output_partition.as_ref() {
        return Ok(partition.clone());
    }
    let exprs = branch
        .output_partition_ordinals
        .iter()
        .map(|ordinal| {
            let idx = usize::try_from(*ordinal).map_err(|_| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} overflows usize"
                )
            })?;
            output_exprs.get(idx).cloned().ok_or_else(|| {
                format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} is out of range"
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let kind = if exprs.is_empty() {
        proto::plan::PartitionKind::Unpartitioned
    } else {
        proto::plan::PartitionKind::Hash
    };
    Ok(proto::plan::DataPartition {
        kind: kind as i32,
        exprs,
    })
}

fn output_slot_id_for_ordinal(
    output_columns: &[proto::common::OutputColumn],
    ordinal: u64,
    label: &str,
) -> Result<i32, String> {
    let idx = usize::try_from(ordinal)
        .map_err(|_| format!("native router {label} output ordinal {ordinal} overflows usize"))?;
    let column = output_columns
        .get(idx)
        .ok_or_else(|| format!("native router {label} output ordinal {ordinal} is out of range"))?;
    i32::try_from(column.column_id).map_err(|_| {
        format!(
            "native router {label} output ordinal {ordinal} column id {} exceeds i32",
            column.column_id
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{common, plan};

    fn int_output_column(id: u32, name: &str) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: name.to_string(),
            r#type: Some(common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: common::PrimitiveType::Int as i32,
                    ..Default::default()
                })),
            }),
            nullable: false,
            is_internal: false,
        }
    }

    fn noop_values_fragment() -> plan::PlanFragment {
        let columns = vec![int_output_column(1, "v")];
        plan::PlanFragment {
            fragment_id: 1,
            root: Some(plan::DistributedNode {
                node_id: 10,
                fragment_id: 1,
                limit: -1,
                payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                    output_columns: columns.clone(),
                    kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                        rows: Vec::new(),
                        columns: columns.clone(),
                    })),
                })),
                ..Default::default()
            }),
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::Noop(true)),
            }),
            output_columns: columns,
            ..Default::default()
        }
    }

    fn instance_params() -> proto::novarocks::InstanceParams {
        proto::novarocks::InstanceParams {
            query_id: Some(common::UniqueId { hi: 11, lo: 12 }),
            fragment_instance_id: Some(common::UniqueId { hi: 21, lo: 22 }),
            backend_num: 1,
            query_options: Some(proto::novarocks::QueryOptions {
                batch_size: 1024,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn converts_native_query_options_consumed_subset() {
        let opts = native_wire::query_options_from_native(&proto::novarocks::QueryOptions {
            batch_size: 8192,
            enable_profile: true,
            query_mem_limit: 1 << 20,
            connector_io_tasks_per_scan_operator: 7,
            runtime_filter_wait_timeout_ms: Some(123),
            allow_throw_exception: true,
            enable_spill: true,
            spill_options: Some(proto::novarocks::SpillOptions {
                spill_mode: native_wire::SpillMode::FORCE.0,
                spill_mem_limit_threshold: 0.5,
                spill_operator_min_bytes: 1024,
                spill_mem_table_size: 32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("query options");

        assert_eq!(opts.batch_size, Some(8192));
        assert!(opts.enable_profile);
        assert_eq!(opts.exec_mem_limit, Some(1 << 20));
        assert_eq!(opts.connector_io_tasks_per_scan_operator, Some(7));
        assert_eq!(opts.runtime_filter_wait_timeout_ms, Some(123));
        assert!(opts.allow_throw_exception);
        let spill = opts.spill.expect("spill options");
        assert_eq!(spill.spill_mode, crate::exec::spill::SpillMode::Force);
        assert_eq!(spill.spill_mem_limit_threshold, Some(0.5));
        assert_eq!(spill.spill_operator_min_bytes, Some(1024));
        assert_eq!(spill.spill_mem_table_size, Some(32));
    }

    #[test]
    fn rejects_native_spill_without_spill_options() {
        let err = native_wire::query_options_from_native(&proto::novarocks::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required");

        assert!(err.contains("spill_options"), "{err}");
    }

    #[test]
    fn converts_runtime_filter_params_and_addresses() {
        let rf = native_wire::runtime_filter_params_from_native(
            &proto::novarocks::RuntimeFilterParams {
                id_to_prober_params: [(
                    3,
                    proto::novarocks::ProberParamsList {
                        params: vec![proto::novarocks::ProberParams {
                            fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
                            endpoint: "127.0.0.1:9050".to_string(),
                        }],
                    },
                )]
                .into_iter()
                .collect(),
                runtime_filter_builder_number: [(3, 2)].into_iter().collect(),
                runtime_filter_max_size: 4096,
            },
        )
        .expect("runtime filter params");

        assert_eq!(rf.runtime_filter_max_size(), Some(4096));
        assert_eq!(rf.runtime_filter_builder_number().get(&3), Some(&2));
        let prober = &rf.id_to_prober_params()[&3][0];
        assert_eq!(
            prober.fragment_instance_id(),
            &crate::thrift::types::TUniqueId::new(1, 2)
        );
        assert_eq!(prober.endpoint().as_host_port(), "127.0.0.1:9050");
    }

    #[test]
    fn rejects_native_fragment_without_query_id() {
        let fragment = noop_values_fragment();
        let mut params = instance_params();
        params.query_id = None;

        let err = execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect_err("query_id is required");
        assert!(err.contains("query_id"), "{err}");
    }

    #[test]
    fn rejects_native_fragment_without_fragment_instance_id() {
        let fragment = noop_values_fragment();
        let mut params = instance_params();
        params.fragment_instance_id = None;

        let err = execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect_err("fragment_instance_id is required");
        assert!(err.contains("fragment_instance_id"), "{err}");
    }

    #[test]
    fn rejects_nonpositive_exchange_sender_count() {
        let fragment = noop_values_fragment();
        let mut params = instance_params();
        params.per_exch_num_senders.insert(30, 0);

        let err = execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect_err("sender count must be positive");
        assert!(err.contains("must be positive"), "{err}");
    }

    #[test]
    fn executes_native_noop_values_fragment() {
        let fragment = noop_values_fragment();
        let params = instance_params();

        execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect("native noop fragment executes");
    }
}
