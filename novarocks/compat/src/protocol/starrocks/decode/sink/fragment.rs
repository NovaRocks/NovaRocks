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

use crate::protocol::starrocks::decode::node::Lowered;
use crate::protocol::starrocks::decode::type_lowering::{
    native_primitive_type_from_desc, render_schema_from_type_desc,
};
use crate::protocol::starrocks::decode::{
    FragmentExprArenaOwner, StarRocksExternalDependencyDraft, StarRocksFragmentDecodeError,
    decode_fragment_destination,
};
use crate::thrift::{data_sinks, descriptors, planner};
use novarocks::common::ids::SlotId;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::fragment::program::FragmentSinkSpec;
use novarocks::exec::fragment::sink::{
    DataStreamSinkBranchProgram, DataStreamSinkFactoryInput, DataStreamSinkProgram,
    FragmentSinkProgram, MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    build_change_stream_split_predicate,
};
use novarocks::exec::node::ExecPlan;
use novarocks::protocol::FieldPath;
use novarocks::runtime::endpoint::FragmentDestination;
use novarocks::runtime::fragment::{FragmentSinkAssignment, ResultPresentation, ResultProjection};
use novarocks_types::PrimitiveType;

fn runtime_destination_from_thrift(
    dest: &data_sinks::TPlanFragmentDestination,
    path: FieldPath,
) -> Result<FragmentDestination, StarRocksFragmentDecodeError> {
    decode_fragment_destination(dest, path).map_err(StarRocksFragmentDecodeError::from)
}

fn runtime_destinations_from_thrift(
    destinations: Vec<data_sinks::TPlanFragmentDestination>,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, StarRocksFragmentDecodeError> {
    destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            runtime_destination_from_thrift(destination, path.clone().index(index))
        })
        .collect()
}

fn lower_stream_partition_exprs(
    stream: &data_sinks::TDataStreamSink,
    stream_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<novarocks::exec::expr::ExprId>, StarRocksFragmentDecodeError> {
    let output_partition_path = stream_path.field("output_partition");
    let partition_type =
        crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
            stream.output_partition.type_,
        )
        .map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_enum(
                output_partition_path.clone().field("type"),
                detail,
            )
        })?;
    if !partition_type.requires_exprs() {
        return Ok(Vec::new());
    }
    stream
        .output_partition
        .partition_exprs
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            crate::protocol::starrocks::decode::expr::lower_t_expr_at(
                expr,
                arena,
                layout,
                last_query_id,
                external_dependencies,
                output_partition_path
                    .clone()
                    .field("partition_exprs")
                    .index(idx),
            )
        })
        .collect()
}

fn data_stream_input_from_compat(
    stream: &data_sinks::TDataStreamSink,
    stream_path: FieldPath,
    destinations: Vec<data_sinks::TPlanFragmentDestination>,
    destinations_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<DataStreamSinkFactoryInput, StarRocksFragmentDecodeError> {
    let partition_exprs = lower_stream_partition_exprs(
        stream,
        stream_path.clone(),
        arena,
        layout,
        last_query_id,
        external_dependencies,
    )?;
    let partition_type =
        crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
            stream.output_partition.type_,
        )
        .map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_enum(
                stream_path.clone().field("output_partition").field("type"),
                detail,
            )
        })?;
    DataStreamSinkFactoryInput::try_new(
        stream.dest_node_id,
        partition_type,
        Vec::new(),
        partition_exprs,
        stream.output_columns.clone().unwrap_or_default(),
        runtime_destinations_from_thrift(destinations, destinations_path)?,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(stream_path, detail))
}

pub(crate) fn multi_cast_inputs_from_compat(
    multi_cast: &data_sinks::TMultiCastDataStreamSink,
    multi_cast_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<(DataStreamSinkFactoryInput, Option<i64>)>, StarRocksFragmentDecodeError> {
    if multi_cast.sinks.len() != multi_cast.destinations.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            multi_cast_path.clone().field("destinations"),
            format!(
                "MULTI_CAST_DATA_STREAM_SINK: sinks size {} != destinations size {}",
                multi_cast.sinks.len(),
                multi_cast.destinations.len()
            ),
        ));
    }
    multi_cast
        .sinks
        .iter()
        .zip(multi_cast.destinations.iter())
        .enumerate()
        .map(|(branch_index, (stream, destinations))| {
            Ok((
                data_stream_input_from_compat(
                    stream,
                    multi_cast_path.clone().field("sinks").index(branch_index),
                    destinations.clone(),
                    multi_cast_path
                        .clone()
                        .field("destinations")
                        .index(branch_index),
                    arena,
                    layout,
                    last_query_id,
                    external_dependencies,
                )?,
                stream.limit,
            ))
        })
        .collect()
}

fn split_inputs_from_compat(
    split: &data_sinks::TSplitDataStreamSink,
    split_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<Vec<DataStreamSinkFactoryInput>, StarRocksFragmentDecodeError> {
    let sinks = split.sinks.as_ref().cloned().unwrap_or_default();
    let destinations = split.destinations.as_ref().cloned().unwrap_or_default();
    if sinks.len() != destinations.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            split_path.clone().field("destinations"),
            format!(
                "SPLIT_DATA_STREAM_SINK: sinks size {} != destinations size {}",
                sinks.len(),
                destinations.len()
            ),
        ));
    }
    sinks
        .iter()
        .zip(destinations)
        .enumerate()
        .map(|(branch_index, (stream, destinations))| {
            data_stream_input_from_compat(
                stream,
                split_path.clone().field("sinks").index(branch_index),
                destinations,
                split_path.clone().field("destinations").index(branch_index),
                arena,
                layout,
                last_query_id,
                external_dependencies,
            )
        })
        .collect()
}

struct CompatChangeStreamRouterBranchInput {
    branch_kind: novarocks::exec::change_op::ChangeStreamBranchKind,
    stream_sink: DataStreamSinkFactoryInput,
}

struct CompatChangeStreamRouterInput {
    change_op_slot_id: i32,
    data_route_slot_id: Option<i32>,
    branches: Vec<CompatChangeStreamRouterBranchInput>,
}

fn change_stream_router_input_from_compat(
    router: &data_sinks::TIcebergChangeStreamRouterSink,
    router_path: FieldPath,
    arena: &mut ExprArena,
    layout: &crate::protocol::starrocks::decode::layout::Layout,
    last_query_id: Option<&str>,
    external_dependencies: Option<
        &crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft,
    >,
) -> Result<CompatChangeStreamRouterInput, StarRocksFragmentDecodeError> {
    let branches = router
        .branches
        .iter()
        .enumerate()
        .map(|(branch_index, branch)| {
            let branch_path = router_path.clone().field("branches").index(branch_index);
            let branch_kind = branch_kind_from_thrift(branch.branch_kind).map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_enum(
                    branch_path.clone().field("branch_kind"),
                    detail,
                )
            })?;
            Ok(CompatChangeStreamRouterBranchInput {
                branch_kind,
                stream_sink: data_stream_input_from_compat(
                    &branch.stream_sink,
                    branch_path.clone().field("stream_sink"),
                    branch.destinations.clone(),
                    branch_path.field("destinations"),
                    arena,
                    layout,
                    last_query_id,
                    external_dependencies,
                )?,
            })
        })
        .collect::<Result<Vec<_>, StarRocksFragmentDecodeError>>()?;
    Ok(CompatChangeStreamRouterInput {
        change_op_slot_id: router.change_op_slot_id,
        data_route_slot_id: router.data_route_slot_id,
        branches,
    })
}

pub(crate) struct DecodedStarRocksFragmentSink {
    pub(crate) spec: FragmentSinkSpec,
    pub(crate) assignment: FragmentSinkAssignment,
    pub(crate) result_override: Option<(ResultPresentation, Option<Vec<ResultProjection>>)>,
    pub(crate) root_sink_dop: Option<i32>,
}

fn static_branch_from_factory_input(
    input: DataStreamSinkFactoryInput,
    limit: Option<i64>,
    path: FieldPath,
) -> Result<(DataStreamSinkBranchProgram, Vec<FragmentDestination>), StarRocksFragmentDecodeError> {
    let program = DataStreamSinkBranchProgram::try_new(
        input.dest_node_id,
        input.output_exprs,
        input.output_partition_type,
        input.output_partition_exprs,
        input.output_columns,
        limit,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path, detail))?;
    Ok((program, input.destinations))
}

fn static_stream_from_factory_input(
    input: DataStreamSinkFactoryInput,
    limit: Option<i64>,
    arena: ExprArena,
    path: FieldPath,
) -> Result<(DataStreamSinkProgram, Vec<FragmentDestination>), StarRocksFragmentDecodeError> {
    let program = DataStreamSinkProgram::try_new(
        input.dest_node_id,
        input.output_exprs,
        input.output_partition_type,
        input.output_partition_exprs,
        input.output_columns,
        limit,
        arena,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path, detail))?;
    Ok((program, input.destinations))
}

fn decoded_compat_sink(
    program: FragmentSinkProgram,
    assignment: FragmentSinkAssignment,
    path: FieldPath,
) -> Result<DecodedStarRocksFragmentSink, StarRocksFragmentDecodeError> {
    Ok(DecodedStarRocksFragmentSink {
        spec: FragmentSinkSpec::try_new(program)
            .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path, detail))?,
        assignment,
        result_override: None,
        root_sink_dop: None,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_fragment_sink(
    sink: &data_sinks::TDataSink,
    fragment: &planner::TPlanFragment,
    destinations: &[FragmentDestination],
    sender_id: Option<i32>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    arena: &mut ExprArena,
    lowered: &Lowered,
    last_query_id: Option<&str>,
    session_time_zone: Option<&str>,
    external_dependencies: &StarRocksExternalDependencyDraft,
    compat_iceberg_execution: Option<
        &std::sync::Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
    >,
    compat_connector_writer: Option<&novarocks_spi::connector::ConnectorWriterIdentity>,
    compat_connector_context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    sink_path: FieldPath,
    fragment_path: FieldPath,
) -> Result<DecodedStarRocksFragmentSink, StarRocksFragmentDecodeError> {
    match sink.type_ {
        data_sinks::TDataSinkType::DATA_STREAM_SINK => {
            let stream_path = sink_path.clone().field("stream_sink");
            let stream_sink = sink.stream_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    stream_path.clone(),
                    "DATA_STREAM_SINK missing stream_sink payload",
                )
            })?;
            let mut sink_arena = arena.clone();
            let partition_exprs = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::DataStream,
                || {
                    lower_stream_partition_exprs(
                        stream_sink,
                        stream_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                },
            )?;
            let partition_type =
                crate::protocol::starrocks::decode::sink::decode_data_stream_partition_type(
                    stream_sink.output_partition.type_,
                )
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_enum(
                        stream_path.clone().field("output_partition").field("type"),
                        detail,
                    )
                })?;
            let input = DataStreamSinkFactoryInput::try_new(
                stream_sink.dest_node_id,
                partition_type,
                Vec::new(),
                partition_exprs,
                stream_sink.output_columns.clone().unwrap_or_default(),
                destinations.to_vec(),
            )
            .map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_value(stream_path.clone(), detail)
            })?;
            let (program, destinations) = static_stream_from_factory_input(
                input,
                stream_sink.limit,
                sink_arena,
                stream_path,
            )?;
            decoded_compat_sink(
                FragmentSinkProgram::DataStream(program),
                FragmentSinkAssignment::StreamDestinations {
                    destinations,
                    sender_id,
                },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            let multi_cast_path = sink_path.clone().field("multi_cast_stream_sink");
            let multi_cast = sink.multi_cast_stream_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    multi_cast_path.clone(),
                    "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload",
                )
            })?;
            let mut sink_arena = arena.clone();
            let inputs = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::MultiCastDataStream,
                || {
                    multi_cast_inputs_from_compat(
                        multi_cast,
                        multi_cast_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                },
            )?;
            let (programs, groups): (Vec<_>, Vec<_>) = inputs
                .into_iter()
                .enumerate()
                .map(|(index, (input, limit))| {
                    static_branch_from_factory_input(
                        input,
                        limit,
                        multi_cast_path.clone().field("sinks").index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            let program = MultiCastDataStreamSinkProgram::try_new(programs, sink_arena).map_err(
                |detail| StarRocksFragmentDecodeError::invalid_value(multi_cast_path, detail),
            )?;
            decoded_compat_sink(
                FragmentSinkProgram::MultiCastDataStream(program),
                FragmentSinkAssignment::DestinationGroups { groups, sender_id },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            let split_path = sink_path.clone().field("split_stream_sink");
            let split = sink.split_stream_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    split_path.clone(),
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload",
                )
            })?;
            let split_exprs = split.split_exprs.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    split_path.clone().field("split_exprs"),
                    "SPLIT_DATA_STREAM_SINK missing split_exprs payload",
                )
            })?;
            let mut sink_arena = arena.clone();
            let (split_expr_ids, inputs) = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::SplitDataStream,
                || {
                    let split_expr_ids = split_exprs
                        .iter()
                        .enumerate()
                        .map(|(index, expr)| {
                            crate::protocol::starrocks::decode::expr::lower_t_expr_at(
                                expr,
                                &mut sink_arena,
                                &lowered.layout,
                                last_query_id,
                                Some(external_dependencies),
                                split_path.clone().field("split_exprs").index(index),
                            )
                        })
                        .collect::<Result<Vec<_>, StarRocksFragmentDecodeError>>()?;
                    let inputs = split_inputs_from_compat(
                        split,
                        split_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )?;
                    Ok::<_, StarRocksFragmentDecodeError>((split_expr_ids, inputs))
                },
            )?;
            let (programs, groups): (Vec<_>, Vec<_>) = inputs
                .into_iter()
                .enumerate()
                .map(|(index, input)| {
                    static_branch_from_factory_input(
                        input,
                        None,
                        split_path.clone().field("sinks").index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            let program = SplitDataStreamSinkProgram::try_new(programs, split_expr_ids, sink_arena)
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(split_path, detail)
                })?;
            decoded_compat_sink(
                FragmentSinkProgram::SplitDataStream(program),
                FragmentSinkAssignment::DestinationGroups { groups, sender_id },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK => {
            let router_path = sink_path.clone().field("change_stream_router_sink");
            let router = sink
                .iceberg_change_stream_router_sink
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        router_path.clone(),
                        "CHANGE_STREAM_ROUTER_SINK missing change_stream_router_sink",
                    )
                })?;
            let mut sink_arena = arena.clone();
            let input = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::ChangeStreamRouter,
                || {
                    change_stream_router_input_from_compat(
                        router,
                        router_path.clone(),
                        &mut sink_arena,
                        &lowered.layout,
                        last_query_id,
                        Some(external_dependencies),
                    )
                },
            )?;
            let change_op_slot_id =
                SlotId::try_from(input.change_op_slot_id).map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(
                        router_path.clone().field("change_op_slot_id"),
                        detail,
                    )
                })?;
            let data_route_slot_id = input
                .data_route_slot_id
                .map(SlotId::try_from)
                .transpose()
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(
                        router_path.clone().field("data_route_slot_id"),
                        detail,
                    )
                })?;
            let mut streams = Vec::with_capacity(input.branches.len());
            let mut split_exprs = Vec::with_capacity(input.branches.len());
            let mut groups = Vec::with_capacity(input.branches.len());
            for (index, branch) in input.branches.into_iter().enumerate() {
                let (stream, destinations) = static_branch_from_factory_input(
                    branch.stream_sink,
                    None,
                    router_path
                        .clone()
                        .field("branches")
                        .index(index)
                        .field("stream_sink"),
                )?;
                split_exprs.push(
                    build_change_stream_split_predicate(
                        &mut sink_arena,
                        change_op_slot_id,
                        data_route_slot_id,
                        branch.branch_kind,
                    )
                    .map_err(|detail| {
                        StarRocksFragmentDecodeError::invalid_value(
                            router_path.clone().field("branches").index(index),
                            detail,
                        )
                    })?,
                );
                streams.push(stream);
                groups.push(destinations);
            }
            let program = SplitDataStreamSinkProgram::try_new(streams, split_exprs, sink_arena)
                .map_err(|detail| {
                    StarRocksFragmentDecodeError::invalid_value(router_path, detail)
                })?;
            decoded_compat_sink(
                FragmentSinkProgram::SplitDataStream(program),
                FragmentSinkAssignment::DestinationGroups { groups, sender_id },
                sink_path,
            )
        }
        data_sinks::TDataSinkType::RESULT_SINK => {
            let result_sink_path = sink_path.clone().field("result_sink");
            let result_sink = sink.result_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    result_sink_path.clone(),
                    "RESULT_SINK missing result_sink payload",
                )
            })?;
            let mut decoded = decoded_compat_sink(
                FragmentSinkProgram::Result,
                FragmentSinkAssignment::None,
                sink_path,
            )?;
            decoded.result_override = Some((
                result_sink_config_from_thrift(result_sink, result_sink_path)?,
                result_projections_from_thrift_exprs(
                    fragment.output_exprs.as_ref(),
                    fragment_path.field("output_exprs"),
                )?,
            ));
            Ok(decoded)
        }
        data_sinks::TDataSinkType::NOOP_SINK | data_sinks::TDataSinkType::SCHEMA_TABLE_SINK => {
            decoded_compat_sink(
                FragmentSinkProgram::Noop,
                FragmentSinkAssignment::None,
                sink_path,
            )
        }
        data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
        | data_sinks::TDataSinkType::ICEBERG_DV_SINK
        | data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => {
            let compat_iceberg_execution = compat_iceberg_execution.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    sink_path.clone(),
                    "Iceberg sink requires the startup-composed compat execution binding",
                )
            })?;
            let compat_connector_writer = compat_connector_writer.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    sink_path.clone(),
                    "Iceberg sink requires a batch-frozen connector writer identity",
                )
            })?;
            if compat_connector_writer.binding_key() != compat_iceberg_execution.key() {
                return Err(StarRocksFragmentDecodeError::invalid_value(
                    sink_path.clone(),
                    "compat connector writer owner does not match the exact execution binding",
                ));
            }
            let compat_connector_context = compat_connector_context.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    sink_path.clone(),
                    "Iceberg sink requires a bounded connector writer request context",
                )
            })?;
            let sink_type_name = iceberg_sink_type_name(sink.type_);
            let iceberg_sink_path = sink_path.clone().field("iceberg_table_sink");
            let iceberg_sink = sink.iceberg_table_sink.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    iceberg_sink_path.clone(),
                    format!("{sink_type_name} missing iceberg_table_sink payload"),
                )
            })?;
            let output_exprs = fragment.output_exprs.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    fragment_path.clone().field("output_exprs"),
                    format!("{sink_type_name} missing output_exprs"),
                )
            })?;
            let desc_tbl = desc_tbl.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    FieldPath::root("exec_plan_fragment").field("desc_tbl"),
                    format!("{sink_type_name} requires descriptor table"),
                )
            })?;
            let sink_mode =
                crate::protocol::starrocks::decode::sink::iceberg::iceberg_sink_mode_for_type(
                    sink.type_,
                );
            let input = external_dependencies.with_expr_arena_owner(
                FragmentExprArenaOwner::IcebergTable,
                || crate::protocol::starrocks::decode::sink::iceberg::lower_iceberg_sink_factory_input(
                    iceberg_sink,
                    sink_mode,
                    output_exprs,
                    &lowered.layout,
                    desc_tbl,
                    last_query_id,
                    Some(external_dependencies),
                    iceberg_sink_path.clone(),
                    fragment_path.clone().field("output_exprs"),
                ),
            )
            .map_err(|error| error.into_fragment(iceberg_sink_path.clone()))?;
            let program = novarocks::connector::iceberg::plan_compat_connector_write(
                std::sync::Arc::clone(compat_iceberg_execution),
                compat_connector_writer.clone(),
                input,
                lowered.layout.order.len(),
                compat_connector_context.clone(),
            )
            .map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_value(iceberg_sink_path, detail)
            })?;
            eprintln!(
                "compat_connector_write carrier=common collector=fragment_owned projector=provider_owned"
            );
            let mut decoded = decoded_compat_sink(
                FragmentSinkProgram::ConnectorWrite(program),
                FragmentSinkAssignment::None,
                sink_path,
            )?;
            decoded.root_sink_dop = (sink_mode
                == novarocks::connector::iceberg::IcebergSinkMode::DeletionVectors)
                .then_some(1);
            Ok(decoded)
        }
        data_sinks::TDataSinkType::OLAP_TABLE_SINK => {
            Err(StarRocksFragmentDecodeError::unsupported(
                sink_path.field("type"),
                "OLAP_TABLE_SINK is retired; StarRocks table execution is not part of the fragment kernel",
            ))
        }
        other => Err(StarRocksFragmentDecodeError::unsupported(
            sink_path.field("type"),
            format!(
                "unsupported sink type: {:?}. Only DATA_STREAM_SINK, MULTI_CAST_DATA_STREAM_SINK, SPLIT_DATA_STREAM_SINK, CHANGE_STREAM_ROUTER_SINK, RESULT_SINK, NOOP_SINK, SCHEMA_TABLE_SINK, ICEBERG_TABLE_SINK, ICEBERG_DELETE_SINK, ICEBERG_DV_SINK, and ICEBERG_EQUALITY_DELETE_SINK are supported",
                other
            ),
        )),
    }
}

fn branch_kind_from_thrift(
    value: data_sinks::TIcebergChangeStreamRouterBranchKind,
) -> Result<novarocks::exec::change_op::ChangeStreamBranchKind, String> {
    use novarocks::exec::change_op::ChangeStreamBranchKind;

    match value {
        data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV => {
            Ok(ChangeStreamBranchKind::DeleteDv)
        }
        data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA => {
            Ok(ChangeStreamBranchKind::ReuseData)
        }
        data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA => {
            Ok(ChangeStreamBranchKind::FreshData)
        }
        _ => Err(format!(
            "unsupported Iceberg change-stream router branch kind {}",
            value.0
        )),
    }
}

fn iceberg_sink_type_name(t: data_sinks::TDataSinkType) -> &'static str {
    match t {
        data_sinks::TDataSinkType::ICEBERG_DELETE_SINK => "ICEBERG_DELETE_SINK",
        data_sinks::TDataSinkType::ICEBERG_DV_SINK => "ICEBERG_DV_SINK",
        data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => "ICEBERG_EQUALITY_DELETE_SINK",
        _ => "ICEBERG_TABLE_SINK",
    }
}

fn result_sink_config_from_thrift(
    result_sink: &data_sinks::TResultSink,
    result_sink_path: FieldPath,
) -> Result<ResultPresentation, StarRocksFragmentDecodeError> {
    let sink_type = result_sink
        .type_
        .unwrap_or(data_sinks::TResultSinkType::MYSQL_PROTOCAL);
    match sink_type {
        t if t == data_sinks::TResultSinkType::MYSQL_PROTOCAL => Ok(ResultPresentation::MysqlText),
        t if t == data_sinks::TResultSinkType::HTTP_PROTOCAL => {
            let format = result_sink
                .format
                .unwrap_or(data_sinks::TResultSinkFormatType::JSON);
            if format != data_sinks::TResultSinkFormatType::JSON {
                return Err(StarRocksFragmentDecodeError::invalid_enum(
                    result_sink_path.field("format"),
                    format!(
                        "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                        format
                    ),
                ));
            }
            Ok(ResultPresentation::HttpJson)
        }
        t if t == data_sinks::TResultSinkType::STATISTIC => Ok(ResultPresentation::Statistic),
        other => Err(StarRocksFragmentDecodeError::invalid_enum(
            result_sink_path.field("type"),
            format!("unsupported RESULT_SINK type {:?}", other),
        )),
    }
}

fn result_projection_from_thrift_expr(
    expr: &crate::thrift::exprs::TExpr,
    expr_path: FieldPath,
) -> Result<ResultProjection, StarRocksFragmentDecodeError> {
    let root_path = expr_path.field("nodes").index(0);
    let root = expr.nodes.first().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            root_path.clone(),
            "RESULT_SINK output expression is empty",
        )
    })?;
    if root.node_type != crate::thrift::exprs::TExprNodeType::SLOT_REF {
        return Err(StarRocksFragmentDecodeError::invalid_enum(
            root_path.clone().field("node_type"),
            format!(
                "RESULT_SINK output expression has unsupported node_type {:?} (expected SLOT_REF)",
                root.node_type
            ),
        ));
    }
    let slot = root.slot_ref.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            root_path.clone().field("slot_ref"),
            "RESULT_SINK output expression missing slot_ref payload",
        )
    })?;
    Ok(ResultProjection::new(
        SlotId::try_from(slot.slot_id).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                root_path.clone().field("slot_ref").field("slot_id"),
                detail,
            )
        })?,
        native_primitive_type_from_desc(&root.type_).unwrap_or(PrimitiveType::Invalid),
        render_schema_from_type_desc(&root.type_).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(root_path.field("type"), detail)
        })?,
    ))
}

fn result_projections_from_thrift_exprs(
    output_exprs: Option<&Vec<crate::thrift::exprs::TExpr>>,
    output_exprs_path: FieldPath,
) -> Result<Option<Vec<ResultProjection>>, StarRocksFragmentDecodeError> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    output_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            result_projection_from_thrift_expr(expr, output_exprs_path.clone().index(idx))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}
