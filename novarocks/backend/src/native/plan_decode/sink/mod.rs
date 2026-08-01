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

//! Native proto sink lowering.

mod equality_delete;
mod metadata;
mod partition;
mod position_delete;

use std::collections::HashMap;
use std::sync::Arc;

use crate::native::type_decode::decode_type;
use novarocks::common::ids::SlotId;
use novarocks::connector::iceberg::position_delete_descriptor::PositionDeleteExpectedBinding;
use novarocks::connector::iceberg::schema::build_full_output_schema;
use novarocks::connector::iceberg::sink_plan::{
    DeferredPositionDeleteDataFilePartitionIndex, IcebergSinkFactoryInput, IcebergSinkMode,
    IcebergSinkPlan,
};
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::exec::fragment::sink::{
    DataStreamSinkBranchProgram, FragmentSinkProgram, IcebergChangeStreamRouterBranchProgram,
    IcebergChangeStreamRouterProgram, IcebergTableSinkProgram, MultiCastDataStreamSinkProgram,
};
use novarocks::exec::operators::DataStreamPartitionType;
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};
use novarocks::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks::runtime::fragment::instance::FragmentSinkAssignment;
use novarocks_protocol::novarocks as native_proto;
use novarocks_protocol::{common, expr, plan};

use super::context::NativePlanDecodeContext;
use super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use super::layout::Layout;

use self::equality_delete::{
    build_equality_delete_output_schema, validate_equality_delete_unpartitioned_target_metadata,
};
use self::metadata::{
    iceberg_table_descriptor_from_native, iceberg_table_location, map_native_compression,
    parse_target_table_metadata, resolve_native_sink_s3_config,
    schema_has_reserved_row_lineage_columns, validate_iceberg_sink_file_format,
};
use self::partition::{
    build_partition_exprs_from_output_exprs, partition_info_from_metadata,
    partition_source_field_ids_from_metadata,
};
use self::position_delete::bind_position_delete_descriptor_from_native;

pub(crate) fn decode_fragment_sink_program(
    fragment: &plan::PlanFragment,
    layout: &Layout,
) -> Result<FragmentSinkProgram, NativeFragmentDecodeError> {
    decode_fragment_sink_program_with_context(fragment, layout, None)
}

pub(crate) fn decode_fragment_sink_program_with_context(
    fragment: &plan::PlanFragment,
    layout: &Layout,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<FragmentSinkProgram, NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let sink = fragment.sink.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "native PlanFragment requires sink")
    })?;
    let kind = sink.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::Result(true) => {
            if !fragment.output_exprs.is_empty() {
                return Err(NativeFragmentDecodeError::unsupported(
                    path.field("result"),
                    "native RESULT sink does not support fragment output_exprs yet",
                ));
            }
            Ok(FragmentSinkProgram::Result)
        }
        plan::data_sink::Kind::Noop(true) => Ok(FragmentSinkProgram::Noop),
        plan::data_sink::Kind::Result(false) => Err(NativeFragmentDecodeError::invalid_value(
            path.field("result"),
            "native RESULT sink marker must be true",
        )),
        plan::data_sink::Kind::Noop(false) => Err(NativeFragmentDecodeError::invalid_value(
            path.field("noop"),
            "native NOOP sink marker must be true",
        )),
        plan::data_sink::Kind::DataStream(stream) => {
            let mut partition_arena = ExprArena::default();
            let branch = decode_data_stream_branch(
                stream,
                &mut partition_arena,
                layout,
                "native DATA_STREAM_SINK",
                ctx,
            )
            .map_err(|error| error.into_native(path.clone().field("data_stream")))?;
            branch
                .into_program(partition_arena)
                .map(FragmentSinkProgram::DataStream)
                .map_err(NativeFragmentDecodeError::from)
        }
        plan::data_sink::Kind::MultiCastDataStream(grouped) => {
            let mut partition_arena = ExprArena::default();
            let mut sinks = Vec::with_capacity(grouped.sinks.len());
            for (index, stream) in grouped.sinks.iter().enumerate() {
                sinks.push(
                    decode_data_stream_branch(
                        stream,
                        &mut partition_arena,
                        layout,
                        &format!("native MULTI_CAST_DATA_STREAM_SINK sink[{index}]"),
                        ctx,
                    )
                    .map_err(|error| {
                        error.into_native(
                            path.clone()
                                .field("multi_cast_data_stream")
                                .field("sinks")
                                .index(index),
                        )
                    })?,
                );
            }
            MultiCastDataStreamSinkProgram::try_new(sinks, partition_arena)
                .map(FragmentSinkProgram::MultiCastDataStream)
                .map_err(NativeFragmentDecodeError::from)
        }
        plan::data_sink::Kind::IcebergWrite(iceberg) => {
            let (input, _mode) = decode_iceberg_write_sink_factory_input_with_context(
                iceberg,
                &fragment.output_exprs,
                &fragment.output_columns,
                layout,
                ctx,
            )
            .map_err(|error| error.into_native(path.clone().field("iceberg_write")))?;
            IcebergTableSinkProgram::try_from_factory_input(input)
                .map(FragmentSinkProgram::IcebergTable)
                .map_err(NativeFragmentDecodeError::from)
        }
        plan::data_sink::Kind::IcebergChangeStreamRouter(router) => {
            decode_change_stream_router_program_with_context(
                router,
                &fragment.output_exprs,
                &fragment.output_columns,
                layout,
                ctx,
            )
            .map(FragmentSinkProgram::IcebergChangeStreamRouter)
            .map_err(|error| error.into_native(path.field("iceberg_change_stream_router")))
        }
    }
}

pub(crate) fn decode_fragment_sink_assignment(
    sink: &plan::DataSink,
    instance: &native_proto::InstanceParams,
) -> Result<FragmentSinkAssignment, NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let kind = sink.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::DataStream(_) => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: decode_instance_destinations(&instance.destinations)?,
            sender_id: None,
        }),
        plan::data_sink::Kind::MultiCastDataStream(grouped) => {
            let groups = grouped
                .destinations
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    decode_stream_destination_list(
                        group,
                        path.clone()
                            .field("multi_cast_data_stream")
                            .field("destinations")
                            .index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::IcebergChangeStreamRouter(router) => {
            let groups = router
                .branches
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let group_path = path
                        .clone()
                        .field("iceberg_change_stream_router")
                        .field("branches")
                        .index(index)
                        .field("destinations");
                    let group = branch.destinations.as_ref().ok_or_else(|| {
                        NativeFragmentDecodeError::missing(
                            group_path.clone(),
                            "native change-stream branch requires destinations",
                        )
                    })?;
                    decode_stream_destination_list(group, group_path)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::Result(_)
        | plan::data_sink::Kind::Noop(_)
        | plan::data_sink::Kind::IcebergWrite(_) => {
            if instance.destinations.is_empty() {
                Ok(FragmentSinkAssignment::None)
            } else {
                Ok(FragmentSinkAssignment::StreamDestinations {
                    destinations: decode_instance_destinations(&instance.destinations)?,
                    sender_id: None,
                })
            }
        }
    }
}

fn decode_data_stream_branch(
    stream: &plan::DataStreamSink,
    partition_arena: &mut ExprArena,
    layout: &Layout,
    context: &str,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<DataStreamSinkBranchProgram, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<DataStreamSinkBranchProgram, NativeFragmentLeafDecodeError> {
        let partition = stream.output_partition.as_ref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "output_partition",
                format!("{context} missing output_partition"),
            )
        })?;
        let partition_type = decode_stream_partition_type(partition.kind).map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidEnum, "kind", error)
                .prepend_field("output_partition")
        })?;
        let output_partition_exprs = if partition_type.requires_exprs() {
            partition
                .exprs
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    decode_expr_with_context(expression, partition_arena, layout, ctx).map_err(
                        |error| {
                            NativeFragmentLeafDecodeError::at_field(
                                ProtocolErrorKind::InvalidValue,
                                "expr",
                                error,
                            )
                            .prepend_index(index)
                            .prepend_field("exprs")
                            .prepend_field("output_partition")
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let output_columns = decode_output_slot_ids(&stream.output_columns, context)?;
        DataStreamSinkBranchProgram::try_new(
            stream.dest_node_id,
            Vec::new(),
            partition_type,
            output_partition_exprs,
            output_columns,
            stream.limit,
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "output_partition",
                error,
            )
        })
    })();
    decoded
}

fn decode_output_slot_ids(
    raw_ids: &[i32],
    context: &str,
) -> Result<Vec<SlotId>, NativeFragmentLeafDecodeError> {
    let mut seen = std::collections::HashSet::new();
    raw_ids
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            let slot_id = SlotId::try_from(*raw).map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "output_columns",
                    format!("{context}: invalid output_columns slot id: {error}"),
                )
                .append_index(index)
            })?;
            if !seen.insert(slot_id) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "output_columns",
                    format!("{context}: duplicate output_columns slot id: {slot_id}"),
                )
                .append_index(index));
            }
            Ok(slot_id)
        })
        .collect()
}

fn decode_change_stream_router_program(
    router: &plan::IcebergChangeStreamRouterSink,
    output_exprs: &[expr::Expr],
    output_columns: &[common::OutputColumn],
    layout: &Layout,
) -> Result<IcebergChangeStreamRouterProgram, NativeFragmentLeafDecodeError> {
    decode_change_stream_router_program_with_context(
        router,
        output_exprs,
        output_columns,
        layout,
        None,
    )
}

fn decode_change_stream_router_program_with_context(
    router: &plan::IcebergChangeStreamRouterSink,
    output_exprs: &[expr::Expr],
    output_columns: &[common::OutputColumn],
    layout: &Layout,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<IcebergChangeStreamRouterProgram, NativeFragmentLeafDecodeError> {
    let decoded =
        (|| -> Result<IcebergChangeStreamRouterProgram, NativeFragmentLeafDecodeError> {
            let change_op_slot_id = SlotId::try_from(output_slot_id_for_ordinal(
                output_columns,
                router.change_op_output_ordinal,
                "change_op_output_ordinal",
            )?)
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "change_op_output_ordinal",
                    error,
                )
            })?;
            let data_route_slot_id = router
                .data_route_output_ordinal
                .map(|ordinal| {
                    output_slot_id_for_ordinal(output_columns, ordinal, "data_route_output_ordinal")
                })
                .transpose()?
                .map(SlotId::try_from)
                .transpose()
                .map_err(|error| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidValue,
                        "data_route_output_ordinal",
                        error,
                    )
                })?;
            let mut partition_arena = ExprArena::default();
            let branches = router
        .branches
        .iter()
        .enumerate()
        .map(|(index, branch)| {
            let branch_path = |error: NativeFragmentLeafDecodeError| error.prepend_index(index).prepend_field("branches");
            let partition = branch_partition_from_native(branch, output_exprs).map_err(branch_path)?;
            let partition_type = decode_stream_partition_type(partition.kind).map_err(|error| branch_path(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidEnum, "kind", error).prepend_field("output_partition")))?;
            let output_partition_exprs = if partition_type.requires_exprs() {
                partition
                    .exprs
                    .iter()
                    .enumerate()
                    .map(|(expr_index, expression)| {
                        decode_expr_with_context(expression, &mut partition_arena, layout, ctx).map_err(|error| {
                            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "exprs", format!(
                                "native ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{index}] partition expr[{expr_index}]: {error}"
                            )).append_index(expr_index).prepend_field("output_partition").prepend_index(index).prepend_field("branches")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
            let branch_output_columns = decode_router_output_slots(
                &branch.output_ordinals,
                output_columns,
                "output_ordinals",
            ).map_err(branch_path)?;
            Ok(IcebergChangeStreamRouterBranchProgram::new(
                branch.branch_id,
                decode_change_stream_branch_kind(branch.branch_kind).map_err(|error| branch_path(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidEnum, "branch_kind", error)))?,
                DataStreamSinkBranchProgram::try_new(
                    branch.target_exchange_node_id,
                    Vec::new(),
                    partition_type,
                    output_partition_exprs,
                    branch_output_columns,
                    None,
                )
                .map_err(|error| branch_path(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "output_partition", error)))?,
            ))
        })
        .collect::<Result<Vec<_>, NativeFragmentLeafDecodeError>>()?;
            IcebergChangeStreamRouterProgram::try_new(
                change_op_slot_id,
                data_route_slot_id,
                branches,
                partition_arena,
            )
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "branches",
                    error,
                )
            })
        })();
    decoded
}

fn branch_partition_from_native(
    branch: &plan::IcebergChangeStreamBranchRoute,
    output_exprs: &[expr::Expr],
) -> Result<plan::DataPartition, NativeFragmentLeafDecodeError> {
    if let Some(partition) = branch.output_partition.as_ref() {
        return Ok(partition.clone());
    }
    let exprs = branch
        .output_partition_ordinals
        .iter()
        .enumerate()
        .map(|(ordinal_index, ordinal)| {
            let output_index = usize::try_from(*ordinal).map_err(|_| {
                NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "output_partition_ordinals", format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} overflows usize"
                )).append_index(ordinal_index)
            })?;
            output_exprs.get(output_index).cloned().ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "output_partition_ordinals", format!(
                    "native ICEBERG_CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} is out of range"
                )).append_index(ordinal_index)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let kind = if exprs.is_empty() {
        plan::PartitionKind::Unpartitioned
    } else {
        plan::PartitionKind::Hash
    };
    Ok(plan::DataPartition {
        kind: kind as i32,
        exprs,
    })
}

fn output_slot_id_for_ordinal(
    output_columns: &[common::OutputColumn],
    ordinal: u64,
    field: &'static str,
) -> Result<i32, NativeFragmentLeafDecodeError> {
    let index = usize::try_from(ordinal).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("native router output ordinal {ordinal} overflows usize"),
        )
    })?;
    let column = output_columns.get(index).ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("native router output ordinal {ordinal} is out of range"),
        )
    })?;
    i32::try_from(column.column_id).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!(
                "native router output ordinal {ordinal} column id {} exceeds i32",
                column.column_id
            ),
        )
    })
}

fn decode_router_output_slots(
    ordinals: &[u64],
    output_columns: &[common::OutputColumn],
    field: &'static str,
) -> Result<Vec<SlotId>, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<Vec<SlotId>, NativeFragmentLeafDecodeError> {
        let mut seen = std::collections::HashSet::new();
        ordinals
        .iter()
        .enumerate().map(|(index, ordinal)| {
            let raw_slot_id = output_slot_id_for_ordinal(output_columns, *ordinal, field)
                .map_err(|error| error.append_index(index))?;
            let slot_id = SlotId::try_from(raw_slot_id).map_err(|error| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, field, error).append_index(index))?;
            if !seen.insert(slot_id) {
                return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InconsistentFields, field, format!("native ICEBERG_CHANGE_STREAM_ROUTER_SINK duplicate output slot id: {slot_id}")).append_index(index));
            }
            Ok(slot_id)
        })
        .collect()
    })();
    decoded
}

fn decode_change_stream_branch_kind(
    value: i32,
) -> Result<novarocks::sql::common::ChangeStreamBranchKind, String> {
    match plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(novarocks::sql::common::ChangeStreamBranchKind::DeleteDv)
        }
        plan::ChangeStreamBranchKind::ReuseData => {
            Ok(novarocks::sql::common::ChangeStreamBranchKind::ReuseData)
        }
        plan::ChangeStreamBranchKind::FreshData => {
            Ok(novarocks::sql::common::ChangeStreamBranchKind::FreshData)
        }
        plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}

fn decode_stream_destination_list(
    group: &plan::StreamDestinationList,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, NativeFragmentDecodeError> {
    group
        .destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let destination_path = path.clone().field("destinations").index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    destination_path.clone().field("finst_id"),
                    "native stream destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                novarocks::common::types::UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                },
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        destination_path.field("endpoint"),
                        error,
                    )
                })?,
            ))
        })
        .collect()
}

fn decode_instance_destinations(
    destinations: &[native_proto::Destination],
) -> Result<Vec<FragmentDestination>, NativeFragmentDecodeError> {
    destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let destination_path = FieldPath::root("instance_params")
                .field("destinations")
                .index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    destination_path.clone().field("finst_id"),
                    "native Destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                novarocks::common::types::UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                },
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        destination_path.field("endpoint"),
                        error,
                    )
                })?,
            ))
        })
        .collect()
}

pub(crate) fn decode_iceberg_write_sink_factory_input(
    sink: &plan::IcebergWriteFragmentSink,
    fragment_output_exprs: &[expr::Expr],
    fragment_output_columns: &[common::OutputColumn],
    layout: &Layout,
) -> Result<(IcebergSinkFactoryInput, IcebergSinkMode), NativeFragmentLeafDecodeError> {
    decode_iceberg_write_sink_factory_input_with_context(
        sink,
        fragment_output_exprs,
        fragment_output_columns,
        layout,
        None,
    )
}

fn decode_iceberg_write_sink_factory_input_with_context(
    sink: &plan::IcebergWriteFragmentSink,
    fragment_output_exprs: &[expr::Expr],
    fragment_output_columns: &[common::OutputColumn],
    layout: &Layout,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<(IcebergSinkFactoryInput, IcebergSinkMode), NativeFragmentLeafDecodeError> {
    let decoded =
        (|| -> Result<(IcebergSinkFactoryInput, IcebergSinkMode), NativeFragmentLeafDecodeError> {
        let spec = sink
            .spec
            .as_ref()
            .ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::MissingField,
                    "spec",
                    "native Iceberg write sink missing spec",
                )
            })?;
        let mode = iceberg_sink_mode_from_native(spec.mode)
            .map_err(|error| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidEnum, "mode", error).prepend_field("spec"))?;

        let mut arena = ExprArena::default();
        let lowered_output_exprs = if fragment_output_exprs.is_empty() {
            lower_output_columns_as_slot_refs(
                fragment_output_columns,
                sink.input.as_ref(),
                &mut arena,
            )?
        } else {
            lower_output_exprs(fragment_output_exprs, &mut arena, layout, ctx)?
        };

        let target_table = spec
            .target_table
            .as_ref()
            .ok_or_else(|| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "target_table", "native Iceberg write sink missing target_table").prepend_field("spec"))?;
        let writer_columns = if spec.target_columns.is_empty() {
            target_table.columns.as_slice()
        } else {
            spec.target_columns.as_slice()
        };
        if lowered_output_exprs.len() != writer_columns.len() {
            return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InconsistentFields, "target_columns", format!(
                "native Iceberg write sink input column count {} does not match target column count {}",
                lowered_output_exprs.len(),
                writer_columns.len()
            )).prepend_field("spec"));
        }

        let iceberg_table = spec
            .iceberg
            .as_ref()
            .ok_or_else(|| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "iceberg", "native Iceberg write sink missing iceberg table info").prepend_field("spec"))?;
        let iceberg_table =
            iceberg_table_descriptor_from_native(iceberg_table, &target_table.columns, mode)
                .map_err(|error| error.prepend_field("spec"))?;
        let target_partition_spec_id = spec.target_partition_spec_id;

        let target_table_metadata = parse_target_table_metadata(&iceberg_table, mode)
            .map_err(|error| error.prepend_field("iceberg").prepend_field("spec"))?;
        let (partition_source_column_names, partition_column_names, transform_exprs) =
            partition_info_from_metadata(target_table_metadata.as_ref(), target_partition_spec_id)
                .map_err(|error| error.prepend_field("spec"))?;
        let position_delete_binding = if matches!(
            mode,
            IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
        ) {
            let target_schema = build_full_output_schema(&iceberg_table)
                .map_err(|error| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "iceberg", error).prepend_field("spec"))?;
            let metadata = target_table_metadata.as_ref().ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "serialized_metadata", format!(
                    "native Iceberg {:?} sink requires serialized target table metadata",
                    mode
                )).prepend_field("iceberg").prepend_field("spec")
            })?;
            let partition_source_field_ids =
                partition_source_field_ids_from_metadata(metadata, &partition_source_column_names)
                    .map_err(|error| error.prepend_field("spec"))?;
            let expected = PositionDeleteExpectedBinding {
                target_partition_spec_id,
                partition_source_column_names: partition_source_column_names.clone(),
                partition_column_names: partition_column_names.clone(),
                partition_transform_exprs: transform_exprs.clone(),
                partition_source_field_ids,
                output_expr_count: lowered_output_exprs.len(),
            };
            let binding = bind_position_delete_descriptor_from_native(
                spec.position_delete_output_descriptor.as_ref(),
                expected,
            )
            .map_err(|error| {
                error.prepend_field("spec")
            })?;
            Some((target_schema, binding))
        } else {
            None
        };

        let (output_schema, target_schema, equality_delete_columns) = match mode {
            IcebergSinkMode::Data => {
                let target_schema = build_full_output_schema(&iceberg_table)
                    .map_err(|error| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "iceberg", error).prepend_field("spec"))?;
                if lowered_output_exprs.len() != target_schema.fields().len() {
                    return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InconsistentFields, "target_columns", format!(
                        "native Iceberg sink output expr count mismatch: exprs={} columns={}",
                        lowered_output_exprs.len(),
                        target_schema.fields().len()
                    )).prepend_field("spec"));
                }
                (Arc::clone(&target_schema), target_schema, Vec::new())
            }
            IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors => {
                let (target_schema, binding) = position_delete_binding
                    .as_ref()
                    .expect("position delete binding must exist for delete-like sink");
                (
                    Arc::clone(&binding.output_schema),
                    Arc::clone(target_schema),
                    Vec::new(),
                )
            }
            IcebergSinkMode::EqualityDeletes => {
                validate_equality_delete_unpartitioned_target_metadata(
                    &iceberg_table,
                    target_partition_spec_id,
                ).map_err(|error| error.prepend_field("spec"))?;
                let (schema, columns) = build_equality_delete_output_schema(&iceberg_table)
                    .map_err(|error| error.prepend_field("spec"))?;
                if lowered_output_exprs.len() != schema.fields().len() {
                    return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InconsistentFields, "target_columns", format!(
                        "native Iceberg equality-delete sink expects {} output exprs; got {}",
                        schema.fields().len(),
                        lowered_output_exprs.len()
                    )).prepend_field("spec"));
                }
                (Arc::clone(&schema), schema, columns)
            }
        };
        let partition_exprs = if mode == IcebergSinkMode::Data {
            build_partition_exprs_from_output_exprs(
                &partition_source_column_names,
                &transform_exprs,
                writer_columns,
                &lowered_output_exprs,
                &mut arena,
            ).map_err(|error| error.prepend_field("spec"))?
        } else {
            Vec::new()
        };

        let row_lineage_data = mode == IcebergSinkMode::Data
            && schema_has_reserved_row_lineage_columns(&target_schema)
                .map_err(|error| NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "target_table", error).prepend_field("spec"))?;
        let table_location = if spec.table_location.is_empty() {
            iceberg_table_location(iceberg_table.serialized_metadata.as_deref()).unwrap_or_else(
                || {
                    spec.iceberg
                        .as_ref()
                        .map(|t| t.location.clone())
                        .unwrap_or_default()
                },
            )
        } else {
            spec.table_location.clone()
        };
        if table_location.is_empty() {
            return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "table_location", "native Iceberg write sink missing table location").prepend_field("spec"));
        }
        let data_location = if spec.data_location.is_empty() {
            format!("{}/data", table_location.trim_end_matches('/'))
        } else {
            spec.data_location.clone()
        };
        let object_store_s3 =
            resolve_native_sink_s3_config(&data_location, &spec.cloud_properties)
                .map_err(|error| error.prepend_field("spec"))?;
        let target_snapshot_id = iceberg_table.current_snapshot_id;
        let position_delete_data_file_partition_index_input = if matches!(
            mode,
            IcebergSinkMode::PositionDeletes | IcebergSinkMode::DeletionVectors
        ) {
            let metadata = target_table_metadata.as_ref().ok_or_else(|| NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "serialized_metadata",
                "native Iceberg delete sink missing target table metadata",
            ).prepend_field("iceberg").prepend_field("spec"))?;
            Some(DeferredPositionDeleteDataFilePartitionIndex::new(
                metadata.clone(),
                target_snapshot_id,
                table_location.clone(),
                object_store_s3.clone(),
            ))
        } else {
            None
        };
        let (file_format, report_file_format) =
            validate_iceberg_sink_file_format(&spec.file_format)
                .map_err(|error| error.prepend_field("spec"))?;
        let compression = map_native_compression(spec.compression)
            .map_err(|error| error.prepend_field("spec"))?;

        let plan = IcebergSinkPlan {
            mode,
            table_location,
            data_location,
            target_partition_spec_id,
            target_table_metadata,
            target_snapshot_id,
            position_delete_data_file_partitions: HashMap::new(),
            position_delete_data_file_partition_index_input,
            object_store_s3,
            file_format,
            report_file_format,
            compression,
            output_schema,
            target_schema,
            equality_delete_columns,
            row_lineage_data,
            output_exprs: lowered_output_exprs,
            partition_exprs,
            partition_source_column_names,
            partition_column_names,
            transform_exprs,
            position_delete_binding: position_delete_binding.map(|(_, binding)| binding),
        };

        Ok((
            IcebergSinkFactoryInput {
                name: "ICEBERG_TABLE_SINK".to_string(),
                arena,
                plan,
            },
            mode,
        ))
    })();
    decoded
}

fn decode_stream_partition_type(kind: i32) -> Result<DataStreamPartitionType, String> {
    match plan::PartitionKind::try_from(kind)
        .map_err(|_| format!("unknown native PartitionKind value {kind}"))?
    {
        plan::PartitionKind::Unpartitioned => Ok(DataStreamPartitionType::Unpartitioned),
        plan::PartitionKind::Random => Ok(DataStreamPartitionType::Random),
        plan::PartitionKind::Hash => Ok(DataStreamPartitionType::HashPartitioned),
        plan::PartitionKind::Unspecified => {
            Err("native DataPartition kind is unspecified".to_string())
        }
    }
}

fn decode_expr_with_context(
    expression: &expr::Expr,
    arena: &mut ExprArena,
    layout: &Layout,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<ExprId, NativeFragmentDecodeError> {
    match ctx {
        Some(ctx) => ctx.decode_expression(
            expression,
            FieldPath::root("plan_fragment").field("sink"),
            arena,
            layout,
        ),
        None => Err(NativeFragmentDecodeError::unsupported(
            FieldPath::root("plan_fragment").field("sink"),
            "native expression decoder must be supplied by the backend runtime",
        )),
    }
}

fn lower_output_exprs(
    output_exprs: &[expr::Expr],
    arena: &mut ExprArena,
    layout: &Layout,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<Vec<ExprId>, NativeFragmentLeafDecodeError> {
    if output_exprs.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "output_exprs",
            "native Iceberg sink missing output exprs",
        ));
    }
    output_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            decode_expr_with_context(expr, arena, layout, ctx).map_err(|err| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "output_exprs",
                    format!("native Iceberg sink output_exprs[{idx}]: {err}"),
                )
                .append_index(idx)
            })
        })
        .collect()
}

fn lower_output_columns_as_slot_refs(
    output_columns: &[common::OutputColumn],
    input: Option<&plan::IcebergWriteInputBinding>,
    arena: &mut ExprArena,
) -> Result<Vec<ExprId>, NativeFragmentLeafDecodeError> {
    let selected = match input.and_then(|input| input.kind.as_ref()) {
        Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(true)) | None => {
            output_columns.iter().collect::<Vec<_>>()
        }
        Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(false)) => {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "root_output_by_ordinal",
                "native Iceberg write sink root_output_by_ordinal marker must be true",
            )
            .prepend_field("input"));
        }
        Some(plan::iceberg_write_input_binding::Kind::OutputOrdinals(ordinals)) => ordinals
            .values
            .iter()
            .enumerate()
            .map(|(index, ordinal)| {
                let idx = usize::try_from(*ordinal).map_err(|_| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::OutOfRange,
                        "values",
                        format!(
                            "native Iceberg write sink output ordinal {ordinal} overflows usize"
                        ),
                    )
                    .append_index(index)
                    .prepend_field("output_ordinals")
                    .prepend_field("input")
                })?;
                output_columns.get(idx).ok_or_else(|| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::OutOfRange,
                        "values",
                        format!(
                            "native Iceberg write sink output ordinal {ordinal} is out of range"
                        ),
                    )
                    .append_index(index)
                    .prepend_field("output_ordinals")
                    .prepend_field("input")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    if selected.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "output_columns",
            "native Iceberg write sink requires at least one output column",
        ));
    }
    selected
        .into_iter()
        .map(|column| {
            let data_type = column
                .r#type
                .as_ref()
                .ok_or_else(|| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::MissingField,
                        "type",
                        format!(
                            "native Iceberg write sink output column {} missing type",
                            column.name
                        ),
                    )
                })
                .and_then(|wire| {
                    decode_type(wire).map_err(|error| {
                        NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::InvalidValue,
                            "type",
                            error,
                        )
                    })
                })?;
            Ok(arena.push_typed(ExprNode::SlotId(SlotId::new(column.column_id)), data_type))
        })
        .collect()
}

fn iceberg_sink_mode_from_native(value: i32) -> Result<IcebergSinkMode, String> {
    let mode = plan::IcebergWriteSinkMode::try_from(value)
        .map_err(|_| format!("unknown native IcebergWriteSinkMode value {value}"))?;
    match mode {
        plan::IcebergWriteSinkMode::Data | plan::IcebergWriteSinkMode::RowLineageData => {
            Ok(IcebergSinkMode::Data)
        }
        plan::IcebergWriteSinkMode::PositionDeletes => Ok(IcebergSinkMode::PositionDeletes),
        plan::IcebergWriteSinkMode::DeletionVectors => Ok(IcebergSinkMode::DeletionVectors),
        plan::IcebergWriteSinkMode::EqualityDeletes => Ok(IcebergSinkMode::EqualityDeletes),
        plan::IcebergWriteSinkMode::Unspecified => {
            Err("native Iceberg write sink mode is unspecified".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::decode_fragment_sink_assignment;
    use novarocks::runtime::fragment::instance::FragmentSinkAssignment;
    use novarocks_protocol::{novarocks as proto, plan};

    #[test]
    fn result_sink_without_destinations_has_no_assignment() {
        let assignment = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::Result(true)),
            },
            &proto::InstanceParams::default(),
        )
        .expect("result sink assignment decodes");

        assert!(matches!(assignment, FragmentSinkAssignment::None));
    }

    #[test]
    fn stream_destination_missing_id_preserves_wire_error() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(
                    plan::DataStreamSink::default(),
                )),
            },
            &proto::InstanceParams {
                destinations: vec![proto::Destination::default()],
                ..Default::default()
            },
        )
        .expect_err("destination id is required");

        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.destinations[0].finst_id (missing field): native Destination requires finst_id"
        );
    }
}
