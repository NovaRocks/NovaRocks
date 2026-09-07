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

//! Fragment-native proto sink lowering.

use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
use novarocks_execution::exec::fragment::sink::{
    DataStreamSinkBranchProgram, FragmentSinkProgram, MultiCastDataStreamSinkProgram,
    SplitDataStreamSinkProgram, build_change_stream_split_predicate,
};
use novarocks_execution::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks_execution::runtime::fragment::FragmentSinkAssignment;
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::novarocks as native_proto;
use novarocks_proto_models::{common, expr, plan};
use novarocks_spi::connector::ConnectorRowMutationEffect;
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_types::SlotId;

use super::context::NativePlanDecodeContext;
use super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use super::layout::Layout;

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
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
        plan::data_sink::Kind::ChangeStreamRouter(router) => decode_change_stream_router_program(
            router,
            &fragment.output_exprs,
            &fragment.output_columns,
            layout,
            ctx,
        )
        .map(FragmentSinkProgram::SplitDataStream)
        .map_err(|error| error.into_native(path.field("change_stream_router"))),
    }
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
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
        plan::data_sink::Kind::ChangeStreamRouter(router) => {
            let groups = router
                .routes
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let group_path = path
                        .clone()
                        .field("change_stream_router")
                        .field("routes")
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
        plan::data_sink::Kind::Result(_) | plan::data_sink::Kind::Noop(_) => {
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
    (|| -> Result<DataStreamSinkBranchProgram, NativeFragmentLeafDecodeError> {
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
                    decode_sink_expression(
                        expression,
                        partition_arena,
                        layout,
                        ctx,
                        FieldPath::root("plan_fragment")
                            .field("sink")
                            .field("output_partition")
                            .field("exprs")
                            .index(index),
                    )
                    .map_err(|error| {
                        NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::InvalidValue,
                            "expr",
                            error.to_string(),
                        )
                        .prepend_index(index)
                        .prepend_field("exprs")
                        .prepend_field("output_partition")
                    })
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
    })()
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

/// Decode every branch's logical write target and check it against the set this
/// plan sealed.
///
/// A change-stream router's branches are exactly the plan's logical write
/// targets, dense from zero. An ordinal outside that set names a writer this
/// plan does not contain, and the rows routed to it would be staged against the
/// wrong writer handle -- or none -- with nothing downstream able to notice. It
/// is refused here, at the wire boundary, with the exact field that carried it.
fn decode_router_write_target_ordinals(
    router: &plan::ChangeStreamRouterSink,
) -> Result<(), NativeFragmentLeafDecodeError> {
    let sealed_target_count = router.routes.len();
    let mut seen = std::collections::BTreeSet::new();
    for (index, branch) in router.routes.iter().enumerate() {
        let at = |kind, message: String| {
            NativeFragmentLeafDecodeError::at_field(kind, "write_target_ordinal", message)
                .prepend_index(index)
                .prepend_field("routes")
        };
        let ordinal = WriteTargetOrdinal::try_new(branch.write_target_ordinal)
            .map_err(|error| at(ProtocolErrorKind::InvalidValue, error.to_string()))?;
        if usize::try_from(ordinal.get()).is_ok_and(|value| value < sealed_target_count) {
            if !seen.insert(ordinal) {
                return Err(at(
                    ProtocolErrorKind::InconsistentFields,
                    format!(
                        "native CHANGE_STREAM_ROUTER_SINK repeats write target ordinal {}",
                        ordinal.get()
                    ),
                ));
            }
            continue;
        }
        return Err(at(
            ProtocolErrorKind::InvalidValue,
            format!(
                "native CHANGE_STREAM_ROUTER_SINK route write target ordinal {} is outside the \
                 sealed set of {sealed_target_count} write targets",
                branch.write_target_ordinal
            ),
        ));
    }
    Ok(())
}

fn decode_change_stream_router_program(
    router: &plan::ChangeStreamRouterSink,
    output_exprs: &[expr::Expr],
    output_columns: &[common::OutputColumn],
    layout: &Layout,
    context: Option<&NativePlanDecodeContext>,
) -> Result<SplitDataStreamSinkProgram, NativeFragmentLeafDecodeError> {
    let effect_slot_id = SlotId::try_from(output_slot_id_for_ordinal(
        output_columns,
        router.effect_output_ordinal,
        "effect_output_ordinal",
    )?)
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "effect_output_ordinal",
            error,
        )
    })?;
    decode_router_write_target_ordinals(router)?;
    let mut partition_arena = ExprArena::default();
    let branches = router
        .routes
        .iter()
        .enumerate()
        .map(|(index, branch)| {
            let branch_path = |error: NativeFragmentLeafDecodeError| {
                error.prepend_index(index).prepend_field("routes")
            };
            let partition = branch_partition_from_native(branch, output_exprs).map_err(branch_path)?;
            let partition_type = decode_stream_partition_type(partition.kind).map_err(|error| {
                branch_path(
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidEnum,
                        "kind",
                        error,
                    )
                    .prepend_field("output_partition"),
                )
            })?;
            let output_partition_exprs = if partition_type.requires_exprs() {
                partition
                    .exprs
                    .iter()
                    .enumerate()
                    .map(|(expr_index, expression)| {
                        decode_sink_expression(
                            expression,
                            &mut partition_arena,
                            layout,
                            context,
                            FieldPath::root("plan_fragment")
                                .field("sink")
                                .field("change_stream_router")
                                .field("routes")
                                .index(index)
                                .field("output_partition")
                                .field("exprs")
                                .index(expr_index),
                        )
                        .map_err(|error| {
                            NativeFragmentLeafDecodeError::at_field(
                                ProtocolErrorKind::InvalidValue,
                                "exprs",
                                format!(
                                    "native CHANGE_STREAM_ROUTER_SINK branch[{index}] partition expr[{expr_index}]: {error}"
                                ),
                            )
                            .append_index(expr_index)
                            .prepend_field("output_partition")
                            .prepend_index(index)
                            .prepend_field("routes")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
            let branch_output_columns = decode_router_output_slots(
                &branch.input_ordinals,
                output_columns,
                "input_ordinals",
            )
            .map_err(branch_path)?;
            let _route_id = decode_route_id(&branch.route_id).map_err(|error| branch_path(
                NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "route_id", error)
            ))?;
            let accepted_effects = branch.accepted_effects.iter().enumerate().map(|(effect_index, value)| {
                decode_row_mutation_effect(*value).map_err(|error| branch_path(
                    NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidEnum, "accepted_effects", error).append_index(effect_index)
                ))
            }).collect::<Result<Vec<_>, _>>()?;
            if accepted_effects.is_empty() {
                return Err(branch_path(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::MissingField, "accepted_effects", "native CHANGE_STREAM_ROUTER_SINK route accepts no effects"
                )));
            }
            Ok((
                accepted_effects,
                DataStreamSinkBranchProgram::try_new(
                    branch.target_exchange_node_id,
                    Vec::new(),
                    partition_type,
                    output_partition_exprs,
                    branch_output_columns,
                    None,
                )
                .map_err(|error| {
                    branch_path(NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidValue,
                        "output_partition",
                        error,
                    ))
                })?,
            ))
        })
        .collect::<Result<Vec<_>, NativeFragmentLeafDecodeError>>()?;
    let (accepted_effect_sets, streams): (Vec<_>, Vec<_>) = branches.into_iter().unzip();
    let split_exprs = accepted_effect_sets
        .into_iter()
        .map(|accepted_effects| {
            build_change_stream_split_predicate(
                &mut partition_arena,
                effect_slot_id,
                &accepted_effects,
            )
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "branches",
                    error,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    SplitDataStreamSinkProgram::try_new_with_fanout(streams, split_exprs, partition_arena, true)
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "branches",
                error,
            )
        })
}

fn decode_sink_expression(
    expression: &expr::Expr,
    arena: &mut ExprArena,
    layout: &Layout,
    context: Option<&NativePlanDecodeContext>,
    path: FieldPath,
) -> Result<novarocks_execution::exec::expr::ExprId, NativeFragmentDecodeError> {
    let context = context.ok_or_else(|| {
        NativeFragmentDecodeError::unsupported(
            path.clone(),
            "native sink expression requires the backend decode context",
        )
    })?;
    context.decode_expression(expression, path, arena, layout)
}

fn branch_partition_from_native(
    branch: &plan::ChangeStreamBranchRoute,
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
                    "native CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} overflows usize"
                )).append_index(ordinal_index)
            })?;
            output_exprs.get(output_index).cloned().ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "output_partition_ordinals", format!(
                    "native CHANGE_STREAM_ROUTER_SINK partition ordinal {ordinal} is out of range"
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
    {
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
    }
}

fn decode_row_mutation_effect(value: i32) -> Result<ConnectorRowMutationEffect, String> {
    match plan::RowMutationEffect::try_from(value)
        .map_err(|_| format!("unknown native RowMutationEffect value {value}"))?
    {
        plan::RowMutationEffect::Delete => Ok(ConnectorRowMutationEffect::Delete),
        plan::RowMutationEffect::Replace => Ok(ConnectorRowMutationEffect::Replace),
        plan::RowMutationEffect::Insert => Ok(ConnectorRowMutationEffect::Insert),
        plan::RowMutationEffect::Unspecified => {
            Err("native RowMutationEffect is unspecified".to_string())
        }
    }
}

fn decode_route_id(value: &[u8]) -> Result<[u8; 32], String> {
    value.try_into().map_err(|_| {
        format!(
            "native route_id must contain exactly 32 bytes, got {}",
            value.len()
        )
    })
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
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
            let source_finst_id = destination.source_finst_id.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    destination_path.clone().field("source_finst_id"),
                    "native stream destination requires source_finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                novarocks_types::UniqueId::new(finst_id.hi, finst_id.lo),
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        destination_path.field("endpoint"),
                        error,
                    )
                })?,
                novarocks_types::UniqueId::new(source_finst_id.hi, source_finst_id.lo),
                destination.sender_ordinal,
                destination.sender_count,
            )
            .map_err(|detail| NativeFragmentDecodeError::invalid_value(destination_path, detail))?)
        })
        .collect()
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
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
            let source_finst_id = destination.source_finst_id.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    destination_path.clone().field("source_finst_id"),
                    "native Destination requires source_finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                novarocks_types::UniqueId::new(finst_id.hi, finst_id.lo),
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        destination_path.field("endpoint"),
                        error,
                    )
                })?,
                novarocks_types::UniqueId::new(source_finst_id.hi, source_finst_id.lo),
                destination.sender_ordinal,
                destination.sender_count,
            )
            .map_err(|detail| NativeFragmentDecodeError::invalid_value(destination_path, detail))?)
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use novarocks_execution::runtime::fragment::FragmentSinkAssignment;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_proto_models::{common, novarocks as proto, plan};
    use novarocks_spi::connector::ConnectorRowMutationEffect;
    use prost::Message;

    use super::{
        decode_fragment_sink_assignment, decode_fragment_sink_program,
        decode_fragment_sink_program_with_context, decode_row_mutation_effect,
    };
    use crate::fragment::decode::plan::context::NativePlanDecodeContext;
    use crate::fragment::decode::plan::layout::Layout;

    #[test]
    fn row_mutation_effect_decoding_rejects_unspecified_and_unknown_values() {
        assert_eq!(
            decode_row_mutation_effect(plan::RowMutationEffect::Delete as i32),
            Ok(ConnectorRowMutationEffect::Delete)
        );
        assert_eq!(
            decode_row_mutation_effect(plan::RowMutationEffect::Replace as i32),
            Ok(ConnectorRowMutationEffect::Replace)
        );
        assert_eq!(
            decode_row_mutation_effect(plan::RowMutationEffect::Insert as i32),
            Ok(ConnectorRowMutationEffect::Insert)
        );
        assert_eq!(
            decode_row_mutation_effect(plan::RowMutationEffect::Unspecified as i32),
            Err("native RowMutationEffect is unspecified".to_string())
        );
        assert_eq!(
            decode_row_mutation_effect(99),
            Err("unknown native RowMutationEffect value 99".to_string())
        );
    }

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

    fn plan_destination(id: i64) -> plan::StreamDestination {
        plan::StreamDestination {
            finst_id: Some(common::UniqueId { hi: 1, lo: id }),
            endpoint: "127.0.0.1:8060".to_string(),
            source_finst_id: Some(common::UniqueId { hi: 9, lo: 10 }),
            sender_ordinal: 0,
            sender_count: 1,
        }
    }

    fn instance_destination(id: i64) -> proto::Destination {
        proto::Destination {
            finst_id: Some(common::UniqueId { hi: 2, lo: id }),
            endpoint: "127.0.0.1:8061".to_string(),
            source_finst_id: Some(common::UniqueId { hi: 9, lo: 10 }),
            sender_ordinal: 0,
            sender_count: 1,
        }
    }

    fn assert_single_destination_group(assignment: FragmentSinkAssignment, expected_lo: i64) {
        let FragmentSinkAssignment::DestinationGroups { groups, sender_id } = assignment else {
            panic!("expected destination groups");
        };
        assert_eq!(sender_id, None);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 1);
        assert_eq!(groups[0][0].finst_id().low(), expected_lo);
    }

    #[test]
    fn multicast_assignment_ignores_redundant_flat_instance_destinations() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                plan::MultiCastDataStreamSink {
                    sinks: Vec::new(),
                    destinations: vec![plan::StreamDestinationList {
                        destinations: vec![plan_destination(11)],
                    }],
                },
            )),
        };
        let instance = proto::InstanceParams {
            destinations: vec![instance_destination(99)],
            ..Default::default()
        };

        let assignment = decode_fragment_sink_assignment(&sink, &instance)
            .expect("redundant flat destinations must remain wire compatible");

        assert_single_destination_group(assignment, 11);
    }

    #[test]
    fn data_stream_missing_partition_uses_exact_sink_branch_path() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(
                    plan::DataStreamSink::default(),
                )),
            }),
            ..Default::default()
        };

        let error = decode_fragment_sink_program(&fragment, &Layout::default())
            .expect_err("missing stream partition must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.data_stream.output_partition"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn data_stream_invalid_output_column_uses_exact_indexed_path_and_kind() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                    output_partition: Some(plan::DataPartition {
                        kind: plan::PartitionKind::Unpartitioned as i32,
                        ..Default::default()
                    }),
                    output_columns: vec![1, -1],
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        let error = decode_fragment_sink_program(&fragment, &Layout::default())
            .expect_err("invalid output column must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.data_stream.output_columns[1]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn data_stream_duplicate_output_column_uses_exact_indexed_path_and_kind() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                    output_partition: Some(plan::DataPartition {
                        kind: plan::PartitionKind::Unpartitioned as i32,
                        ..Default::default()
                    }),
                    output_columns: vec![1, 1],
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        let error = decode_fragment_sink_program(&fragment, &Layout::default())
            .expect_err("duplicate output column must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.data_stream.output_columns[1]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
    }

    /// The connector write sink is no longer a fragment terminal: writing is a
    /// dataflow shape rooted at a table writer node, and `DataSink` tag 7 is
    /// reserved. A plan that still carries the retired sink decodes it as an
    /// unknown field and is refused for having no sink kind at all, so the
    /// retired tag can never be mistaken for a decodable terminal.
    #[test]
    fn the_retired_connector_write_sink_tag_fails_closed() {
        // DataSink field 7, wire type 2: the retired `connector_write` arm.
        let sink =
            plan::DataSink::decode(&[0x3a, 0x00][..]).expect("retired sink tag stays decodable");
        assert!(sink.kind.is_none());

        let fragment = plan::PlanFragment {
            sink: Some(sink),
            ..Default::default()
        };

        let error = decode_fragment_sink_program_with_context(
            &fragment,
            &Layout::default(),
            Some(&NativePlanDecodeContext::default()),
        )
        .expect_err("the retired connector write sink must fail closed");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.sink.kind");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    fn router_fragment(route: plan::ChangeStreamBranchRoute) -> plan::PlanFragment {
        plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                    plan::ChangeStreamRouterSink {
                        routes: vec![route],
                        ..Default::default()
                    },
                )),
            }),
            output_columns: vec![common::OutputColumn {
                column_id: 1,
                name: "effect".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn router_effect_uses_exact_indexed_path() {
        let error = decode_fragment_sink_program(
            &router_fragment(plan::ChangeStreamBranchRoute {
                route_id: vec![1; 32],
                accepted_effects: vec![plan::RowMutationEffect::Unspecified as i32],
                input_ordinals: vec![0],
                ..Default::default()
            }),
            &Layout::default(),
        )
        .expect_err("unspecified branch effect must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.routes[0].accepted_effects[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
        assert_eq!(protocol.detail(), "native RowMutationEffect is unspecified");
    }

    #[test]
    fn router_output_ordinal_uses_exact_indexed_path() {
        let error = decode_fragment_sink_program(
            &router_fragment(plan::ChangeStreamBranchRoute {
                route_id: vec![1; 32],
                accepted_effects: vec![plan::RowMutationEffect::Insert as i32],
                input_ordinals: vec![1],
                ..Default::default()
            }),
            &Layout::default(),
        )
        .expect_err("out-of-range branch output ordinal must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.routes[0].input_ordinals[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            protocol.detail(),
            "native router output ordinal 1 is out of range"
        );
    }

    #[test]
    fn router_partition_ordinal_uses_exact_indexed_path() {
        let error = decode_fragment_sink_program(
            &router_fragment(plan::ChangeStreamBranchRoute {
                route_id: vec![1; 32],
                accepted_effects: vec![plan::RowMutationEffect::Insert as i32],
                input_ordinals: vec![0],
                output_partition_ordinals: vec![1],
                ..Default::default()
            }),
            &Layout::default(),
        )
        .expect_err("out-of-range partition ordinal must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.routes[0].output_partition_ordinals[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            protocol.detail(),
            "native CHANGE_STREAM_ROUTER_SINK partition ordinal 1 is out of range"
        );
    }

    #[test]
    fn router_assignment_ignores_redundant_flat_instance_destinations() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                plan::ChangeStreamRouterSink {
                    routes: vec![plan::ChangeStreamBranchRoute {
                        destinations: Some(plan::StreamDestinationList {
                            destinations: vec![plan_destination(12)],
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        };
        let instance = proto::InstanceParams {
            destinations: vec![instance_destination(98)],
            ..Default::default()
        };

        let assignment = decode_fragment_sink_assignment(&sink, &instance)
            .expect("redundant flat destinations must remain wire compatible");
        assert_single_destination_group(assignment, 12);
    }

    #[test]
    fn router_branch_rejects_duplicate_output_slots() {
        let error = decode_fragment_sink_program(
            &router_fragment(plan::ChangeStreamBranchRoute {
                route_id: vec![1; 32],
                accepted_effects: vec![plan::RowMutationEffect::Insert as i32],
                input_ordinals: vec![0, 0],
                ..Default::default()
            }),
            &Layout::default(),
        )
        .expect_err("duplicate router output slots must be rejected during decode");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.routes[0].input_ordinals[1]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            protocol.detail(),
            "native ICEBERG_CHANGE_STREAM_ROUTER_SINK duplicate output slot id: 1"
        );
    }

    /// The router's branches are the plan's logical write targets. A branch
    /// naming a target this plan never sealed would stage its rows against the
    /// wrong writer handle, so decode refuses it and says which field carried
    /// the value.
    #[test]
    fn router_write_target_ordinal_outside_the_sealed_set_uses_exact_indexed_path() {
        let error = decode_fragment_sink_program(
            &router_fragment(plan::ChangeStreamBranchRoute {
                route_id: vec![1; 32],
                accepted_effects: vec![plan::RowMutationEffect::Insert as i32],
                input_ordinals: vec![0],
                write_target_ordinal: 3,
                ..Default::default()
            }),
            &Layout::default(),
        )
        .expect_err("a write target outside the sealed set must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.routes[0].write_target_ordinal"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            protocol.detail(),
            concat!(
                "native CHANGE_STREAM_ROUTER_SINK route write target ordinal 3 ",
                "is outside the sealed set of 1 write targets"
            )
        );
    }

    /// Two branches claiming one write target make the plan's target map
    /// ambiguous and leave one sealed writer with no branch at all.
    #[test]
    fn router_repeated_write_target_ordinal_is_rejected() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                    plan::ChangeStreamRouterSink {
                        routes: vec![
                            plan::ChangeStreamBranchRoute {
                                route_id: vec![1; 32],
                                accepted_effects: vec![plan::RowMutationEffect::Insert as i32],
                                input_ordinals: vec![0],
                                write_target_ordinal: 0,
                                ..Default::default()
                            },
                            plan::ChangeStreamBranchRoute {
                                route_id: vec![2; 32],
                                accepted_effects: vec![plan::RowMutationEffect::Delete as i32],
                                input_ordinals: vec![0],
                                write_target_ordinal: 0,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                )),
            }),
            output_columns: vec![common::OutputColumn {
                column_id: 1,
                name: "effect".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let error = decode_fragment_sink_program(&fragment, &Layout::default())
            .expect_err("a repeated write target must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.routes[1].write_target_ordinal"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            protocol.detail(),
            "native CHANGE_STREAM_ROUTER_SINK repeats write target ordinal 0"
        );
    }
}
