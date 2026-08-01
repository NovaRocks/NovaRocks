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

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use novarocks::common::ids::SlotId;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::fragment::sink::DataStreamPartitionType;
use novarocks::exec::fragment::sink::{
    ConnectorWriteSinkProgram, DataStreamSinkBranchProgram, FragmentSinkProgram,
    MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    build_change_stream_split_predicate,
};
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};
use novarocks::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks::runtime::fragment::FragmentSinkAssignment;
use novarocks::runtime::query_options::query_expire_durations;
use novarocks_protocol::novarocks as native_proto;
use novarocks_protocol::{common, expr, plan};
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorOpenWriterRequest, ConnectorRequestContext, ConnectorWriteExecutionId,
    ConnectorWriteOperationId, ConnectorWriterHandle, ConnectorWriterIdentity,
};

use super::context::NativePlanDecodeContext;
use super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use super::layout::Layout;

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
        plan::data_sink::Kind::ConnectorWrite(connector) => {
            let context = ctx.ok_or_else(|| {
                NativeFragmentDecodeError::unsupported(
                    path.clone().field("connector_write"),
                    "native connector writer requires the backend decode context",
                )
            })?;
            decode_connector_write_sink_program(connector, fragment, context)
                .map(FragmentSinkProgram::ConnectorWrite)
                .map_err(|error| error.into_native(path.field("connector_write")))
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

fn decode_connector_write_sink_program(
    sink: &plan::ConnectorWriteFragmentSink,
    fragment: &plan::PlanFragment,
    context: &NativePlanDecodeContext,
) -> Result<ConnectorWriteSinkProgram, NativeFragmentLeafDecodeError> {
    let envelope = sink.handle.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "handle",
            "native connector write sink requires a writer handle",
        )
    })?;
    let wire_writer = envelope.writer.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "handle",
            "native connector writer handle requires writer identity",
        )
        .append_field("writer")
    })?;
    let operation_id = ConnectorWriteOperationId::from_bytes(required_uuid_bytes(
        &wire_writer.operation_id,
        "operation_id",
    )?);
    let execution_id = ConnectorWriteExecutionId::new(
        required_uuid_bytes(&wire_writer.execution_query_id, "execution_query_id")?,
        wire_writer.execution_attempt_id,
    );
    let wire_finst = wire_writer.fragment_instance_id.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "handle",
            "native connector writer identity requires fragment_instance_id",
        )
        .append_field("writer")
        .append_field("fragment_instance_id")
    })?;
    let fragment_instance_id = unique_id_bytes(wire_finst.hi, wire_finst.lo);
    let context_finst = context.fragment_instance_id().get();
    if fragment_instance_id != unique_id_bytes(context_finst.high(), context_finst.low()) {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "handle",
            "connector writer identity fragment instance does not match native submission",
        )
        .append_field("writer")
        .append_field("fragment_instance_id"));
    }
    if wire_writer.fragment_id != fragment.fragment_id as i32 {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "handle",
            "connector writer identity fragment id does not match native fragment",
        )
        .append_field("writer")
        .append_field("fragment_id"));
    }
    let query_id = context.query_id().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "handle",
            "connector writer handle requires native query identity",
        )
    })?;
    if execution_id.query_id() != unique_id_bytes(query_id.high(), query_id.low()) {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "handle",
            "connector writer execution query id does not match native submission",
        )
        .append_field("writer")
        .append_field("execution_query_id"));
    }
    let binding_key = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(&wire_writer.connector_instance_id).map_err(
            |error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "handle",
                    error.to_string(),
                )
                .append_field("writer")
                .append_field("connector_instance_id")
            },
        )?,
        incarnation: ConnectorInstanceIncarnation::from_bytes(required_uuid_bytes(
            &wire_writer.connector_incarnation,
            "connector_incarnation",
        )?),
    };
    let writer = ConnectorWriterIdentity::new(
        operation_id,
        novarocks_spi::connector::ConnectorWriteCohortId::from_bytes(required_digest_bytes(
            &wire_writer.cohort_id,
            "cohort_id",
        )?),
        execution_id,
        fragment_instance_id,
        wire_writer.fragment_id,
        wire_writer.backend_num,
        wire_writer.sink_ordinal,
        binding_key.clone(),
    );
    let handle = ConnectorWriterHandle::try_new(
        binding_key.clone(),
        writer,
        envelope.contract_version,
        Bytes::copy_from_slice(&envelope.payload),
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "handle",
            error.to_string(),
        )
    })?;
    if envelope.payload_sha256.as_slice() != handle.payload_digest() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "handle",
            "connector writer handle payload digest does not match payload",
        )
        .append_field("payload_sha256"));
    }
    let binding = context
        .execution_resolver()?
        .resolve(&binding_key)
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "handle",
                format!("resolve connector writer execution binding: {error}"),
            )
        })?;
    if binding.key() != &binding_key || binding.write().is_none() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "handle",
            "connector writer requires its exact BE write execution binding",
        ));
    }
    let root_schema = context
        .decode_output_layout(
            &fragment.output_columns,
            FieldPath::root("plan_fragment").field("output_columns"),
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "input",
                error.to_string(),
            )
        })?
        .chunk_schema()
        .arrow_schema_ref();
    let root_input_width = root_schema.fields().len();
    let (expected_schema, input_ordinals) =
        decode_connector_write_input(sink.input.as_ref(), root_schema)?;
    let (_, query_expire) = query_expire_durations(context.query_options());
    let request = ConnectorOpenWriterRequest {
        handle,
        expected_schema,
        context: ConnectorRequestContext::try_new(
            Instant::now() + query_expire,
            context.connector_cancellation()?,
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "handle",
                error.to_string(),
            )
        })?,
    };
    ConnectorWriteSinkProgram::try_new(binding, request, root_input_width, input_ordinals).map_err(
        |error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "handle",
                error,
            )
        },
    )
}

fn decode_connector_write_input(
    input: Option<&plan::ConnectorWriteInputBinding>,
    root_schema: SchemaRef,
) -> Result<(SchemaRef, Option<Vec<usize>>), NativeFragmentLeafDecodeError> {
    let input = input.ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "input",
            "native connector write sink requires input binding",
        )
    })?;
    match input.kind.as_ref() {
        Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(true)) => {
            Ok((root_schema, None))
        }
        Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(false)) => {
            Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "input",
                "connector write root_output_by_ordinal marker must be true",
            ))
        }
        Some(plan::connector_write_input_binding::Kind::OutputOrdinals(ordinals)) => {
            if ordinals.values.is_empty() {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "input",
                    "connector writer output_ordinals must not be empty",
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            let ordinals = ordinals
                .values
                .iter()
                .map(|value| {
                    let ordinal = usize::try_from(*value).map_err(|_| {
                        NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::OutOfRange,
                            "input",
                            "connector writer output ordinal does not fit usize",
                        )
                    })?;
                    if ordinal >= root_schema.fields().len() || !seen.insert(ordinal) {
                        return Err(NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::InvalidValue,
                            "input",
                            "connector writer output ordinal is duplicated or outside root output",
                        ));
                    }
                    Ok(ordinal)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let fields = ordinals
                .iter()
                .map(|ordinal| root_schema.fields()[*ordinal].clone())
                .collect::<Vec<_>>();
            Ok((Arc::new(Schema::new(fields)), Some(ordinals)))
        }
        None => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "input",
            "connector write input requires kind",
        )),
    }
}

fn required_uuid_bytes(
    value: &[u8],
    field: &'static str,
) -> Result<[u8; 16], NativeFragmentLeafDecodeError> {
    value.try_into().map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            field,
            format!("connector write {field} must contain exactly 16 bytes"),
        )
    })
}

fn required_digest_bytes(
    value: &[u8],
    field: &'static str,
) -> Result<[u8; 32], NativeFragmentLeafDecodeError> {
    value.try_into().map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            field,
            format!("connector write {field} must contain exactly 32 bytes"),
        )
    })
}

fn unique_id_bytes(hi: i64, lo: i64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&hi.to_be_bytes());
    bytes[8..].copy_from_slice(&lo.to_be_bytes());
    bytes
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
        plan::data_sink::Kind::ChangeStreamRouter(router) => {
            let groups = router
                .branches
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let group_path = path
                        .clone()
                        .field("change_stream_router")
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
        | plan::data_sink::Kind::ConnectorWrite(_) => {
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
    router: &plan::ChangeStreamRouterSink,
    output_exprs: &[expr::Expr],
    output_columns: &[common::OutputColumn],
    layout: &Layout,
    context: Option<&NativePlanDecodeContext>,
) -> Result<SplitDataStreamSinkProgram, NativeFragmentLeafDecodeError> {
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
            let branch_path = |error: NativeFragmentLeafDecodeError| {
                error.prepend_index(index).prepend_field("branches")
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
                                .field("branches")
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
                            .prepend_field("branches")
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
            )
            .map_err(branch_path)?;
            Ok((
                decode_change_stream_branch_kind(branch.branch_kind).map_err(|error| {
                    branch_path(NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidEnum,
                        "branch_kind",
                        error,
                    ))
                })?,
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
    let (branch_kinds, streams): (Vec<_>, Vec<_>) = branches.into_iter().unzip();
    let split_exprs = branch_kinds
        .into_iter()
        .map(|branch_kind| {
            build_change_stream_split_predicate(
                &mut partition_arena,
                change_op_slot_id,
                data_route_slot_id,
                branch_kind,
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
    SplitDataStreamSinkProgram::try_new(streams, split_exprs, partition_arena).map_err(|error| {
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
) -> Result<novarocks::exec::expr::ExprId, NativeFragmentDecodeError> {
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
) -> Result<novarocks::exec::change_op::ChangeStreamBranchKind, String> {
    match plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(novarocks::exec::change_op::ChangeStreamBranchKind::DeleteDv)
        }
        plan::ChangeStreamBranchKind::ReuseData => {
            Ok(novarocks::exec::change_op::ChangeStreamBranchKind::ReuseData)
        }
        plan::ChangeStreamBranchKind::FreshData => {
            Ok(novarocks::exec::change_op::ChangeStreamBranchKind::FreshData)
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
                novarocks_types::UniqueId::new(finst_id.hi, finst_id.lo),
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
                novarocks_types::UniqueId::new(finst_id.hi, finst_id.lo),
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
    use super::decode_fragment_sink_assignment;
    use novarocks::runtime::fragment::FragmentSinkAssignment;
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
