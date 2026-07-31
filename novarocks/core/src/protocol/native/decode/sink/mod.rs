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

use super::error::NativeFragmentLeafDecodeError;
use super::node::NativePlanDecodeContext;
use crate::common::ids::SlotId;
use crate::exec::expr::ExprArena;
use crate::exec::fragment::sink::{
    ConnectorWriteSinkProgram, DataStreamSinkBranchProgram, FragmentSinkProgram,
    MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    build_change_stream_split_predicate,
};
use crate::exec::operators::DataStreamPartitionType;
use crate::proto::{common, expr, novarocks, plan};
use crate::protocol::common::error::{FieldPath, ProtocolErrorKind};
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment::instance::FragmentSinkAssignment;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::query_expire_durations;
use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorOpenWriterRequest, ConnectorRequestContext, ConnectorWriteExecutionId,
    ConnectorWriteOperationId, ConnectorWriterHandle, ConnectorWriterIdentity, StatisticsMetric,
    StatisticsMetricRequest,
};

struct NativeWriterCancellation {
    query_id: QueryId,
}

impl novarocks_spi::connector::ConnectorCancellation for NativeWriterCancellation {
    fn is_cancelled(&self) -> bool {
        query_context_manager().is_query_canceled(self.query_id)
    }
}

pub(crate) fn decode_fragment_sink_program(
    fragment: &plan::PlanFragment,
    layout: &super::layout::Layout,
) -> Result<FragmentSinkProgram, super::NativeFragmentDecodeError> {
    decode_fragment_sink_program_with_context(fragment, layout, None)
}

pub(crate) fn decode_fragment_sink_program_with_context(
    fragment: &plan::PlanFragment,
    layout: &super::layout::Layout,
    context: Option<&NativePlanDecodeContext>,
) -> Result<FragmentSinkProgram, super::NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let sink = fragment.sink.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(path.clone(), "native PlanFragment requires sink")
    })?;
    let kind = sink.kind.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::Result(true) => {
            if !fragment.output_exprs.is_empty() {
                return Err(super::NativeFragmentDecodeError::unsupported(
                    path.field("result"),
                    "native RESULT sink does not support fragment output_exprs yet",
                ));
            }
            Ok(FragmentSinkProgram::Result)
        }
        plan::data_sink::Kind::Noop(true) => Ok(FragmentSinkProgram::Noop),
        plan::data_sink::Kind::Statistics(statistics) => decode_statistics_sink(statistics)
            .map(FragmentSinkProgram::Statistics)
            .map_err(|error| error.into_native(path.field("statistics"))),
        plan::data_sink::Kind::Result(false) => {
            Err(super::NativeFragmentDecodeError::invalid_value(
                path.field("result"),
                "native RESULT sink marker must be true",
            ))
        }
        plan::data_sink::Kind::Noop(false) => Err(super::NativeFragmentDecodeError::invalid_value(
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
                context,
            )
            .map_err(|error| error.into_native(path.clone().field("data_stream")))?;
            branch
                .into_program(partition_arena)
                .map(FragmentSinkProgram::DataStream)
                .map_err(super::NativeFragmentDecodeError::from)
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
                        context,
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
                .map_err(super::NativeFragmentDecodeError::from)
        }
        plan::data_sink::Kind::ConnectorWrite(connector) => {
            let context = context.ok_or_else(|| {
                super::NativeFragmentDecodeError::unsupported(
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
            context,
        )
        .map(FragmentSinkProgram::SplitDataStream)
        .map_err(|error| error.into_native(path.field("change_stream_router"))),
    }
}

fn decode_statistics_sink(
    sink: &plan::StatisticsSink,
) -> Result<crate::exec::fragment::sink::StatisticsSinkProgram, NativeFragmentLeafDecodeError> {
    let mut metrics = Vec::with_capacity(sink.metrics.len());
    for (index, metric) in sink.metrics.iter().enumerate() {
        let path = format!("statistics metric[{index}]");
        let kind = metric.kind.as_ref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_collection(
                ProtocolErrorKind::MissingField,
                format!("{path} requires a kind"),
            )
        })?;
        let column = |value: &String, name: &'static str| {
            (!value.is_empty())
                .then(|| value.clone().into())
                .ok_or_else(|| {
                    NativeFragmentLeafDecodeError::at_collection(
                        ProtocolErrorKind::InvalidValue,
                        format!("{path} {name} must not be empty"),
                    )
                })
        };
        metrics.push(match kind {
            plan::statistics_metric::Kind::RowCount(true) => StatisticsMetric::RowCount,
            plan::statistics_metric::Kind::RowCount(false) => {
                return Err(NativeFragmentLeafDecodeError::at_collection(
                    ProtocolErrorKind::InvalidValue,
                    format!("{path} row_count marker must be true"),
                ));
            }
            plan::statistics_metric::Kind::NullCountColumn(value) => StatisticsMetric::NullCount {
                column: column(value, "null_count_column")?,
            },
            plan::statistics_metric::Kind::MinimumColumn(value) => StatisticsMetric::Minimum {
                column: column(value, "minimum_column")?,
            },
            plan::statistics_metric::Kind::MaximumColumn(value) => StatisticsMetric::Maximum {
                column: column(value, "maximum_column")?,
            },
            plan::statistics_metric::Kind::AverageSizeColumn(value) => {
                StatisticsMetric::AverageSize {
                    column: column(value, "average_size_column")?,
                }
            }
            plan::statistics_metric::Kind::ThetaNdvColumn(value) => StatisticsMetric::ThetaNdv {
                column: column(value, "theta_ndv_column")?,
            },
        });
    }
    StatisticsMetricRequest::try_new(metrics)
        .map(crate::exec::fragment::sink::StatisticsSinkProgram::new)
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_collection(ProtocolErrorKind::InvalidValue, error)
        })
}

fn decode_connector_write_sink_program(
    sink: &plan::ConnectorWriteFragmentSink,
    fragment: &plan::PlanFragment,
    context: &super::NativePlanDecodeContext,
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
    if fragment_instance_id != unique_id_bytes(context_finst.hi, context_finst.lo) {
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
    if execution_id.query_id() != unique_id_bytes(query_id.hi(), query_id.lo()) {
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
            Arc::new(NativeWriterCancellation { query_id }),
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
    instance: &novarocks::InstanceParams,
) -> Result<FragmentSinkAssignment, super::NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let kind = sink.kind.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::DataStream(_) => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: super::instance::decode_destinations(&instance.destinations)?,
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
                        super::NativeFragmentDecodeError::missing(
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
        | plan::data_sink::Kind::Statistics(_)
        | plan::data_sink::Kind::ConnectorWrite(_) => {
            if instance.destinations.is_empty() {
                Ok(FragmentSinkAssignment::None)
            } else {
                Ok(FragmentSinkAssignment::StreamDestinations {
                    destinations: super::instance::decode_destinations(&instance.destinations)?,
                    sender_id: None,
                })
            }
        }
    }
}

fn decode_data_stream_branch(
    stream: &plan::DataStreamSink,
    partition_arena: &mut ExprArena,
    layout: &super::layout::Layout,
    label: &str,
    context: Option<&NativePlanDecodeContext>,
) -> Result<DataStreamSinkBranchProgram, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<DataStreamSinkBranchProgram, NativeFragmentLeafDecodeError> {
        let partition = stream.output_partition.as_ref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "output_partition",
                format!("{label} missing output_partition"),
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
                        context,
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
        let output_columns = decode_output_slot_ids(&stream.output_columns, label)?;
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
    layout: &super::layout::Layout,
    context: Option<&NativePlanDecodeContext>,
) -> Result<SplitDataStreamSinkProgram, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<SplitDataStreamSinkProgram, NativeFragmentLeafDecodeError> {
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
                        ).map_err(|error| {
                            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "exprs", format!(
                                "native CHANGE_STREAM_ROUTER_SINK branch[{index}] partition expr[{expr_index}]: {error}"
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
            Ok((
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
        SplitDataStreamSinkProgram::try_new(streams, split_exprs, partition_arena).map_err(
            |error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "branches",
                    error,
                )
            },
        )
    })();
    decoded
}

fn decode_sink_expression(
    expression: &expr::Expr,
    arena: &mut ExprArena,
    layout: &super::layout::Layout,
    context: Option<&NativePlanDecodeContext>,
    path: FieldPath,
) -> Result<crate::exec::expr::ExprId, super::NativeFragmentDecodeError> {
    let context = context.ok_or_else(|| {
        super::NativeFragmentDecodeError::unsupported(
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
            .enumerate()
            .map(|(index, ordinal)| {
                let raw_slot_id = output_slot_id_for_ordinal(output_columns, *ordinal, field)
                    .map_err(|error| error.append_index(index))?;
                let slot_id = SlotId::try_from(raw_slot_id).map_err(|error| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidValue,
                        field,
                        error,
                    )
                    .append_index(index)
                })?;
                if !seen.insert(slot_id) {
                    return Err(NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InconsistentFields,
                        field,
                        format!(
                            "native CHANGE_STREAM_ROUTER_SINK duplicate output slot id: {slot_id}"
                        ),
                    )
                    .append_index(index));
                }
                Ok(slot_id)
            })
            .collect()
    })();
    decoded
}

fn decode_change_stream_branch_kind(
    value: i32,
) -> Result<crate::sql::common::ChangeStreamBranchKind, String> {
    match plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(crate::sql::common::ChangeStreamBranchKind::DeleteDv)
        }
        plan::ChangeStreamBranchKind::ReuseData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::ReuseData)
        }
        plan::ChangeStreamBranchKind::FreshData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::FreshData)
        }
        plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}

fn decode_stream_destination_list(
    group: &plan::StreamDestinationList,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, super::NativeFragmentDecodeError> {
    group
        .destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let destination_path = path.clone().field("destinations").index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    destination_path.clone().field("finst_id"),
                    "native stream destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                crate::common::types::UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                },
                RuntimeEndpoint::parse(&destination.endpoint).map_err(|error| {
                    super::NativeFragmentDecodeError::invalid_value(
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
    use arrow::datatypes::DataType;

    use super::*;
    use crate::protocol::native::type_mapping::encode_type;

    fn plan_destination(id: i64) -> plan::StreamDestination {
        plan::StreamDestination {
            finst_id: Some(common::UniqueId { hi: 1, lo: id }),
            endpoint: "127.0.0.1:8060".to_string(),
        }
    }

    fn instance_destination(id: i64) -> novarocks::Destination {
        novarocks::Destination {
            finst_id: Some(common::UniqueId { hi: 2, lo: id }),
            endpoint: "127.0.0.1:8061".to_string(),
        }
    }

    fn assert_single_destination_group(assignment: FragmentSinkAssignment, expected_lo: i64) {
        let FragmentSinkAssignment::DestinationGroups { groups, sender_id } = assignment else {
            panic!("expected destination groups");
        };
        assert_eq!(sender_id, None);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 1);
        assert_eq!(groups[0][0].finst_id().lo, expected_lo);
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
        let instance = novarocks::InstanceParams {
            destinations: vec![instance_destination(99)],
            ..Default::default()
        };

        let assignment = decode_fragment_sink_assignment(&sink, &instance)
            .expect("redundant flat destinations must remain wire compatible");

        assert_single_destination_group(assignment, 11);
    }

    #[test]
    fn stream_assignment_preserves_instance_destination_field_path() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(
                plan::DataStreamSink::default(),
            )),
        };
        let instance = novarocks::InstanceParams {
            destinations: vec![novarocks::Destination::default()],
            ..Default::default()
        };

        let error = decode_fragment_sink_assignment(&sink, &instance)
            .expect_err("missing destination finst id must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "instance_params.destinations[0].finst_id"
        );
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::MissingField
        );
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

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
                .expect_err("missing stream partition must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.data_stream.output_partition"
        );
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::MissingField
        );
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

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
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

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
                .expect_err("duplicate output column must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.data_stream.output_columns[1]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn connector_write_missing_handle_fails_closed() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::ConnectorWrite(
                    plan::ConnectorWriteFragmentSink::default(),
                )),
            }),
            ..Default::default()
        };

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
                .expect_err("connector carrier must require a bounded writer handle");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.connector_write.handle"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn router_branch_kind_uses_exact_indexed_path() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                    plan::ChangeStreamRouterSink {
                        branches: vec![plan::ChangeStreamBranchRoute {
                            branch_kind: plan::ChangeStreamBranchKind::Unspecified as i32,
                            output_ordinals: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }),
            output_columns: vec![common::OutputColumn {
                column_id: 1,
                name: "change_op".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
                .expect_err("unspecified branch kind must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.branches[0].branch_kind"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn router_output_ordinal_uses_exact_indexed_path() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                    plan::ChangeStreamRouterSink {
                        branches: vec![plan::ChangeStreamBranchRoute {
                            branch_kind: plan::ChangeStreamBranchKind::FreshData as i32,
                            output_ordinals: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }),
            output_columns: vec![common::OutputColumn {
                column_id: 1,
                name: "change_op".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
                .expect_err("out-of-range branch output ordinal must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.branches[0].output_ordinals[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn router_partition_ordinal_uses_exact_indexed_path() {
        let fragment = plan::PlanFragment {
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                    plan::ChangeStreamRouterSink {
                        branches: vec![plan::ChangeStreamBranchRoute {
                            branch_kind: plan::ChangeStreamBranchKind::FreshData as i32,
                            output_ordinals: vec![0],
                            output_partition_ordinals: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }),
            output_columns: vec![common::OutputColumn {
                column_id: 1,
                name: "change_op".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let error =
            decode_fragment_sink_program(&fragment, &super::super::layout::Layout::default())
                .expect_err("out-of-range partition ordinal must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.change_stream_router.branches[0].output_partition_ordinals[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn router_assignment_ignores_redundant_flat_instance_destinations() {
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::ChangeStreamRouter(
                plan::ChangeStreamRouterSink {
                    branches: vec![plan::ChangeStreamBranchRoute {
                        destinations: Some(plan::StreamDestinationList {
                            destinations: vec![plan_destination(12)],
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        };
        let instance = novarocks::InstanceParams {
            destinations: vec![instance_destination(98)],
            ..Default::default()
        };

        let assignment = decode_fragment_sink_assignment(&sink, &instance)
            .expect("redundant flat destinations must remain wire compatible");

        assert_single_destination_group(assignment, 12);
    }

    #[test]
    fn router_branch_rejects_duplicate_output_slots() {
        let output_columns = vec![common::OutputColumn {
            column_id: 7,
            name: "value".to_string(),
            ..Default::default()
        }];

        let error = decode_router_output_slots(&[0, 0], &output_columns, "branch[0] output")
            .expect_err("duplicate router output slots must be rejected during decode");

        assert_eq!(
            error,
            "native CHANGE_STREAM_ROUTER_SINK duplicate output slot id: 7"
        );
    }
}
