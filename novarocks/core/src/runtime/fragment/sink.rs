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

use crate::exec::fragment::program::{FragmentProgram, FragmentSinkSpec};
use crate::exec::fragment::sink::{
    DataStreamSinkBranchProgram, DataStreamSinkFactoryInput, FragmentSinkProgram,
    MultiCastDataStreamSinkProgram,
};
use crate::exec::operators::{
    ConnectorWriteSinkFactory, DataStreamSinkFactory, MultiCastDataStreamSinkFactory,
    NoopSinkFactory, ResultBufferSinkFactory, SplitDataStreamSinkFactory,
};
use crate::exec::operators::{StatisticsSinkFactory, StatisticsSinkHandle};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::{FragmentInstanceSpec, FragmentSinkAssignment};
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use crate::runtime::fragment::io::FragmentResultSession;

pub(crate) fn materialize_fragment_sink(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_session: Option<std::sync::Arc<dyn FragmentResultSession>>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    materialize_fragment_sink_with_result(program, instance, transmitter, result_session)
        .map(|materialized| materialized.factory)
}

/// Runtime-owned result of materializing a fragment sink. Statistics is the
/// only sink that has a terminal side-channel; its handle is process-local and
/// consumed exactly once by the fragment host when it freezes the terminal
/// fact.
pub(crate) struct MaterializedFragmentSink {
    pub(crate) factory: Box<dyn OperatorFactory>,
    pub(crate) statistics_handle: Option<StatisticsSinkHandle>,
}

pub(crate) fn materialize_fragment_sink_with_result(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_session: Option<std::sync::Arc<dyn FragmentResultSession>>,
) -> Result<MaterializedFragmentSink, FragmentLaunchError> {
    materialize_fragment_sink_components_with_result_and_statistics(
        program.sink(),
        Some(program.plan()),
        instance.sink_assignment(),
        instance.fragment_instance_id().get(),
        instance.runtime_options().typed_result_sink(),
        program.root_plan_node_id().get(),
        transmitter,
        result_session,
    )
}

pub(crate) fn materialize_fragment_sink_components(
    program: &FragmentSinkSpec,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: crate::common::types::UniqueId,
    typed_result_sink: bool,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_session: Option<std::sync::Arc<dyn FragmentResultSession>>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    materialize_fragment_sink_components_with_result(
        program,
        assignment,
        fragment_instance_id,
        typed_result_sink,
        plan_node_id,
        transmitter,
        result_session,
    )
}

pub(crate) fn materialize_fragment_sink_components_with_result(
    program: &FragmentSinkSpec,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: crate::common::types::UniqueId,
    typed_result_sink: bool,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_session: Option<std::sync::Arc<dyn FragmentResultSession>>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    materialize_fragment_sink_components_with_result_and_statistics(
        program,
        None,
        assignment,
        fragment_instance_id,
        typed_result_sink,
        plan_node_id,
        transmitter,
        result_session,
    )
    .map(|materialized| materialized.factory)
}

fn materialize_fragment_sink_components_with_result_and_statistics(
    program: &FragmentSinkSpec,
    root_plan: Option<&crate::exec::node::ExecPlan>,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: crate::common::types::UniqueId,
    _typed_result_sink: bool,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_session: Option<std::sync::Arc<dyn FragmentResultSession>>,
) -> Result<MaterializedFragmentSink, FragmentLaunchError> {
    match (program.program(), assignment) {
        (FragmentSinkProgram::Result, FragmentSinkAssignment::None) => {
            let session = result_session.ok_or_else(|| {
                materialization_error("RESULT_SINK requires an opened Fragment result session")
            })?;
            Ok(MaterializedFragmentSink {
                factory: Box::new(ResultBufferSinkFactory::new(session, None)),
                statistics_handle: None,
            })
        }
        (FragmentSinkProgram::Noop, FragmentSinkAssignment::None) => Ok(MaterializedFragmentSink {
            factory: Box::new(NoopSinkFactory::new()),
            statistics_handle: None,
        }),
        (FragmentSinkProgram::Statistics(statistics), FragmentSinkAssignment::None) => {
            let root_plan = root_plan.ok_or_else(|| {
                materialization_error("STATISTICS_SINK requires a fragment root plan")
            })?;
            let schema =
                crate::exec::pipeline::builder::output_chunk_schema_for_node(&root_plan.root)
                    .ok_or_else(|| {
                        materialization_error("STATISTICS_SINK requires a root output schema")
                    })?;
            let (factory, statistics_handle) = StatisticsSinkFactory::try_new(
                schema.arrow_schema_ref(),
                statistics.metrics().clone(),
                Some(plan_node_id),
            )
            .map_err(materialization_error)?;
            Ok(MaterializedFragmentSink {
                factory: Box::new(factory),
                statistics_handle: Some(statistics_handle),
            })
        }
        (
            FragmentSinkProgram::DataStream(stream),
            FragmentSinkAssignment::StreamDestinations {
                destinations,
                sender_id,
            },
        ) => {
            let input = stream_input(stream, destinations.clone())?;
            Ok(MaterializedFragmentSink {
                factory: Box::new(DataStreamSinkFactory::new(
                    input,
                    fragment_instance_id,
                    *sender_id,
                    plan_node_id,
                    stream.partition_arena().clone(),
                    std::sync::Arc::clone(&transmitter),
                )),
                statistics_handle: None,
            })
        }
        (
            FragmentSinkProgram::MultiCastDataStream(grouped),
            FragmentSinkAssignment::DestinationGroups { groups, sender_id },
        ) => materialize_multicast(
            grouped,
            groups,
            fragment_instance_id,
            *sender_id,
            plan_node_id,
            std::sync::Arc::clone(&transmitter),
        )
        .map(|factory| MaterializedFragmentSink {
            factory,
            statistics_handle: None,
        }),
        (
            FragmentSinkProgram::SplitDataStream(split),
            FragmentSinkAssignment::DestinationGroups { groups, sender_id },
        ) => materialize_split(
            split,
            groups,
            fragment_instance_id,
            *sender_id,
            plan_node_id,
            std::sync::Arc::clone(&transmitter),
        )
        .map(|factory| MaterializedFragmentSink {
            factory,
            statistics_handle: None,
        }),
        (FragmentSinkProgram::ConnectorWrite(connector), FragmentSinkAssignment::None) => {
            ConnectorWriteSinkFactory::try_new(connector)
                .map(|factory| MaterializedFragmentSink {
                    factory: Box::new(factory) as Box<dyn OperatorFactory>,
                    statistics_handle: None,
                })
                .map_err(materialization_error)
        }
        (static_program, dynamic_assignment) => Err(materialization_error(format!(
            "sink {} cannot be materialized with assignment {}",
            sink_program_name(static_program),
            sink_assignment_name(dynamic_assignment)
        ))),
    }
}

fn materialize_multicast(
    program: &MultiCastDataStreamSinkProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    ensure_group_count(program.sinks().len(), groups.len())?;
    let sinks = program
        .sinks()
        .iter()
        .zip(groups)
        .map(|(stream, destinations)| {
            Ok((branch_input(stream, destinations.clone())?, stream.limit()))
        })
        .collect::<Result<Vec<_>, FragmentLaunchError>>()?;
    Ok(Box::new(MultiCastDataStreamSinkFactory::new(
        sinks,
        fragment_instance_id,
        sender_id,
        program.partition_arena().clone(),
        plan_node_id,
        transmitter,
    )))
}

fn materialize_split(
    program: &crate::exec::fragment::sink::SplitDataStreamSinkProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    let sinks = program
        .sinks()
        .iter()
        .zip(groups)
        .map(|(stream, destinations)| branch_input(stream, destinations.clone()))
        .collect::<Result<Vec<_>, FragmentLaunchError>>()?;
    Ok(Box::new(SplitDataStreamSinkFactory::new(
        sinks,
        fragment_instance_id,
        sender_id,
        program.arena().clone(),
        plan_node_id,
        std::sync::Arc::new(program.arena().clone()),
        program.split_exprs().to_vec(),
        transmitter,
    )))
}

fn stream_input(
    program: &crate::exec::fragment::sink::DataStreamSinkProgram,
    destinations: Vec<FragmentDestination>,
) -> Result<DataStreamSinkFactoryInput, FragmentLaunchError> {
    DataStreamSinkFactoryInput::try_from_static_program(
        program.dest_node_id(),
        program.output_partition_type(),
        program.output_exprs().to_vec(),
        program.output_partition_exprs().to_vec(),
        program.output_columns().to_vec(),
        destinations,
    )
    .map_err(materialization_error)
}

fn branch_input(
    program: &DataStreamSinkBranchProgram,
    destinations: Vec<FragmentDestination>,
) -> Result<DataStreamSinkFactoryInput, FragmentLaunchError> {
    DataStreamSinkFactoryInput::try_from_static_program(
        program.dest_node_id(),
        program.output_partition_type(),
        program.output_exprs().to_vec(),
        program.output_partition_exprs().to_vec(),
        program.output_columns().to_vec(),
        destinations,
    )
    .map_err(materialization_error)
}

fn ensure_group_count(expected: usize, actual: usize) -> Result<(), FragmentLaunchError> {
    if expected != actual {
        return Err(materialization_error(format!(
            "expected {expected} destination groups, got {actual}"
        )));
    }
    Ok(())
}

fn materialization_error(detail: impl Into<String>) -> FragmentLaunchError {
    FragmentLaunchError::new(
        FragmentLaunchStage::Materialize,
        FragmentLaunchErrorKind::Materialization,
        detail,
    )
}

fn sink_program_name(program: &FragmentSinkProgram) -> &'static str {
    match program {
        FragmentSinkProgram::Result => "result",
        FragmentSinkProgram::Noop => "noop",
        FragmentSinkProgram::Statistics(_) => "statistics",
        FragmentSinkProgram::DataStream(_) => "data_stream",
        FragmentSinkProgram::MultiCastDataStream(_) => "multi_cast_data_stream",
        FragmentSinkProgram::SplitDataStream(_) => "split_data_stream",
        FragmentSinkProgram::ConnectorWrite(_) => "connector_write",
    }
}

fn sink_assignment_name(assignment: &FragmentSinkAssignment) -> &'static str {
    match assignment {
        FragmentSinkAssignment::None => "none",
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations",
        FragmentSinkAssignment::DestinationGroups { .. } => "destination_groups",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkSpec,
        RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::DataStreamPartitionType;
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::fragment::error::{FragmentLaunchErrorKind, FragmentLaunchStage};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::io::ExchangeFrameTransmitter;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;

    use super::{materialize_fragment_sink, materialize_fragment_sink_components};

    fn test_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter> {
        crate::runtime::fragment::io::exchange::discard_exchange_transmitter()
    }

    fn stream_program() -> DataStreamSinkProgram {
        DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            None,
            ExprArena::default(),
        )
        .expect("stream program")
    }

    fn stream_branch(dest_node_id: i32) -> DataStreamSinkBranchProgram {
        DataStreamSinkBranchProgram::try_new(
            dest_node_id,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            None,
        )
        .expect("stream branch")
    }

    fn instance(sink_assignment: FragmentSinkAssignment) -> FragmentInstanceSpec {
        FragmentInstanceSpec::new_native(
            crate::exec::fragment::program::FragmentContractVersion::CURRENT,
            QueryId::new(1, 2),
            FragmentInstanceId::new(UniqueId::new(3, 4)),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            sink_assignment,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(0).expect("backend number"),
        )
    }

    fn fragment_program(sink: FragmentSinkProgram) -> FragmentProgram {
        FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: Chunk::default(),
                        node_id: 99,
                    }),
                },
            },
            FragmentSinkSpec::try_new(sink).expect("fragment sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::default(),
        )
    }

    fn assert_factory_and_operator_name(
        factory: &dyn crate::exec::pipeline::operator_factory::OperatorFactory,
        expected: &str,
    ) {
        assert_eq!(factory.name(), expected);
        assert_eq!(factory.create(1, 0).name(), expected);
    }

    #[test]
    fn data_stream_materialization_uses_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::DataStream(stream_program()));
        let instance = instance(FragmentSinkAssignment::StreamDestinations {
            destinations: Vec::new(),
            sender_id: None,
        });

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter(), None)
            .expect("data stream sink");

        assert_factory_and_operator_name(factory.as_ref(), "EXCHANGE_SINK (id=99)");
    }

    #[test]
    fn multicast_materialization_uses_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(vec![stream_branch(17)], ExprArena::default())
                .expect("multicast program"),
        ));
        let instance = instance(FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        });

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter(), None)
            .expect("multicast sink");

        assert_factory_and_operator_name(factory.as_ref(), "MULTI_CAST_DATA_STREAM_SINK (id=99)");
    }

    #[test]
    fn split_materialization_is_owned_by_fragment_sink_materializer() {
        let mut arena = ExprArena::default();
        let split_expr = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Bool(true)),
            arrow::datatypes::DataType::Boolean,
        );
        let program = fragment_program(FragmentSinkProgram::SplitDataStream(
            SplitDataStreamSinkProgram::try_new(vec![stream_branch(17)], vec![split_expr], arena)
                .expect("split program"),
        ));
        let instance = instance(FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        });

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter(), None)
            .expect("split sink");

        assert_factory_and_operator_name(factory.as_ref(), "SPLIT_DATA_STREAM_SINK (id=99)");
    }

    #[test]
    fn result_materialization_remains_anonymous_with_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::Result);
        let instance = instance(FragmentSinkAssignment::None);

        let factory = materialize_fragment_sink(
            &program,
            &instance,
            test_transmitter(),
            Some(crate::runtime::fragment::io::result::discard_result_session()),
        )
        .expect("result sink");

        assert_factory_and_operator_name(factory.as_ref(), "RESULT_BUFFER_SINK (plan_node_id=-1)");
    }

    #[test]
    fn data_stream_materialization_requires_stream_destinations() {
        let program = fragment_program(FragmentSinkProgram::DataStream(stream_program()));

        let error = match materialize_fragment_sink(
            &program,
            &instance(FragmentSinkAssignment::None),
            test_transmitter(),
            None,
        ) {
            Ok(_) => panic!("missing stream destinations must fail"),
            Err(error) => error,
        };

        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
    }

    #[test]
    fn grouped_materialization_rejects_mismatched_destination_count() {
        let program = fragment_program(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(
                vec![stream_branch(17), stream_branch(18)],
                ExprArena::default(),
            )
            .expect("multicast program"),
        ));
        let assignment = FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        };

        let error = match materialize_fragment_sink(
            &program,
            &instance(assignment),
            test_transmitter(),
            None,
        ) {
            Ok(_) => panic!("one destination group must not be truncated against two branches"),
            Err(error) => error,
        };

        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
        assert!(error.detail().contains("expected 2 destination groups"));
    }

    #[test]
    fn result_materialization_keeps_bridge_plan_node_unset() {
        let sink = FragmentSinkSpec::try_new(FragmentSinkProgram::Result).expect("result sink");

        let factory = materialize_fragment_sink_components(
            &sink,
            &FragmentSinkAssignment::None,
            UniqueId::new(3, 4),
            false,
            47,
            test_transmitter(),
            Some(crate::runtime::fragment::io::result::discard_result_session()),
        )
        .expect("result sink materialization");

        assert_eq!(factory.name(), "RESULT_BUFFER_SINK (plan_node_id=-1)");
    }
}
