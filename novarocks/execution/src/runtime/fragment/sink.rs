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

use super::{ExecutionFailure, sink_failure};
use crate::exec::fragment::sink::DataStreamSinkFactoryInput;
use crate::exec::operators::{CompiledPartitionKeys, DataStreamSinkFactory, NoopSinkFactory};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::FragmentSinkAssignment;
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates;
use novarocks_local_program::{StaticSinkProgram, StaticStreamBranch};

/// Materialize the root sink of one compiled LocalProgram (local-compiler
/// output).
///
/// Partition keys are frozen compiled roots. Task-local destinations and
/// outbound edge gates are bound without constructing an expression arena.
pub(crate) fn materialize_compiled_sink(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_sink: Option<Box<dyn OperatorFactory>>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    let graph = program.graph();
    let sink = graph
        .sink()
        .ok_or_else(|| materialization_contract("compiled local program has no static sink"))?;
    if !matches!(
        sink,
        StaticSinkProgram::Result | StaticSinkProgram::RootResult(_)
    ) && result_sink.is_some()
    {
        return Err(materialization_contract(format!(
            "compiled {} sink cannot take a result sink",
            sink_program_name(sink)
        )));
    }
    match (sink, assignment) {
        (
            StaticSinkProgram::Result | StaticSinkProgram::RootResult(_),
            FragmentSinkAssignment::None,
        ) => result_sink
            .ok_or_else(|| materialization_contract("compiled RESULT_SINK requires a result sink")),
        (StaticSinkProgram::Noop, FragmentSinkAssignment::None) => {
            Ok(Box::new(NoopSinkFactory::new()))
        }
        (
            StaticSinkProgram::DataStream { branch, .. },
            FragmentSinkAssignment::StreamDestinations {
                destinations,
                sender_id,
            },
        ) => materialize_stream_branch(
            program,
            branch,
            0,
            destinations,
            *sender_id,
            fragment_instance_id,
            transmitter,
            edge_gates,
        ),
        (StaticSinkProgram::ChangeStreamRouter { .. }, _) => Err(materialization_contract(
            "compiled change router requires computing-pipeline geometry and its independent summary output",
        )),
        (
            StaticSinkProgram::MultiCastDataStream { .. }
            | StaticSinkProgram::SplitDataStream { .. },
            _,
        ) => Err(materialization_contract(format!(
            "compiled {} sink not executable yet",
            sink_program_name(sink)
        ))),
        (static_program, dynamic_assignment) => Err(materialization_contract(format!(
            "compiled sink {} cannot be materialized with assignment {}",
            sink_program_name(static_program),
            sink_assignment_name(dynamic_assignment)
        ))),
    }
}

/// A root sink and its optional independent, complete summary pipeline.
/// All factories join the original Task driver batch before activation.
pub(crate) struct MaterializedCompiledSink {
    pub(crate) root: Box<dyn OperatorFactory>,
    pub(crate) summary: Option<IndependentSummaryPipeline>,
}
pub(crate) struct IndependentSummaryPipeline {
    pub(crate) source: Box<dyn OperatorFactory>,
    pub(crate) sink: Box<dyn OperatorFactory>,
}
impl MaterializedCompiledSink {
    pub(crate) fn single(root: Box<dyn OperatorFactory>) -> Self {
        Self {
            root,
            summary: None,
        }
    }
}

fn materialize_stream_branch(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    branch: &StaticStreamBranch,
    branch_index: u32,
    destinations: &[crate::runtime::endpoint::FragmentDestination],
    sender_id: Option<i32>,
    fragment_instance_id: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    let partition_keys = compiled_partition_keys(program, branch, branch_index)?;
    if let Some(limit) = branch.limit() {
        return Err(materialization_contract(format!(
            "compiled stream sink limit {limit} is not executable yet"
        )));
    }
    let plan_node_id = sink_failure::root_identity(program.graph().root().index())
        .map_err(materialization_error)?;
    let input = DataStreamSinkFactoryInput::try_from_static_program(
        branch.dest_node_id(),
        branch.partition_type(),
        branch.contract().delivery,
        branch.output_columns().to_vec(),
        destinations.to_vec(),
    )
    .map_err(materialization_error)?;
    let factory = DataStreamSinkFactory::new(
        input,
        fragment_instance_id,
        sender_id,
        plan_node_id,
        transmitter,
    );
    let factory = match partition_keys {
        Some(keys) => factory
            .with_compiled_partition_keys(keys)
            .map_err(materialization_error)?,
        None => factory,
    };
    let factory = match edge_gates {
        Some(gates) => factory.with_edge_gates(gates),
        None => factory,
    };
    Ok(Box::new(factory))
}

/// The computing graph has already authored its exact root width. The
/// summary contributor set is frozen once here, before any driver exists.
pub(crate) fn materialize_compiled_sink_for_pipeline(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_sink: Option<Box<dyn OperatorFactory>>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
    actual_root_dop: std::num::NonZeroUsize,
    tracker: Option<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>>,
) -> Result<MaterializedCompiledSink, FragmentLaunchError> {
    use crate::exec::operators::{
        compiled_change_router::CompiledChangeRouterFactory,
        logical_change_summary::{LogicalSummarySourceFactory, LogicalSummaryState},
    };
    use std::sync::Arc;
    let Some(StaticSinkProgram::ChangeStreamRouter {
        branches,
        effect_slot,
        routes,
        summary_branch,
        summary_layout,
        ..
    }) = program.graph().sink()
    else {
        return materialize_compiled_sink(
            program,
            assignment,
            fragment_instance_id,
            transmitter,
            result_sink,
            edge_gates,
        )
        .map(MaterializedCompiledSink::single);
    };
    if result_sink.is_some() {
        return Err(materialization_contract(
            "compiled change router cannot take a result sink",
        ));
    }
    let FragmentSinkAssignment::DestinationGroups { groups, sender_id } = assignment else {
        return Err(materialization_contract(
            "compiled change router requires its exact destination groups",
        ));
    };
    if groups.len() != branches.len() {
        return Err(materialization_contract(
            "compiled change router destination count differs from its branch set",
        ));
    }
    let tracker = tracker.ok_or_else(|| {
        materialization_contract("logical summary has no original Task memory tracker")
    })?;
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_child("logical_change_summary", &tracker);
    let state =
        LogicalSummaryState::new(actual_root_dop.get(), tracker).map_err(materialization_error)?;
    let mut provider_factories = Vec::with_capacity(routes.len());
    let mut dense_routes = Vec::with_capacity(routes.len());
    for (dense, route) in routes.iter().enumerate() {
        let index = usize::try_from(route.branch)
            .map_err(|_| materialization_contract("router branch index exceeds usize"))?;
        let branch = branches
            .get(index)
            .ok_or_else(|| materialization_contract("router provider branch is absent"))?;
        let destinations = groups
            .get(index)
            .ok_or_else(|| materialization_contract("router provider assignment is absent"))?;
        provider_factories.push(Arc::from(materialize_stream_branch(
            program,
            branch,
            route.branch,
            destinations,
            *sender_id,
            fragment_instance_id,
            Arc::clone(&transmitter),
            edge_gates.clone(),
        )?));
        let mut dense_route = route.clone();
        dense_route.branch = u32::try_from(dense)
            .map_err(|_| materialization_contract("router provider count exceeds u32"))?;
        dense_routes.push(dense_route);
    }
    let index = usize::try_from(*summary_branch)
        .map_err(|_| materialization_contract("summary branch index exceeds usize"))?;
    let branch = branches
        .get(index)
        .ok_or_else(|| materialization_contract("router summary branch is absent"))?;
    let destinations = groups
        .get(index)
        .ok_or_else(|| materialization_contract("router summary assignment is absent"))?;
    let summary_sink = materialize_stream_branch(
        program,
        branch,
        *summary_branch,
        destinations,
        *sender_id,
        fragment_instance_id,
        transmitter,
        edge_gates,
    )?;
    let summary_source = LogicalSummarySourceFactory::new(state.clone(), summary_layout.clone())
        .map_err(materialization_error)?;
    let root = CompiledChangeRouterFactory::new(
        *effect_slot,
        dense_routes.into(),
        provider_factories,
        state,
    )
    .map_err(materialization_error)?;
    Ok(MaterializedCompiledSink {
        root: Box::new(root),
        summary: Some(IndependentSummaryPipeline {
            source: Box::new(summary_source),
            sink: summary_sink,
        }),
    })
}

/// The compiled partition-key roots of the one DataStream branch, in key
/// order. A partitioning that evaluates no key must carry none.
fn compiled_partition_keys(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    branch: &StaticStreamBranch,
    branch_index: u32,
) -> Result<Option<CompiledPartitionKeys>, FragmentLaunchError> {
    let keys = branch.partition_exprs().len();
    if !branch.partition_type().requires_exprs() {
        if keys != 0 {
            return Err(materialization_contract(format!(
                "compiled {} stream sink cannot carry partition keys",
                branch.partition_type().display_name()
            )));
        }
        return Ok(None);
    }
    if keys == 0 {
        return Err(materialization_contract(format!(
            "compiled {} stream sink has no partition key",
            branch.partition_type().display_name()
        )));
    }
    let mut sites = Vec::with_capacity(keys);
    for key in 0..keys {
        let key = sink_failure::key_identity(key).map_err(materialization_error)?;
        sites.push(
            novarocks_local_program::ProgramExpressionRootSite::SinkPartition {
                branch: branch_index,
                key,
            },
        );
    }
    use novarocks_local_program::{ProgramExpressionRootSite, StaticStreamRouting};
    let program = std::sync::Arc::clone(program);
    Ok(Some(match branch.contract().routing {
        StaticStreamRouting::Ordinary => CompiledPartitionKeys::new(program, sites),
        StaticStreamRouting::HierarchicalHashV1 => {
            CompiledPartitionKeys::hierarchical(program, sites)
        }
        StaticStreamRouting::ConditionalKeyV1 { .. } => CompiledPartitionKeys::conditional(
            program,
            sites,
            ProgramExpressionRootSite::SinkApplicability {
                branch: branch_index,
            },
        ),
    }))
}
fn materialization_error(source: impl Into<ExecutionFailure>) -> FragmentLaunchError {
    FragmentLaunchError::from_failure(
        FragmentLaunchStage::Materialize,
        FragmentLaunchErrorKind::Materialization,
        source.into(),
    )
}

fn materialization_contract(detail: impl std::fmt::Display) -> FragmentLaunchError {
    materialization_error(sink_failure::contract(detail))
}

fn sink_program_name(program: &StaticSinkProgram) -> &'static str {
    match program {
        StaticSinkProgram::Result => "result",
        StaticSinkProgram::RootResult(_) => "root_result",
        StaticSinkProgram::Noop => "noop",
        StaticSinkProgram::DataStream { .. } => "data_stream",
        StaticSinkProgram::MultiCastDataStream { .. } => "multi_cast_data_stream",
        StaticSinkProgram::SplitDataStream { .. } => "split_data_stream",
        StaticSinkProgram::ChangeStreamRouter { .. } => "change_stream_router",
    }
}

fn sink_assignment_name(assignment: &FragmentSinkAssignment) -> &'static str {
    match assignment {
        FragmentSinkAssignment::None => "none",
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations",
        FragmentSinkAssignment::DestinationGroups { .. } => "destination_groups",
    }
}
