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
use crate::runtime::task::error::{
    LocalTaskLaunchError, LocalTaskLaunchErrorKind, LocalTaskLaunchStage,
};
use crate::runtime::task::instance::TaskSinkAssignment;
use crate::runtime::task::io::ExchangeFrameTransmitter;
use crate::runtime::task::io::exchange_edge::ExchangeEdgeGates;
use novarocks_local_program::{StaticSinkProgram, StaticStreamBranch};

/// Materialize the root sink of one compiled LocalProgram (local-compiler
/// output).
///
/// Partition keys are frozen compiled roots. Task-local destinations and
/// outbound edge gates are bound without constructing an expression arena.
pub(crate) fn materialize_compiled_sink(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    assignment: &TaskSinkAssignment,
    task_key: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_sink: Option<Box<dyn OperatorFactory>>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
) -> Result<Box<dyn OperatorFactory>, LocalTaskLaunchError> {
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
            TaskSinkAssignment::None,
        ) => result_sink
            .ok_or_else(|| materialization_contract("compiled RESULT_SINK requires a result sink")),
        (StaticSinkProgram::Noop, TaskSinkAssignment::None) => Ok(Box::new(NoopSinkFactory::new())),
        (
            StaticSinkProgram::DataStream { branch, .. },
            TaskSinkAssignment::StreamDestinations {
                destinations,
                sender_id,
            },
        ) => materialize_stream_branch(
            program,
            graph.nodes()[graph.root().index()].output_layout(),
            branch,
            0,
            destinations,
            *sender_id,
            task_key,
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
    pub(crate) summaries: Vec<IndependentSummaryPipeline>,
}
pub(crate) struct IndependentSummaryPipeline {
    pub(crate) source: Box<dyn OperatorFactory>,
    pub(crate) sink: Box<dyn OperatorFactory>,
}
impl MaterializedCompiledSink {
    pub(crate) fn single(root: Box<dyn OperatorFactory>) -> Self {
        Self {
            root,
            summaries: Vec::new(),
        }
    }
}

fn materialize_stream_branch(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    output_layout: &novarocks_local_program::StaticLayout,
    branch: &StaticStreamBranch,
    branch_index: u32,
    destinations: &[crate::runtime::endpoint::TaskDestination],
    sender_id: Option<i32>,
    task_key: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
) -> Result<Box<dyn OperatorFactory>, LocalTaskLaunchError> {
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
    let factory = DataStreamSinkFactory::new(input, task_key, sender_id, plan_node_id, transmitter)
        .with_frozen_output(output_layout);
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

/// Borrow the original assignment. Side outputs do not clone destination groups
/// or acquire a new routing authority.
struct AssignedStreams<'a> {
    assignment: &'a TaskSinkAssignment,
}
impl AssignedStreams<'_> {
    fn len(&self) -> usize {
        match self.assignment {
            TaskSinkAssignment::None => 0,
            TaskSinkAssignment::StreamDestinations { .. } => 1,
            TaskSinkAssignment::DestinationGroups { groups, .. } => groups.len(),
        }
    }
    fn get(&self, index: usize) -> Option<&[crate::runtime::endpoint::TaskDestination]> {
        match self.assignment {
            TaskSinkAssignment::None => None,
            TaskSinkAssignment::StreamDestinations { destinations, .. } => {
                (index == 0).then_some(destinations.as_slice())
            }
            TaskSinkAssignment::DestinationGroups { groups, .. } => {
                groups.get(index).map(Vec::as_slice)
            }
        }
    }
    fn sender_id(&self) -> Option<i32> {
        match self.assignment {
            TaskSinkAssignment::None => None,
            TaskSinkAssignment::StreamDestinations { sender_id, .. }
            | TaskSinkAssignment::DestinationGroups { sender_id, .. } => *sender_id,
        }
    }
}

/// The graph has authored its exact root width. Every author freezes its unit
/// set before any driver exists; every independent Source joins this Task's batch.
pub(crate) fn materialize_compiled_sink_for_pipeline(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    assignment: &TaskSinkAssignment,
    task_key: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_sink: Option<Box<dyn OperatorFactory>>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
    actual_root_dop: std::num::NonZeroUsize,
    tracker: Option<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>>,
    mut authors: crate::exec::operators::logical_change_summary::SummaryAuthorAssemblyObligation,
) -> Result<MaterializedCompiledSink, LocalTaskLaunchError> {
    use crate::exec::operators::{
        compiled_change_router::CompiledChangeRouterFactory,
        logical_change_summary::SummarySourceFactory,
    };
    use novarocks_local_program::StaticSummaryAuthor;
    use novarocks_result_contract::SummaryKind;
    use std::sync::Arc;
    let graph = program.graph();
    let sink = graph
        .sink()
        .ok_or_else(|| materialization_contract("compiled local program has no static sink"))?;
    if graph.summaries().is_empty() && !matches!(sink, StaticSinkProgram::ChangeStreamRouter { .. })
    {
        authors.seal().map_err(materialization_error)?;
        return materialize_compiled_sink(
            program,
            assignment,
            task_key,
            transmitter,
            result_sink,
            edge_gates,
        )
        .map(MaterializedCompiledSink::single);
    }
    let assigned = AssignedStreams { assignment };
    let root_branches = sink.branches().len();
    if assigned.len() != graph.stream_branches().count() {
        return Err(materialization_contract(
            "compiled output assignment differs from its exact branch set",
        ));
    }
    if !matches!(
        sink,
        StaticSinkProgram::Result | StaticSinkProgram::RootResult(_)
    ) && result_sink.is_some()
    {
        return Err(materialization_contract(
            "compiled non-result sink cannot take a result sink",
        ));
    }
    let root_layout = graph.nodes()[graph.root().index()].output_layout();
    let root: Box<dyn OperatorFactory> = match sink {
        StaticSinkProgram::Result | StaticSinkProgram::RootResult(_) => {
            result_sink.ok_or_else(|| {
                materialization_contract("compiled RESULT_SINK requires a result sink")
            })?
        }
        StaticSinkProgram::Noop => Box::new(NoopSinkFactory::new()),
        StaticSinkProgram::DataStream { branch, .. } => materialize_stream_branch(
            program,
            root_layout,
            branch,
            0,
            assigned
                .get(0)
                .ok_or_else(|| materialization_contract("root stream assignment is absent"))?,
            assigned.sender_id(),
            task_key,
            transmitter.clone(),
            edge_gates.clone(),
        )?,
        StaticSinkProgram::ChangeStreamRouter {
            branches,
            effect_slot,
            routes,
            ..
        } => {
            let state =
                if authors.declares(SummaryKind::WriteCounts, StaticSummaryAuthor::RouterSink) {
                    let tracker = tracker.ok_or_else(|| {
                        materialization_contract("summary author has no original Task tracker")
                    })?;
                    Some(
                        authors
                            .claim(SummaryKind::WriteCounts, StaticSummaryAuthor::RouterSink)
                            .and_then(|obligation| {
                                obligation.declare_units(actual_root_dop.get(), tracker)
                            })
                            .map_err(materialization_error)?,
                    )
                } else {
                    None
                };
            let mut factories = Vec::with_capacity(routes.len());
            let mut dense_routes = Vec::with_capacity(routes.len());
            for (dense, route) in routes.iter().enumerate() {
                let index = usize::try_from(route.branch)
                    .map_err(|_| materialization_contract("router branch index exceeds usize"))?;
                let branch = branches
                    .get(index)
                    .ok_or_else(|| materialization_contract("router provider branch is absent"))?;
                factories.push(Arc::from(materialize_stream_branch(
                    program,
                    root_layout,
                    branch,
                    route.branch,
                    assigned.get(index).ok_or_else(|| {
                        materialization_contract("router provider assignment is absent")
                    })?,
                    assigned.sender_id(),
                    task_key,
                    transmitter.clone(),
                    edge_gates.clone(),
                )?));
                let mut dense_route = route.clone();
                dense_route.branch = u32::try_from(dense)
                    .map_err(|_| materialization_contract("router provider count exceeds u32"))?;
                dense_routes.push(dense_route);
            }
            Box::new(
                CompiledChangeRouterFactory::new(
                    *effect_slot,
                    dense_routes.into(),
                    factories,
                    state,
                )
                .map_err(materialization_error)?,
            )
        }
        StaticSinkProgram::MultiCastDataStream { .. }
        | StaticSinkProgram::SplitDataStream { .. } => {
            return Err(materialization_contract(
                "compiled multicast or split sink is not executable yet",
            ));
        }
    };
    let states = authors.seal().map_err(materialization_error)?;
    let mut summaries = Vec::with_capacity(graph.summaries().len());
    for (ordinal, output) in graph.summaries().iter().enumerate() {
        let index = root_branches
            .checked_add(ordinal)
            .ok_or_else(|| materialization_contract("summary branch index exceeds usize"))?;
        let state = states.get(output.kind()).ok_or_else(|| {
            materialization_contract("summary author did not register its exact state")
        })?;
        let summary_sink = materialize_stream_branch(
            program,
            output.layout(),
            output.branch(),
            u32::try_from(index)
                .map_err(|_| materialization_contract("summary branch index exceeds u32"))?,
            assigned
                .get(index)
                .ok_or_else(|| materialization_contract("summary assignment is absent"))?,
            assigned.sender_id(),
            task_key,
            transmitter.clone(),
            edge_gates.clone(),
        )?;
        summaries.push(IndependentSummaryPipeline {
            source: Box::new(
                SummarySourceFactory::new(state.clone(), output.layout().clone())
                    .map_err(materialization_error)?,
            ),
            sink: summary_sink,
        });
    }
    Ok(MaterializedCompiledSink { root, summaries })
}

/// The compiled partition-key roots of the one DataStream branch, in key
/// order. A partitioning that evaluates no key must carry none.
fn compiled_partition_keys(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    branch: &StaticStreamBranch,
    branch_index: u32,
) -> Result<Option<CompiledPartitionKeys>, LocalTaskLaunchError> {
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
fn materialization_error(source: impl Into<ExecutionFailure>) -> LocalTaskLaunchError {
    LocalTaskLaunchError::from_failure(
        LocalTaskLaunchStage::Materialize,
        LocalTaskLaunchErrorKind::Materialization,
        source.into(),
    )
}

fn materialization_contract(detail: impl std::fmt::Display) -> LocalTaskLaunchError {
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

fn sink_assignment_name(assignment: &TaskSinkAssignment) -> &'static str {
    match assignment {
        TaskSinkAssignment::None => "none",
        TaskSinkAssignment::StreamDestinations { .. } => "stream_destinations",
        TaskSinkAssignment::DestinationGroups { .. } => "destination_groups",
    }
}
