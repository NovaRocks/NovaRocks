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

//! Pipeline construction for one compiled LocalProgram (local-compiler
//! output). Expressions are evaluated only through compiled roots; there is no
//! legacy ExprArena thaw and no legacy node identity. A node family without
//! a compiled processor is an explicit refusal, never a legacy fallback.
//!
//! Reuse boundary: families that evaluate expressions (Project, Filter, Sort
//! and every row-count TopN phase, Unpivot, ChangeEventExpand, Aggregate,
//! Analytic) run compiled processors that own one instance per root and
//! driver; Aggregate reuses only the key table that owns group-key
//! equivalence, and Analytic runs pure window kernels over its own partition
//! geometry. Legacy operators are
//! reused only where they evaluate nothing: the all-constant Values source,
//! Limit, the local gather exchange, the row-count assertion and the UnionAll
//! fan-in queue. A Values with dynamic cells evaluates each cell root once
//! through the compiled Values source. Repeat is a compiled processor too,
//! because the legacy one re-derives its output schema. Both join families
//! evaluate their keys, residual and predicate through compiled roots and
//! reuse only array-level kernels: the hash map, build store, gather, match
//! decisions, the shared build states and the nested-loop build sink.
//!
//! Runtime filters bind to the Task's runtime-filter session. This milestone
//! executes BlockingSnapshot membership consumers at a compiled scan source,
//! applied row by row through the scan's compiled key roots, and membership
//! producers at a hash join's build keys. Every other runtime-filter shape is
//! refused by name when the pipeline is built.

use std::collections::{BTreeMap, BTreeSet};

use super::local::{runtime_filter_consumer_contract, runtime_filter_producer};
use super::*;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::operators::compiled_aggregate::CompiledAggregateProcessorFactory;
use crate::exec::operators::compiled_change_events::CompiledChangeEventProcessorFactory;
use crate::exec::operators::compiled_expression::{
    CompiledFilterProcessorFactory, CompiledProjectProcessorFactory,
};
use crate::exec::operators::compiled_generate_series::CompiledGenerateSeriesProcessorFactory;
use crate::exec::operators::compiled_repeat::CompiledRepeatProcessorFactory;
use crate::exec::operators::compiled_sort::CompiledSortProcessorFactory;
use crate::exec::operators::compiled_table_function::CompiledTableFunctionProcessorFactory;
use crate::exec::operators::compiled_unpivot::CompiledUnpivotProcessorFactory;
use crate::exec::operators::compiled_window::CompiledWindowProcessorFactory;
use crate::exec::operators::runtime_filter::CompiledRuntimeFilterConsumers;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::preparation_metadata::{
    CompiledMetadataMode, CompiledSchemaMetadataScope, DirectCompiledSchemaMetadataScope,
};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_local_program::{
    AssertRowsMode, BindingRequirement, FilterConsumerActivation, FilterConsumerAtExpr,
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, RowAssertion, StaticFilterContract,
};

/// Operator display identity for a compiled node: its local program index.
/// Profiles keep the program's provenance as the source relation.
fn display_id(id: ProgramNodeId) -> Result<i32, String> {
    i32::try_from(id.index()).map_err(|_| "compiled program node index exceeds i32".to_string())
}

/// The receiver node id a compiled ExchangeSource is addressed by: the
/// physical destination node of its edge, which is also the sender's
/// `dest_node_id` routing key.
fn receiver_node_id(program: &LocalProgram, id: ProgramNodeId) -> Result<i32, String> {
    let input = program.exchange_inputs().get(&id).ok_or_else(|| {
        format!(
            "compiled exchange source at local node {} has no exchange address",
            id.index()
        )
    })?;
    i32::try_from(input.receiver_node).map_err(|_| {
        format!(
            "compiled exchange receiver node {} exceeds i32",
            input.receiver_node
        )
    })
}

/// The Task's exchange bindings must cover exactly the program's compiled
/// exchange sources: no source without its receiver, no binding without a
/// source, and every binding keyed by the receiver it was registered for.
fn validate_compiled_exchange_bindings(
    program: &LocalProgram,
    bindings: &ExchangeBindings,
) -> Result<(), String> {
    let sources = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| matches!(node.kind(), ProgramNodeKind::ExchangeSource { .. }))
        .map(|(index, _)| ProgramNodeId::new(index))
        .collect::<BTreeSet<_>>();
    if sources != program.exchange_inputs().keys().copied().collect() {
        return Err(
            "compiled exchange addresses do not match the program's exchange sources".to_string(),
        );
    }
    let mut receivers = BTreeSet::new();
    for id in &sources {
        let receiver = receiver_node_id(program, *id)?;
        if !receivers.insert(receiver) {
            return Err(format!(
                "compiled exchange receiver node {receiver} is addressed by more than one source"
            ));
        }
        let binding = bindings.get(receiver).ok_or_else(|| {
            format!("missing exchange binding for compiled receiver node {receiver}")
        })?;
        if binding.key.node_id != receiver {
            return Err(format!(
                "exchange binding for compiled receiver node {receiver} is keyed to node {}",
                binding.key.node_id
            ));
        }
    }
    if let Some(extra) = bindings.node_ids().find(|id| !receivers.contains(id)) {
        return Err(format!(
            "exchange binding for node {extra} has no compiled exchange source"
        ));
    }
    Ok(())
}

/// The physical scan node a compiled Scan is addressed by: the key of its
/// Task split queue and of the scan binding its Task materialized.
fn scan_node_id(program: &LocalProgram, id: ProgramNodeId) -> Result<i32, String> {
    let input = program.scan_inputs().get(&id).ok_or_else(|| {
        format!(
            "compiled scan at local node {} has no scan address",
            id.index()
        )
    })?;
    i32::try_from(input.scan_node)
        .map_err(|_| format!("compiled scan node {} exceeds i32", input.scan_node))
}

/// The Task's scan bindings must cover exactly the program's compiled scans:
/// no scan without its bound operation, no binding without a scan, and every
/// binding keyed by the physical scan node it was bound for.
fn validate_compiled_scan_bindings(
    program: &LocalProgram,
    bindings: &ScanBindings,
) -> Result<(), String> {
    let scans = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| matches!(node.kind(), ProgramNodeKind::Scan { .. }))
        .map(|(index, _)| ProgramNodeId::new(index))
        .collect::<BTreeSet<_>>();
    if scans != program.scan_inputs().keys().copied().collect() {
        return Err("compiled scan addresses do not match the program's scans".to_string());
    }
    let mut scan_nodes = BTreeSet::new();
    for id in &scans {
        let scan_node = scan_node_id(program, *id)?;
        if !scan_nodes.insert(scan_node) {
            return Err(format!(
                "compiled scan node {scan_node} is addressed by more than one scan"
            ));
        }
        if bindings.get(scan_node).is_none() {
            return Err(format!(
                "missing scan binding for compiled scan node {scan_node}"
            ));
        }
    }
    if let Some(extra) = bindings.node_ids().find(|id| !scan_nodes.contains(id)) {
        return Err(format!(
            "scan binding for node {extra} has no compiled scan"
        ));
    }
    Ok(())
}

/// The runtime-filter bindings one node's static sites declare, consumers
/// and producers alike, in their frozen order.
fn node_runtime_filter_bindings(kind: &ProgramNodeKind) -> Vec<u32> {
    match kind {
        ProgramNodeKind::Scan {
            runtime_filters, ..
        }
        | ProgramNodeKind::ExchangeSource {
            runtime_filters, ..
        } => runtime_filters
            .iter()
            .map(|binding| binding.consumer.binding_id())
            .collect(),
        ProgramNodeKind::RuntimeFilterConsumer { bindings, .. } => bindings
            .iter()
            .map(|binding| binding.consumer.binding_id())
            .collect(),
        ProgramNodeKind::Aggregate { topn_filters, .. } => topn_filters
            .iter()
            .map(|filter| filter.producer.binding_id())
            .collect(),
        ProgramNodeKind::Join {
            runtime_filters,
            runtime_filter_consumers,
            ..
        } => runtime_filters
            .iter()
            .map(|filter| filter.producer.binding_id())
            .chain(
                runtime_filter_consumers
                    .iter()
                    .map(|filter| filter.consumer.binding_id()),
            )
            .collect(),
        _ => Vec::new(),
    }
}

/// The program's runtime-filter binding requirements must name exactly the
/// bindings of its runtime-filter sites, and a binding is one participant: it
/// has exactly one site, as a consumer or as a producer. No site runs without
/// its requirement, no requirement is left without its site, and a program
/// with any site runs only with the Task's runtime-filter session.
fn validate_compiled_runtime_filter_bindings(
    program: &LocalProgram,
    session: Option<&crate::runtime_filter::RuntimeFilterSessionRef>,
) -> Result<(), String> {
    let mut sites = BTreeMap::<i32, usize>::new();
    for (index, node) in program.graph().nodes().iter().enumerate() {
        for binding_id in node_runtime_filter_bindings(node.kind()) {
            let binding_id = i32::try_from(binding_id).map_err(|_| {
                format!(
                    "compiled runtime-filter binding_id={binding_id} at local node {index} exceeds i32"
                )
            })?;
            if let Some(other) = sites.insert(binding_id, index) {
                return Err(format!(
                    "compiled runtime-filter binding_id={binding_id} has a site at local node {other} and at local node {index}"
                ));
            }
        }
    }
    let required = program
        .graph()
        .requirements()
        .entries()
        .iter()
        .filter_map(|requirement| match requirement {
            BindingRequirement::RuntimeFilter { binding_id } => Some(*binding_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if let Some((binding_id, index)) = sites.iter().find(|(id, _)| !required.contains(id)) {
        return Err(format!(
            "compiled runtime-filter binding_id={binding_id} at local node {index} has no binding requirement"
        ));
    }
    if let Some(binding_id) = required.iter().find(|id| !sites.contains_key(id)) {
        return Err(format!(
            "compiled runtime-filter binding requirement binding_id={binding_id} has no program site"
        ));
    }
    if let Some((binding_id, index)) = sites.iter().next()
        && session.is_none()
    {
        return Err(format!(
            "compiled runtime-filter binding_id={binding_id} at local node {index} requires an execution runtime-filter session"
        ));
    }
    Ok(())
}

/// The blocking membership consumers of one compiled scan, keyed by the
/// scan's `RuntimeFilter { binding }` roots over its own output. This
/// milestone executes BlockingSnapshot membership (SetUnion) consumers
/// only; an ordered-domain or NonBlockingLive consumer is refused by name.
fn compiled_runtime_filter_consumers(
    owner: &'static str,
    display_owner: &'static str,
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    bindings: &[FilterConsumerAtExpr],
    ctx: &PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
) -> Result<Option<Arc<CompiledRuntimeFilterConsumers>>, String> {
    if bindings.is_empty() {
        return Ok(None);
    }
    let mut contracts = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let consumer = &binding.consumer;
        if matches!(consumer.contract(), StaticFilterContract::Ordered { .. }) {
            return Err(format!(
                "compiled {display_owner} at local node {} runtime-filter binding_id={} with an ordered-domain contract is not executable yet",
                id.index(),
                consumer.binding_id()
            ));
        }
        if let FilterConsumerActivation::NonBlockingLive { late_apply } = consumer.activation() {
            return Err(format!(
                "compiled {display_owner} at local node {} runtime-filter binding_id={} with NonBlockingLive {late_apply:?} activation is not executable yet",
                id.index(),
                consumer.binding_id()
            ));
        }
        runtime_filter_session(&ctx.runtime_filter_execution, consumer.binding_id())?;
        contracts.push(runtime_filter_consumer_contract(consumer)?);
    }
    CompiledRuntimeFilterConsumers::try_new(
        owner,
        Arc::clone(program),
        id,
        contracts,
        Arc::clone(error),
    )
    .map(|consumers| Some(Arc::new(consumers)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn build_compiled_pipeline_graph(
    program: &Arc<LocalProgram>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    dep_manager: DependencyManager,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    function_set: Arc<SealedExecutionFunctionSet>,
    error: Arc<RuntimeErrorState>,
) -> Result<PipelineGraph, String> {
    build_compiled_pipeline_graph_in(
        program,
        exchange_bindings,
        scan_bindings,
        writer_bindings,
        runtime_filter_session,
        dep_manager,
        pipeline_dop,
        root_sink_dop,
        function_set,
        error,
        &mut CompiledMetadataMode::<DirectCompiledSchemaMetadataScope>::Direct,
    )
    .map_err(ExecutionFailure::into_pipeline_message)
}

#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn build_compiled_pipeline_graph_with_metadata_host<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    dep_manager: DependencyManager,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    function_set: Arc<SealedExecutionFunctionSet>,
    error: Arc<RuntimeErrorState>,
    host: &mut H,
) -> ExecutionResult<PipelineGraph> {
    build_compiled_pipeline_graph_in(
        program,
        exchange_bindings,
        scan_bindings,
        writer_bindings,
        runtime_filter_session,
        dep_manager,
        pipeline_dop,
        root_sink_dop,
        function_set,
        error,
        &mut CompiledMetadataMode::Hosted(host),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn build_compiled_pipeline_graph_in<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    dep_manager: DependencyManager,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    function_set: Arc<SealedExecutionFunctionSet>,
    error: Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineGraph> {
    let graph = program.graph();
    if graph
        .nodes()
        .iter()
        .any(|node| node.legacy_native_node_id().is_some())
    {
        return Err("legacy-lowered nodes cannot enter the compiled pipeline".into());
    }
    validate_compiled_exchange_bindings(program, &exchange_bindings)?;
    validate_compiled_scan_bindings(program, &scan_bindings)?;
    writer_bindings.validate(program)?;
    validate_compiled_runtime_filter_bindings(program, runtime_filter_session.as_ref())?;
    let mut ctx = PipelineBuildContext {
        arena: Arc::new(ExprArena::default()),
        function_set,
        dep_manager,
        runtime_filter_execution: PipelineRuntimeFilterExecution {
            session: runtime_filter_session,
        },
        exchange_bindings,
        scan_bindings,
        compiled_writers: writer_bindings,
        next_pipeline_id: 0,
        pipeline_dop: pipeline_dop.max(1),
        operator_buffer_chunks: 1,
        local_exchange_buffer_mem_limit_per_driver: 1,
        local_exchange_max_buffered_rows: 0,
        precomputed_keyed_assert_keys: std::collections::HashMap::new(),
    };
    let mut build = build_node(program, graph.root(), &mut ctx, &error, metadata)?;
    // Statistics owns its specific original bounded materializer. Other root
    // purposes validate the actual computing root output at one final boundary.
    if let Some(novarocks_local_program::StaticSinkProgram::RootResult(contract)) = graph.sink()
        && contract.kind()
            != novarocks_result_contract::RootOutputKind::InternalFacts(
                novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
            )
    {
        let factory = match metadata {
            CompiledMetadataMode::Direct => {
                CompiledProjectProcessorFactory::try_new_final_result_boundary(
                    Arc::clone(program),
                    Arc::clone(&error),
                )?
            }
            CompiledMetadataMode::Hosted(host) => {
                CompiledProjectProcessorFactory::try_new_final_result_boundary_with_metadata_host(
                    Arc::clone(program),
                    Arc::clone(&error),
                    *host,
                )?
            }
        };
        build.pipeline.factories.push(Box::new(factory));
    }
    match root_sink_dop {
        None => {}
        // The frozen profile places the root sink on one driver.
        Some(1) => build = gather_to_one(build, &mut ctx, ROOT_SINK_LOCAL_EXCHANGE_NODE_ID),
        Some(other) => {
            return Err(format!("compiled root sink width {other} is not executable yet").into());
        }
    }
    build.pipeline.needs_sink = true;
    let root_id = build.pipeline.id;
    let mut pipelines = vec![build.pipeline];
    pipelines.append(&mut build.extra_pipelines);
    Ok(PipelineGraph { pipelines, root_id })
}

fn build_node<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineBuildResult> {
    let node = program
        .graph()
        .nodes()
        .get(id.index())
        .ok_or_else(|| format!("missing compiled program node {}", id.index()))?;
    let node_id = display_id(id)?;
    match node.kind() {
        ProgramNodeKind::Values { values } => {
            let source: Box<dyn OperatorFactory> = match values.batch() {
                Some(batch) => {
                    let chunk_schema = ChunkSchema::from_compiled_layout(values.layout())?;
                    let chunk = Chunk::new_with_chunk_schema(batch.clone(), chunk_schema);
                    Box::new(ValuesSourceFactory::new(chunk, node_id))
                }
                // Dynamic cells are evaluated once, at the source's opening
                // turn, by the one compiled evaluator.
                None => Box::new(values_source::CompiledValuesSourceFactory::try_new(
                    Arc::clone(program),
                    id,
                    Arc::clone(error),
                )?),
            };
            let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(1),
            })
        }
        ProgramNodeKind::GenerateSeries { input, .. } => {
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            build.pipeline.factories.push(Box::new(
                CompiledGenerateSeriesProcessorFactory::try_new(
                    Arc::clone(program),
                    id,
                    Arc::clone(error),
                )?,
            ));
            Ok(build)
        }
        ProgramNodeKind::Project { input, .. } => {
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            let factory = match metadata {
                CompiledMetadataMode::Direct => CompiledProjectProcessorFactory::try_new(Arc::clone(program), id, Arc::clone(error))?,
                CompiledMetadataMode::Hosted(host) => CompiledProjectProcessorFactory::try_new_with_metadata_host(Arc::clone(program), id, Arc::clone(error), *host)?,
            };
            build.pipeline.factories.push(Box::new(factory));
            Ok(build)
        }
        ProgramNodeKind::Filter { input, .. } => {
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            build
                .pipeline
                .factories
                .push(Box::new(CompiledFilterProcessorFactory::try_new(
                    Arc::clone(program),
                    ProgramExpressionRootSite::Node {
                        node: id,
                        role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
                    },
                    Arc::clone(error),
                )?));
            Ok(build)
        }
        ProgramNodeKind::Scan {
            source,
            runtime_filters,
            residuals,
            limit,
        } => {
            if source.compiled().is_none() {
                return Err(format!(
                    "scan at local node {} carries no compiled provider read",
                    id.index()
                ).into());
            }
            // Consumers are built first, so a refused binding builds no driver.
            let runtime_filters = compiled_runtime_filter_consumers(
                "Scan",
                "scan",
                program,
                id,
                runtime_filters,
                ctx,
                error,
            )?;
            if limit.is_some() {
                return Err(format!(
                    "compiled scan at local node {} with a scan limit is not executable yet",
                    id.index()
                ).into());
            }
            let scan_node = scan_node_id(program, id)?;
            let op = ctx.scan_bindings.get(scan_node).ok_or_else(|| {
                format!("missing scan binding for compiled scan node {scan_node}")
            })?;
            // The residual is the scan's own TruthOnly root over its output;
            // it is built first so a refused root binds no driver.
            let residual = (!residuals.is_empty())
                .then(|| {
                    CompiledFilterProcessorFactory::try_new(
                        Arc::clone(program),
                        ProgramExpressionRootSite::Node {
                            node: id,
                            role: ProgramNodeExpressionRole::ScanResidual { predicate: 0 },
                        },
                        Arc::clone(error),
                    )
                })
                .transpose()?;
            // One driver owns the scan stream; the shared handoff restores the
            // downstream DOP without duplicating the Task's reader capability.
            // Its runtime filters gate the first read and filter every chunk
            // before the handoff.
            let source: Box<dyn OperatorFactory> = Box::new(StreamScanSourceFactory::new_compiled(
                node_id,
                op,
                runtime_filters,
            ));
            let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
            let mut build = PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(1),
            };
            let target_dop = ctx.pipeline_dop.max(1);
            if target_dop > 1 {
                build = hand_off_to_dop(build, ctx, node_id, target_dop);
            }
            // The residual filters every downstream driver's rows; a scan
            // without one hands its rows on as read.
            if let Some(residual) = residual {
                build.pipeline.factories.push(Box::new(residual));
            }
            Ok(build)
        }
        ProgramNodeKind::ExchangeSource {
            timeout,
            runtime_filters,
            hash_partition_exprs,
        } => {
            if !runtime_filters.is_empty() {
                return Err(format!(
                    "compiled exchange source at local node {} with runtime-filter consumers is not executable yet",
                    id.index()
                ).into());
            }
            if !hash_partition_exprs.is_empty() {
                return Err(format!(
                    "compiled exchange source at local node {} with hash-key expressions is not executable yet",
                    id.index()
                ).into());
            }
            let receiver = receiver_node_id(program, id)?;
            let binding = ctx.exchange_bindings.get(receiver).ok_or_else(|| {
                format!("missing exchange binding for compiled receiver node {receiver}")
            })?;
            let exchange = ExchangeSourceNode::new(
                node_id,
                *timeout,
                ChunkSchema::from_compiled_layout(node.output_layout())?,
            );
            let source: Box<dyn OperatorFactory> =
                Box::new(ExchangeSourceFactory::new_compiled(exchange, binding)?);
            // Every driver pulls from the one instance-wide receiver.
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ProgramNodeKind::Limit {
            input,
            limit,
            offset,
        } => {
            let build = build_node(program, *input, ctx, error, metadata)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build
                .pipeline
                .factories
                .push(Box::new(LimitProcessorFactory::new(
                    node_id, *limit, *offset,
                )));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::Sort { input, .. } => {
            // Global and analytic Sort and every row-count TopN phase order
            // the whole instance input on one driver. An analytic Sort reads
            // an input the plan co-locates by its partition keys per instance,
            // so one driver holds each partition whole. Single and Final read a Singleton
            // input, so the instance input is the relation. A Partial keeps
            // its input distribution and declares its order keys as its
            // output ordering, which the TopN sequence trace matches against
            // the Final; one gathered driver is what makes the instance's
            // output one stream in that order. A per-driver partial would
            // still merge correctly at the Final, but it would emit DOP
            // interleaved runs the declared ordering does not describe, and
            // re-pruning them locally would evaluate a key twice.
            let factory =
                CompiledSortProcessorFactory::try_new(Arc::clone(program), id, Arc::clone(error))?;
            let build = build_node(program, *input, ctx, error, metadata)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::UnionAll { inputs } => {
            build_union_all(program, id, node_id, inputs, ctx, error, metadata)
        }
        ProgramNodeKind::Aggregate { input, .. } => {
            // Preserve the original driver-local Partial -> exchange -> Final
            // chronology. Grouped states hash actual emitted key slots; a
            // group-less state uses the original Single exchange.
            let factory = CompiledAggregateProcessorFactory::try_new(
                Arc::clone(program),
                id,
                Arc::clone(error),
            )?;
            let complete = factory.completes_groups();
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            if factory.requires_local_update_stages() && build.pipeline.dop > 1 {
                let partition_slots = factory.local_group_partition_slots();
                let (partial, final_stage) = factory.into_local_update_stages()?;
                build.pipeline.factories.push(Box::new(partial));
                build = if partition_slots.is_empty() {
                    gather_to_one(build, ctx, node_id)
                } else {
                    let partitions = build.pipeline.dop as usize;
                    shuffle_compiled_group_input_slots(
                        build,
                        ctx,
                        node_id,
                        partition_slots,
                        partitions,
                    )
                };
                build.pipeline.factories.push(Box::new(final_stage));
            } else {
                if complete {
                    build = gather_to_one(build, ctx, node_id);
                } else {
                    build.stream = StreamDesc::any(build.pipeline.dop);
                }
                build.pipeline.factories.push(Box::new(factory));
            }
            Ok(build)
        }
        ProgramNodeKind::Analytic { input, .. } => {
            // M1 evaluates every partition on one driver: the instance input
            // is gathered, in the order its sorted source emits it.
            let factory = CompiledWindowProcessorFactory::try_new(
                Arc::clone(program),
                id,
                Arc::clone(error),
            )?;
            let build = build_node(program, *input, ctx, error, metadata)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::AssertNumRows { input, mode } => {
            let factory = AssertNumRowsProcessorFactory::new(node_id, assertion_mode(mode))?;
            // Both modes judge the whole instance input: a global count, or
            // at most one row per key, so the assertion runs on one driver.
            // The keyed mode keeps the existing owner's key identity: a NULL
            // key equals a NULL key, and values compare by type and display.
            let build = build_node(program, *input, ctx, error, metadata)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ProgramNodeKind::Repeat { input, .. } => {
            let factory = CompiledRepeatProcessorFactory::try_new(program, id)?;
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ProgramNodeKind::Unpivot {
            input,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            max_output_rows,
            max_output_bytes,
        } => {
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            let statistics_root = id == program.graph().root()
                && matches!(program.graph().sink(),
                Some(novarocks_local_program::StaticSinkProgram::RootResult(contract))
                    if contract.kind()==novarocks_result_contract::RootOutputKind::InternalFacts(novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1));
            if statistics_root {
                let mappings = value_mappings
                    .iter()
                    .map(|mapping| {
                        Ok(crate::exec::node::unpivot::UnpivotValueMapping {
                            input_value_slot_id: mapping.input_value_slot_id,
                            constants: mapping
                                .constants
                                .iter()
                                .map(|value| match value {
                                    novarocks_local_program::UnpivotConstant::Scalar {
                                        expr_id,
                                        nullable,
                                    } => Ok(crate::exec::node::unpivot::UnpivotConstant::Scalar {
                                        expr_id: crate::exec::expr::ExprId(
                                            expr_id.index(),
                                        ),
                                        nullable: *nullable,
                                    }),
                                    novarocks_local_program::UnpivotConstant::Int32List(values) => {
                                        Ok(crate::exec::node::unpivot::UnpivotConstant::Int32List(
                                            values.clone(),
                                        ))
                                    }
                                    novarocks_local_program::UnpivotConstant::Utf8Map(values) => {
                                        Ok(crate::exec::node::unpivot::UnpivotConstant::Utf8Map(
                                            values
                                                .iter()
                                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                                .collect(),
                                        ))
                                    }
                                })
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let node = &program.graph().nodes()[id.index()];
                build.pipeline.factories.push(Box::new(
                    crate::exec::operators::StatisticsMaterializerFactory::try_new_compiled(
                        Arc::clone(program),
                        *value_output_slot_id,
                        literal_output_slot_ids.clone(),
                        mappings,
                        program.graph().nodes()[input.index()]
                            .output_layout()
                            .slots(),
                        crate::exec::chunk::ChunkSchema::from_compiled_layout(
                            node.output_layout(),
                        )?,
                        passthrough_columns.len(),
                        *max_output_rows,
                        *max_output_bytes,
                    )?,
                ));
            } else {
                build
                    .pipeline
                    .factories
                    .push(Box::new(CompiledUnpivotProcessorFactory::try_new(
                        Arc::clone(program),
                        id,
                        Arc::clone(error),
                    )?));
            }
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ProgramNodeKind::ChangeEventExpand { input, .. } => {
            let factory = CompiledChangeEventProcessorFactory::try_new(
                Arc::clone(program),
                id,
                Arc::clone(error),
            )?;
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ProgramNodeKind::TableFunction { input, .. } => {
            // Each driver expands its own rows in input order; its input is
            // the subordinate argument Project, so the stream keeps its width
            // and placement.
            let factory = CompiledTableFunctionProcessorFactory::try_new(
                Arc::clone(program),
                id,
                Arc::clone(error),
            )?;
            let mut build = build_node(program, *input, ctx, error, metadata)?;
            build.pipeline.factories.push(Box::new(factory));
            Ok(build)
        }
        ProgramNodeKind::TableWriter { input, .. } => {
            writer_pipelines::build_table_writer(program, id, node_id, *input, ctx, error, metadata)
        }
        ProgramNodeKind::TableFinish { inputs, .. } => {
            writer_pipelines::build_table_finish(program, id, node_id, inputs, ctx, error, metadata)
        }
        // The probe is the left input and the build the right input.
        ProgramNodeKind::Join { left, right, .. } => {
            join_pipelines::build_hash_join(program, id, node_id, *left, *right, ctx, error, metadata)
        }
        ProgramNodeKind::NestedLoopJoin { left, right, .. } => {
            join_pipelines::build_nested_loop_join(program, id, node_id, *left, *right, ctx, error, metadata)
        }
        ProgramNodeKind::RuntimeFilterConsumer { .. } => Err(format!(
            "compiled join probe-key runtime-filter consumer at local node {} is not executable yet",
            id.index()
        ).into()),
        _ => Err(format!(
            "compiled node family at local node {} has no compiled processor yet",
            id.index()
        ).into()),
    }
}

/// UnionAll as the compiler lowers it: every input is that branch's
/// subordinate normalizing Project, whose layout is exactly the union's
/// output layout, so each branch owns the union's channels in their frozen
/// order and its chunks pass through unchanged. The branches fan in through
/// the shared UnionAll queue, which carries no expression and no ordering.
fn build_union_all<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    inputs: &[ProgramNodeId],
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineBuildResult> {
    validate_union_branches(program, id, inputs)?;
    let mut builds = Vec::with_capacity(inputs.len());
    let mut producers = 0usize;
    for input in inputs {
        let child = build_node(program, *input, ctx, error, metadata)?;
        producers =
            producers.saturating_add(usize::try_from(child.pipeline.dop.max(1)).unwrap_or(1));
        builds.push(child);
    }
    let state = UnionAllSharedState::new(producers, node_id);
    let mut extra_pipelines = Vec::new();
    for mut child in builds {
        child
            .pipeline
            .factories
            .push(Box::new(UnionAllSinkFactory::new(state.clone(), node_id)));
        child.pipeline.needs_sink = false;
        extra_pipelines.push(child.pipeline);
        extra_pipelines.append(&mut child.extra_pipelines);
    }
    // The shared queue has one consumer; one driver drains it.
    let source: Box<dyn OperatorFactory> = Box::new(UnionAllSourceFactory::new(state, node_id));
    let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::single(),
    })
}

/// Every UnionAll input must be a subordinate normalizing Project whose layout
/// is exactly the union's: same complete fields, metadata and slot order.
fn validate_union_branches(
    program: &LocalProgram,
    id: ProgramNodeId,
    inputs: &[ProgramNodeId],
) -> Result<(), String> {
    let nodes = program.graph().nodes();
    let layout = nodes
        .get(id.index())
        .ok_or_else(|| format!("missing compiled UnionAll node {}", id.index()))?
        .output_layout();
    if inputs.len() < 2 {
        return Err(format!(
            "compiled UnionAll at local node {} has fewer than two branches",
            id.index()
        ));
    }
    for (ordinal, input) in inputs.iter().enumerate() {
        let branch = nodes
            .get(input.index())
            .ok_or_else(|| format!("missing compiled UnionAll branch {}", input.index()))?;
        let normalizer = matches!(
            branch.kind(),
            ProgramNodeKind::Project {
                is_subordinate: true,
                ..
            }
        );
        let same_layout = branch.output_layout().schema() == layout.schema()
            && branch.output_layout().slots() == layout.slots();
        if !normalizer || !same_layout {
            return Err(format!(
                "compiled UnionAll at local node {} branch {ordinal} is not a normalizer owning the union layout",
                id.index()
            ));
        }
    }
    Ok(())
}

/// The frozen assertion mode in the existing row-count owner's vocabulary.
fn assertion_mode(mode: &AssertRowsMode) -> AssertNumRowsMode {
    use crate::exec::node::assert::Assertion;
    match mode {
        AssertRowsMode::Global {
            desired_num_rows,
            assertion,
            subquery_string,
        } => AssertNumRowsMode::Global {
            desired_num_rows: *desired_num_rows,
            assertion: match assertion {
                RowAssertion::Eq => Assertion::Eq,
                RowAssertion::Ne => Assertion::Ne,
                RowAssertion::Lt => Assertion::Lt,
                RowAssertion::Le => Assertion::Le,
                RowAssertion::Gt => Assertion::Gt,
                RowAssertion::Ge => Assertion::Ge,
            },
            subquery_string: subquery_string.as_ref().map(ToString::to_string),
        },
        AssertRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix,
        } => AssertNumRowsMode::PerKeyAtMostOne {
            key_slots: key_slots.clone(),
            key_labels: key_labels.iter().map(ToString::to_string).collect(),
            message_prefix: message_prefix.to_string(),
        },
    }
}

#[path = "compiled_values.rs"]
mod values_source;

#[path = "compiled_writer.rs"]
mod writer_pipelines;

#[cfg(test)]
#[path = "compiled_writer_tests.rs"]
mod writer_tests;

#[path = "compiled_join.rs"]
mod join_pipelines;

#[cfg(test)]
#[path = "compiled_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "compiled_exchange_tests.rs"]
mod exchange_tests;

#[cfg(test)]
#[path = "compiled_scan_tests.rs"]
mod scan_tests;

#[cfg(test)]
#[path = "compiled_family_fixture.rs"]
mod family_fixture;

#[cfg(test)]
#[path = "compiled_sort_tests.rs"]
mod sort_tests;

#[cfg(test)]
#[path = "compiled_union_tests.rs"]
mod union_tests;

#[cfg(test)]
#[path = "compiled_assert_tests.rs"]
mod assert_tests;

#[cfg(test)]
#[path = "compiled_expand_tests.rs"]
mod expand_tests;

#[cfg(test)]
#[path = "compiled_topn_split_tests.rs"]
mod topn_split_tests;

#[cfg(test)]
#[path = "compiled_values_tests.rs"]
pub(crate) mod values_tests;

#[cfg(test)]
#[path = "compiled_generate_series_tests.rs"]
mod generate_series_tests;

#[cfg(test)]
#[path = "compiled_aggregate_fixture.rs"]
mod aggregate_fixture;

#[cfg(test)]
#[path = "compiled_aggregate_tests.rs"]
mod aggregate_tests;

#[cfg(test)]
#[path = "compiled_join_tests.rs"]
mod join_tests;

#[cfg(test)]
#[path = "compiled_runtime_filter_tests.rs"]
mod runtime_filter_tests;

#[cfg(test)]
#[path = "compiled_runtime_filter_compile_tests.rs"]
mod runtime_filter_compile_tests;

#[cfg(test)]
#[path = "compiled_table_function_tests.rs"]
mod compiled_table_function_tests;

#[cfg(test)]
#[path = "compiled_window_fixture.rs"]
mod window_fixture;

#[cfg(test)]
#[path = "compiled_window_tests.rs"]
mod compiled_window_tests;
