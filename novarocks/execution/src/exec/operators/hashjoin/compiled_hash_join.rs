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

//! Compiled hash join: the build sink and probe processor of one compiled
//! LocalProgram Join node.
//!
//! Keys and the residual are evaluated only through per-driver compiled root
//! instances. A key root reads its own side's batch as the operator receives
//! it; the residual reads the gathered candidate pairs in the join-scope
//! layout. The hash map, build store, gather and match-decision kernels work
//! on the evaluated arrays; nothing reads a legacy expression arena.
//!
//! The local orientation is fixed by the compiler: the probe is the program's
//! left input and the build its right input. One build driver publishes one
//! artifact that every probe driver of the instance shares: every physical
//! distribution has already delivered this instance its whole share of both
//! sides. This milestone executes Inner, LeftOuter, LeftSemi, LeftAnti and
//! NullAwareLeftAnti; a kind that must publish unmatched build rows is
//! refused when the pipeline is built.
//!
//! Output batches are exactly the node's frozen canonical layout: a
//! NULL-extended build column is already nullable there, so no schema is
//! patched at runtime. A row data error of a key or of the residual over a
//! candidate pair is a required error.
//!
//! The build driver also feeds the join's membership runtime-filter
//! producers: each evaluated build-key array is submitted once, the producers
//! close after the artifact is published, and any build failure or cancel
//! fails them. The instance's one build driver is their one local partition.

use super::join_hash_map::build_store::BuildStoreChunkProjection;
use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use novarocks_local_program::{
    JoinDistributionMode, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};

use super::broadcast_join_shared::BroadcastJoinSharedState;
use super::build_artifact::{BuildView, JoinBuildArtifact};
use super::build_requirements::{
    BuildComponentRequirements, NullKeyRequirement, required_build_components,
};
use super::join_hash_map::build_store::BuildStoreBuilder;
use super::join_hash_map::finalize::is_all_match_one;
use super::join_hash_map::gather::{
    JoinGatherError, MAX_JOIN_OUTPUT_ROWS_PER_BATCH, TypedJoinBatch, filter_probe_typed,
    gather_left_with_null_right_typed, gather_probe_build_batches_typed,
};
use super::join_hash_map::method::{JoinCursorFailure, JoinHashMap, JoinKeyBuild, JoinPairCursor};
use super::join_hash_map::search::JoinSelection;
use super::native_runtime_filter::{
    NativeRuntimeFilterProducerFactory, NativeRuntimeFilterProducerSet,
};
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::partition_keys::source_key_owner;
use crate::exec::hash_table::{
    allocation::AggregateAllocator,
    key_index::{BulkWork, PreparedKeyBatch},
};
use crate::exec::node::join::JoinType;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_all, instances, root_value_type,
};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    shape::{Transform, TransformKernel},
    step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime_filter::RuntimeFilterProducerFailure;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::decode::ValueConversionContext;

#[derive(Debug)]
struct HashJoinContractFailure(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for HashJoinContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for HashJoinContractFailure {}
impl novarocks_failure_contract::FailureProjection for HashJoinContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::HashJoinExecutionContract.reason(),
        )
    }
}
fn hash_join_contract(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(HashJoinContractFailure(
        novarocks_failure_contract::BoundedDetail::truncating(detail),
    ))
}
#[derive(Debug)]
struct JoinBuildCountOverflow {
    current: usize,
    incoming: usize,
}
impl std::fmt::Display for JoinBuildCountOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hash join build cannot add {} rows to {}",
            self.incoming, self.current
        )
    }
}
impl std::error::Error for JoinBuildCountOverflow {}
impl novarocks_failure_contract::FailureProjection for JoinBuildCountOverflow {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts, ResourceScope,
        };
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::JoinBuildCountExtent.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Operator),
            constraint_scope: Some(ResourceScope::Operator),
            dimension: ResourceDimension::Positions,
            requested: u64::try_from(self.incoming).ok(),
            required_growth: u64::try_from(self.incoming).ok(),
            limit: u64::try_from(usize::MAX).ok(),
            live: u64::try_from(self.current).ok(),
            committed: None,
        })
    }
}

/// Runtime role schemas retain their own finite source/new storage holds.
/// They never retain the declaring plan, program or creating factory.
pub(crate) struct PreparedJoinSchemas {
    pub(crate) build: ChunkSchemaRef,
    pub(crate) scope: ChunkSchemaRef,
    pub(crate) output: ChunkSchemaRef,
}

pub(crate) fn prepare_join_schemas(
    factory: &dyn InputConstructionFactory,
    build: &novarocks_local_program::StaticLayout,
    scope: &novarocks_local_program::StaticLayout,
    output: &novarocks_local_program::StaticLayout,
) -> ExecutionResult<PreparedJoinSchemas> {
    let build = factory.create_compiled_chunk_schema(build)?;
    let scope = factory.create_compiled_chunk_schema(scope)?;
    let output = factory.create_compiled_chunk_schema(output)?;
    Ok(PreparedJoinSchemas {
        build,
        scope,
        output,
    })
}

/// The frozen facts of one compiled Join node that both sides share.
pub(crate) struct CompiledHashJoinPlan {
    node: ProgramNodeId,
    join_type: JoinType,
    probe_sites: Vec<ProgramExpressionRootSite>,
    build_sites: Vec<ProgramExpressionRootSite>,
    build_key_bindings: Vec<FunctionValueType>,
    probe_key_bindings: Vec<FunctionValueType>,
    residual: Option<ProgramExpressionRootSite>,
    eq_null_safe: Vec<bool>,
    build: novarocks_local_program::StaticLayout,
    scope: novarocks_local_program::StaticLayout,
    output: novarocks_local_program::StaticLayout,
    requirements: BuildComponentRequirements,
}

impl CompiledHashJoinPlan {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled join node {} is absent", node.index()),
            )
        })?;
        let ProgramNodeKind::Join {
            join_type,
            distribution_mode,
            left_layout,
            right_layout,
            join_scope_layout,
            probe_keys,
            build_keys,
            eq_null_safe,
            residual_predicate,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled node {} is not a hash join", node.index()),
            ));
        };
        validate_join_scope(node, left_layout, right_layout, join_scope_layout)?;
        let join_type = match join_type {
            novarocks_local_program::JoinType::Inner => JoinType::Inner,
            novarocks_local_program::JoinType::LeftOuter => JoinType::LeftOuter,
            novarocks_local_program::JoinType::LeftSemi => JoinType::LeftSemi,
            novarocks_local_program::JoinType::LeftAnti => JoinType::LeftAnti,
            novarocks_local_program::JoinType::NullAwareLeftAnti => JoinType::NullAwareLeftAnti,
            other => {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled {other:?} hash join at local node {} publishes build rows and is not executable yet",
                        node.index()
                    ),
                ));
            }
        };
        if *distribution_mode != JoinDistributionMode::Broadcast {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled hash join at local node {} with a local partitioned build is not executable yet",
                    node.index()
                ),
            ));
        }
        if probe_keys.is_empty()
            || probe_keys.len() != build_keys.len()
            || probe_keys.len() != eq_null_safe.len()
        {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled hash join at local node {} has no exact key pairs",
                    node.index()
                ),
            ));
        }
        let site = |role| ProgramExpressionRootSite::Node { node, role };
        let ordinal = |key: usize| {
            u32::try_from(key).map_err(|_| {
                program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "join key count exceeds u32",
                )
            })
        };
        let mut probe_sites = Vec::with_capacity(probe_keys.len());
        let mut build_sites = Vec::with_capacity(build_keys.len());
        for key in 0..probe_keys.len() {
            let key = ordinal(key)?;
            probe_sites.push(site(ProgramNodeExpressionRole::JoinProbeKey { key }));
            build_sites.push(site(ProgramNodeExpressionRole::JoinBuildKey { key }));
        }
        let build_key_bindings = build_sites
            .iter()
            .map(|site| root_value_type(program, *site).map_err(ExecutionFailure::from))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let probe_key_bindings = probe_sites
            .iter()
            .map(|site| root_value_type(program, *site).map_err(ExecutionFailure::from))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let residual = residual_predicate.map(|_| site(ProgramNodeExpressionRole::JoinResidual));
        let requirements = required_build_components(join_type, residual.is_some(), true, true);
        Ok(Self {
            node,
            join_type,
            probe_sites,
            build_sites,
            build_key_bindings,
            probe_key_bindings,
            residual,
            eq_null_safe: eq_null_safe.clone(),
            build: right_layout.clone(),
            scope: join_scope_layout.clone(),
            output: graph_node.output_layout().clone(),
            requirements,
        })
    }

    /// The null-safe equality flag of every key pair, in key order.
    pub(crate) fn eq_null_safe(&self) -> &[bool] {
        &self.eq_null_safe
    }

    /// The build-key root of key `ordinal`.
    pub(crate) fn build_site(&self, ordinal: usize) -> Option<ProgramExpressionRootSite> {
        self.build_sites.get(ordinal).copied()
    }
}

/// The single build driver: evaluates every build key root, collects the
/// rows the kind needs, publishes the instance's one build artifact and
/// feeds the join's runtime-filter producers.
pub(crate) struct CompiledHashJoinBuildSinkFactory {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    error: Arc<RuntimeErrorState>,
    producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
}

impl CompiledHashJoinBuildSinkFactory {
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        plan: Arc<CompiledHashJoinPlan>,
        state: Arc<BroadcastJoinSharedState>,
        error: Arc<RuntimeErrorState>,
        producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
    ) -> Self {
        Self {
            name: format!("COMPILED_HASH_JOIN_BUILD (node={})", plan.node.index()),
            program,
            plan,
            state,
            error,
            producers,
        }
    }
}

/// One build driver's runtime-filter producers: the producer streams of its
/// local partition, or why they could not be created, reported at bind.
struct BuildRuntimeFilterProducers {
    producers: Option<NativeRuntimeFilterProducerSet>,
    create_error: Option<ExecutionFailure>,
    local_partition_count: u32,
}

impl OperatorFactory for CompiledHashJoinBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let null_key_rows =
            (self.plan.requirements.null_keys == NullKeyRequirement::NullKeyRows).then(Vec::new);
        let runtime_filters = self.producers.as_ref().map(|factory| {
            let (producers, create_error) = match factory.create_for_driver(dop, driver_id) {
                Ok(producers) => (Some(producers), None),
                Err(error) => (None, Some(error)),
            };
            BuildRuntimeFilterProducers {
                producers,
                create_error,
                local_partition_count: factory.local_partition_count(),
            }
        });
        Box::new(crate::exec::pipeline::shape::Transform::hydrating(
            CompiledHashJoinBuildSink {
                name: self.name.clone(),
                program: Arc::clone(&self.program),
                plan: Arc::clone(&self.plan),
                state: Arc::clone(&self.state),
                control: RuntimeKernelControl::new(Arc::clone(&self.error)),
                instances: None,
                store: BuildStoreBuilder::new(),
                construction: None,
                prepared_build: None,
                prepare_failure: None,
                input_chunks: Vec::new(),
                key_build: None,
                key_conversions: ValueConversionContext::default(),
                row_count: 0,
                has_null_key: false,
                null_key_rows,
                chunks_tracker: None,
                table_tracker: None,
                artifact_memory: None,
                runtime_filters,
            },
        ))
    }
    fn is_sink(&self) -> bool {
        true
    }
}

struct CompiledHashJoinBuildSink {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    store: BuildStoreBuilder,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    prepared_build: Option<ChunkSchemaRef>,
    prepare_failure: Option<ExecutionFailure>,
    /// Retained only to charge the build rows to this operator's tracker
    /// until the artifact owns them.
    input_chunks: Vec<Chunk>,
    key_build: Option<JoinKeyBuild>,
    key_conversions: ValueConversionContext,
    row_count: usize,
    has_null_key: bool,
    null_key_rows: Option<Vec<u32>>,
    chunks_tracker: Option<Arc<MemTracker>>,
    table_tracker: Option<Arc<MemTracker>>,
    artifact_memory: Option<BuildArtifactMemory>,
    runtime_filters: Option<BuildRuntimeFilterProducers>,
}

struct BuildArtifactMemory {
    table: Option<Arc<MemTracker>>,
    store: Option<Arc<MemTracker>>,
}

impl OperatorLifecycle for CompiledHashJoinBuildSink {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        if self.prepared_build.is_some() {
            return Ok(());
        }
        let result: ExecutionResult<_> = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            Ok(factory.create_compiled_chunk_schema(&self.plan.build)?)
        })();
        match result {
            Ok(schema) => {
                self.prepared_build = Some(schema);
                Ok(())
            }
            Err(first) => {
                self.prepare_failure = Some(first.clone());
                Err(first)
            }
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        let artifact = state.mem_tracker().map(|root| {
            MemTracker::new_child(
                format!("JoinBuildArtifact: {}", self.state.dep_name()),
                &root,
            )
        });
        self.artifact_memory = Some(BuildArtifactMemory {
            table: artifact
                .as_ref()
                .map(|tracker| MemTracker::new_child("BuildHashTable", tracker)),
            store: artifact
                .as_ref()
                .map(|tracker| MemTracker::new_child("BuildStore", tracker)),
        });
        let Some(filters) = self.runtime_filters.as_mut() else {
            return Ok(());
        };
        if let Some(error) = filters.create_error.take() {
            return Err(error);
        }
        match filters.producers.as_mut() {
            Some(producers) => Ok(producers.bind(filters.local_partition_count)?),
            None => Ok(()),
        }
    }
    fn cancel(&mut self) {
        let _ = self.fail_runtime_filters(RuntimeFilterProducerFailure::Cancelled);
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.fail_runtime_filters(RuntimeFilterProducerFailure::ExecutionFailed)
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        let chunks = MemTracker::new_child("BuildInputChunks", &tracker);
        for chunk in self.input_chunks.iter_mut() {
            chunk.transfer_to(&chunks);
        }
        self.chunks_tracker = Some(chunks);
        self.table_tracker = Some(MemTracker::new_child("BuildHashTable", &tracker));
    }
}
impl crate::exec::pipeline::operator::InputEncodingPolicy for CompiledHashJoinBuildSink {}
impl crate::exec::pipeline::shape::SinkKernel for CompiledHashJoinBuildSink {
    fn consume(
        &mut self,
        ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        input: crate::exec::pipeline::input::PreparedInput,
    ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
        use crate::exec::pipeline::{shape::SinkStep, step::NextStep};
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if !chunk.is_empty()
            && let Err(failure) = self.push_build_chunk(chunk)
        {
            ctx.report_failure(failure.clone());
            let _ = self.fail_runtime_filters(RuntimeFilterProducerFailure::ExecutionFailed);
            return Err(failure);
        }
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(
        &mut self,
        ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
        use crate::exec::pipeline::{shape::SinkStep, step::NextStep};
        if let Err(failure) = self.publish_build() {
            ctx.report_failure(failure.clone());
            let _ = self.fail_runtime_filters(RuntimeFilterProducerFailure::ExecutionFailed);
            return Err(failure);
        }
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}

impl CompiledHashJoinBuildSink {
    fn push_build_chunk(&mut self, mut chunk: Chunk) -> ExecutionResult<()> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        let prepared = self
            .prepared_build
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = self.control.construction_owner(prepared)?;
        instances(
            &mut self.instances,
            &self.program,
            &self.plan.build_sites,
            &self.control,
        )?;
        let roots = self.instances.as_mut().expect("instances were created");
        let mut keys = Vec::with_capacity(roots.len());
        for (instance, site) in roots.iter_mut().zip(&self.plan.build_sites) {
            keys.push(evaluate_all(
                instance,
                *site,
                chunk.record_batch(),
                &self.control,
            )?);
        }
        let base = self.row_count;
        let rows = chunk.len();
        self.row_count = base.checked_add(rows).ok_or_else(|| {
            ExecutionFailure::from_domain(JoinBuildCountOverflow {
                current: base,
                incoming: rows,
            })
        })?;
        // The producers observe the same evaluated key arrays the table is
        // built from, after the build accepted the chunk.
        let submitted = self.runtime_filters.is_some().then(|| keys.clone());
        let tracker = self
            .artifact_memory
            .as_ref()
            .and_then(|memory| memory.table.as_ref())
            .ok_or(ConstructionFailure::MissingOwner)?;
        let work = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(Arc::clone(tracker)),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        };
        if self.key_build.is_none() {
            let purpose = self
                .plan
                .requirements
                .join_hash_map_purpose()
                .ok_or_else(|| {
                    hash_join_contract("compiled keyed join lookup purpose is missing")
                })?;
            self.key_build = Some(
                JoinKeyBuild::new(
                    &self.plan.build_key_bindings,
                    &self.plan.eq_null_safe,
                    purpose,
                    &work,
                )
                .map_err(|cause| self.control.project_required_failure(cause))?,
            );
        }
        // Only the original captured input receives source custody. Spec,
        // domain, later graph and copied representatives use destination work.
        let source_owner = source_key_owner(&chunk, owner.as_ref())?;
        let source_work = BulkWork {
            owner: source_owner.as_ref(),
            allocator: work.allocator.clone(),
            host: Arc::clone(&work.host),
            control: work.control,
        };
        let has_null_key = &mut self.has_null_key;
        let null_key_rows = &mut self.null_key_rows;
        self.key_build
            .as_mut()
            .expect("join capture was created")
            .push_observed(
                &keys,
                &self.plan.build_key_bindings,
                rows,
                &mut self.key_conversions,
                &source_work,
                &mut |row, logical_null, participates| {
                    *has_null_key |= logical_null;
                    if !participates && let Some(null_rows) = null_key_rows.as_mut() {
                        let flat = base
                            .checked_add(row)
                            .and_then(|row| u32::try_from(row).ok())
                            .ok_or_else(|| {
                                ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent)
                            })?;
                        let growth =
                            novarocks_type_contract::owned_resources::vec::push_growth(null_rows)
                                .map_err(|cause| match cause {
                                novarocks_type_contract::ControlResourceError::Control(cause) => {
                                    ConstructionFailure::Control(cause)
                                }
                                novarocks_type_contract::ControlResourceError::SourceModel(
                                    cause,
                                ) => ConstructionFailure::InvalidSource(cause.to_owned()),
                            })?;
                        if let Some(layout) = growth.requested_backing {
                            work.owner.request(layout)?;
                            null_rows.try_reserve(1).map_err(|source| {
                                work.allocator
                                    .allocation_error("join null physical rows", source)
                            })?;
                        }
                        null_rows.push(flat);
                    }
                    Ok(())
                },
            )
            .map_err(|cause| self.control.project_required_failure(cause))?;
        self.key_conversions.clear();
        if self.plan.requirements.requires_row_payload() {
            self.store.push_chunk_owned(&chunk, owner.as_ref())?;
            if let Some(tracker) = self.chunks_tracker.as_ref() {
                chunk.transfer_to(tracker);
            }
            self.input_chunks.push(chunk);
        }
        if let Some(keys) = submitted {
            self.submit_runtime_filters(&keys)?;
        }
        Ok(())
    }

    /// Builds and publishes the instance's one artifact, then closes the
    /// runtime-filter producers: the published build is what they describe.
    fn publish_build(&mut self) -> ExecutionResult<()> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        let prepared = self
            .prepared_build
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = self.control.construction_owner(prepared)?;
        let memory = self.artifact_memory.as_ref().ok_or_else(|| {
            KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled hash build runtime binding is missing",
            ))
        })?;
        let requirements = self.plan.requirements;
        let tracker = memory
            .table
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let work = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(Arc::clone(tracker)),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        };
        if self.key_build.is_none() {
            let purpose = requirements.join_hash_map_purpose().ok_or_else(|| {
                hash_join_contract("compiled keyed join lookup purpose is missing")
            })?;
            self.key_build = Some(
                JoinKeyBuild::new(
                    &self.plan.build_key_bindings,
                    &self.plan.eq_null_safe,
                    purpose,
                    &work,
                )
                .map_err(|cause| self.control.project_required_failure(cause))?,
            );
        }
        let table = Some(
            self.key_build
                .take()
                .expect("join capture was created")
                .finish(&work)
                .map_err(|cause| self.control.project_required_failure(cause))?,
        );
        self.key_conversions.clear();
        let mut store = if requirements.requires_row_payload() {
            std::mem::replace(&mut self.store, BuildStoreBuilder::new())
                .finish_owned(owner.as_ref())?
        } else {
            None
        };
        self.input_chunks.clear();
        if let (Some(store), Some(tracker)) = (store.as_mut(), memory.store.as_ref()) {
            store.transfer_to_with_header_owner(tracker, owner.as_ref())?;
        }
        let artifact = Arc::new(JoinBuildArtifact::new_native(
            requirements,
            store,
            table,
            self.row_count,
            self.has_null_key,
            self.null_key_rows.take().map(Arc::new),
        ));
        artifact.validate_components(requirements)?;
        self.state.set_build(artifact)?;
        self.finish_runtime_filters()?;
        Ok(())
    }

    fn submit_runtime_filters(&mut self, keys: &[ArrayRef]) -> ExecutionResult<()> {
        match self
            .runtime_filters
            .as_mut()
            .and_then(|filters| filters.producers.as_mut())
        {
            Some(producers) => producers.submit(keys),
            None => Ok(()),
        }
    }

    fn finish_runtime_filters(&mut self) -> ExecutionResult<()> {
        match self
            .runtime_filters
            .as_mut()
            .and_then(|filters| filters.producers.as_mut())
        {
            Some(producers) => producers.finish(),
            None => Ok(()),
        }
    }

    fn fail_runtime_filters(
        &mut self,
        reason: RuntimeFilterProducerFailure,
    ) -> ExecutionResult<()> {
        match self
            .runtime_filters
            .as_mut()
            .and_then(|filters| filters.producers.as_mut())
        {
            Some(producers) => producers.fail(reason),
            None => Ok(()),
        }
    }
}

/// One probe driver: evaluates every probe key root per chunk and decides
/// matches against the shared build artifact.
pub(crate) struct CompiledHashJoinProbeProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledHashJoinProbeProcessorFactory {
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        plan: Arc<CompiledHashJoinPlan>,
        state: Arc<BroadcastJoinSharedState>,
        error: Arc<RuntimeErrorState>,
    ) -> Self {
        Self {
            name: format!("COMPILED_HASH_JOIN_PROBE (node={})", plan.node.index()),
            program,
            plan,
            state,
            error,
        }
    }
}

impl OperatorFactory for CompiledHashJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        let mut sites = self.plan.probe_sites.clone();
        sites.extend(self.plan.residual);
        Box::new(Transform::hydrating(CompiledHashJoinProbe {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            plan: Arc::clone(&self.plan),
            dep: self.state.dep(),
            state: Arc::clone(&self.state),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            construction: None,
            schemas: None,
            prepare_failure: None,
            sites,
            instances: None,
            build: None,
            cursor: None,
            key_tracker: None,
            key_conversions: ValueConversionContext::default(),
        }))
    }
}

/// The loaded build artifact one probe driver reads.
struct LoadedBuild {
    table: Option<Arc<JoinHashMap>>,
    chunk: Option<BuildStoreChunkProjection>,
    null_key_rows: Option<Arc<Vec<u32>>>,
    row_count: usize,
    has_null_key: bool,
}

struct CompiledHashJoinProbe {
    name: String,
    program: Arc<LocalProgram>,
    plan: Arc<CompiledHashJoinPlan>,
    state: Arc<BroadcastJoinSharedState>,
    dep: DependencyHandle,
    control: RuntimeKernelControl,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    schemas: Option<PreparedJoinSchemas>,
    prepare_failure: Option<ExecutionFailure>,
    /// Probe key roots in key order, then the residual root.
    sites: Vec<ProgramExpressionRootSite>,
    instances: Option<Vec<CompiledExpressionInstance>>,
    build: Option<LoadedBuild>,
    cursor: Option<HashProbeCursor>,
    key_tracker: Option<Arc<MemTracker>>,
    key_conversions: ValueConversionContext,
}

impl OperatorLifecycle for CompiledHashJoinProbe {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        if self.schemas.is_some() {
            return Ok(());
        }
        let result = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            prepare_join_schemas(
                factory.as_ref(),
                &self.plan.build,
                &self.plan.scope,
                &self.plan.output,
            )
        })();
        match result {
            Ok(schemas) => {
                self.schemas = Some(schemas);
                Ok(())
            }
            Err(first) => {
                // Only complete schema preparation is published. Earlier
                // temporary schemas have already dropped on this refusal.
                self.prepare_failure = Some(first.clone());
                Err(first)
            }
        }
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.key_tracker = Some(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.release_owned();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.release_owned();
    }
    fn on_driver_failure(&mut self) {
        self.release_owned();
    }
}

#[derive(Clone, Copy)]
enum HashProbePhase {
    EqualPairs,
    NullKeys,
    NullProbe,
    Tail,
}
struct HashProbeCursor {
    input: Chunk,
    pairs: Option<JoinPairCursor>,
    phase: HashProbePhase,
    matched: Vec<bool>,
    null_probe: Vec<bool>,
    matched_null: Vec<bool>,
    matched_any: Vec<bool>,
    probe_position: usize,
    build_position: usize,
    _key_storage: ValueTypeStorageLifetime,
}
impl InputEncodingPolicy for CompiledHashJoinProbe {}
impl TransformKernel for CompiledHashJoinProbe {
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let observable = self.dep.observable();
        let observed_generation = observable.generation();
        if self.build.is_some() || self.state.has_build() {
            return Ok(None);
        }
        Ok(Some(AdmissionNext::Wait(WaitFact {
            reason: WaitReason::Dependency,
            observable,
            observed_generation,
            deadline: None,
        })))
    }
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let result = self.start_probe(chunk).map(|output| {
            let next = if self.cursor.is_some() {
                NextStep::Runnable
            } else {
                NextStep::NeedInput
            };
            StepResult::new(output, next, rows)
        });
        self.complete_step(ctx, result)
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.cursor.is_none() {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let mut cursor = self.cursor.take().expect("live hash probe cursor");
        let result = self.advance_probe(&mut cursor);
        match result {
            Err(error) => {
                // Report before destroying the accepted input or any unreported work.
                ctx.report_failure(error.clone());
                self.release_owned();
                Err(error)
            }
            Ok(step) => {
                if matches!(step.next, NextStep::Runnable) {
                    self.cursor = Some(cursor);
                }
                Ok(step)
            }
        }
    }

    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let result = self
            .prepared_schemas()
            .map(|_| StepResult::new(None, NextStep::Finished, 0));
        let step = self.complete_step(ctx, result)?;
        self.release_owned();
        Ok(step)
    }
}

impl CompiledHashJoinProbe {
    fn release_owned(&mut self) {
        self.cursor = None;
        self.build = None;
        self.instances = None;
        self.key_conversions.clear();
    }
    fn complete_step(
        &mut self,
        ctx: &StepContext<'_>,
        result: ExecutionResult<StepResult>,
    ) -> ExecutionResult<StepResult> {
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
            self.release_owned();
        }
        result
    }

    fn prepared_schemas(&self) -> ExecutionResult<&PreparedJoinSchemas> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        self.schemas
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }

    fn load_build(&mut self) -> ExecutionResult<()> {
        if self.build.is_some() {
            return Ok(());
        }
        let artifact = self
            .state
            .get_build()
            .ok_or_else(|| hash_join_contract("compiled hash join build is not ready"))?;
        let row_count = artifact.build_row_count;
        let has_null_key = artifact.build_has_null_key;
        let view = BuildView::new(artifact, self.plan.requirements)?;
        self.build = Some(LoadedBuild {
            table: view.build_table(),
            chunk: view.optional_build_chunk(),
            null_key_rows: view.build_null_key_rows(),
            row_count,
            has_null_key,
        });
        Ok(())
    }

    fn loaded(&self) -> ExecutionResult<&LoadedBuild> {
        self.build
            .as_ref()
            .ok_or_else(|| hash_join_contract("compiled hash join build is not loaded"))
    }

    /// The build rows the decision needs, present exactly when the build has
    /// rows and its kind retains them.
    fn build_rows(&self) -> ExecutionResult<Option<BuildStoreChunkProjection>> {
        let build = self.loaded()?;
        if build.row_count == 0 {
            return Ok(None);
        }
        build
            .chunk
            .clone()
            .map(Some)
            .ok_or_else(|| hash_join_contract("compiled hash join build rows are missing"))
    }

    fn start_probe(&mut self, chunk: Chunk) -> ExecutionResult<Option<Chunk>> {
        self.prepared_schemas()?;
        self.load_build()?;
        if chunk.is_empty() {
            return Ok(None);
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let keys = self.plan.probe_sites.len();
        let mut arrays = Vec::with_capacity(keys);
        for (instance, site) in self
            .instances
            .as_mut()
            .expect("instances installed")
            .iter_mut()
            .zip(&self.sites)
            .take(keys)
        {
            arrays.push(evaluate_all(
                instance,
                *site,
                chunk.record_batch(),
                &self.control,
            )?);
        }
        let rows = chunk.len();
        let owner = self
            .control
            .construction_owner(&self.prepared_schemas()?.output)?;
        let tracker = self
            .key_tracker
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let work = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(Arc::clone(tracker)),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        };
        let source_owner = source_key_owner(&chunk, owner.as_ref())?;
        let source_work = BulkWork {
            owner: source_owner.as_ref(),
            allocator: work.allocator.clone(),
            host: Arc::clone(&work.host),
            control: work.control,
        };
        let table = self
            .loaded()?
            .table
            .clone()
            .filter(|table| !table.is_empty());
        let key_input = table
            .as_ref()
            .map(|table| {
                table.input(
                    &arrays,
                    &self.plan.probe_key_bindings,
                    rows,
                    &mut self.key_conversions,
                    &source_work,
                )
            })
            .transpose()
            .map_err(|cause| self.control.project_required_failure(cause))?;
        self.key_conversions.clear();
        let prepared = table
            .as_ref()
            .zip(key_input.as_ref())
            .map(|(table, input)| {
                PreparedKeyBatch::from_captured(
                    table.spec(),
                    input,
                    work.allocator.clone(),
                    &|| self.control.checkpoint_execution(),
                )
            })
            .transpose()
            .map_err(|cause| self.control.project_required_failure(cause))?;
        let null_aware = self.plan.join_type == JoinType::NullAwareLeftAnti;
        let null_probe = if null_aware {
            let mut nulls = Vec::new();
            let layout = std::alloc::Layout::array::<bool>(rows)
                .map_err(|_| hash_join_contract("join logical null mask extent overflow"))?;
            work.owner.request(layout)?;
            nulls.try_reserve_exact(rows).map_err(|source| {
                work.allocator
                    .allocation_error("join logical null mask", source)
            })?;
            for row in 0..rows {
                self.control.checkpoint_execution()?;
                nulls.push(match key_input.as_ref() {
                    Some(input) => {
                        input.row_has_null_observed(row, &|| self.control.checkpoint_execution())?
                    }
                    None => false, // No build lookup: only the empty-build decision consumes this path.
                });
            }
            nulls
        } else {
            Vec::new()
        };
        if self.plan.residual.is_none()
            && matches!(
                self.plan.join_type,
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti
            )
        {
            let matched = match table.as_ref().zip(prepared.as_ref()) {
                Some((table, prepared)) => table
                    .membership(prepared, &work)
                    .map_err(|cause| self.control.project_required_failure(cause))?
                    .0
                    .into_vec(),
                None => vec![false; rows],
            };
            let keep = match self.plan.join_type {
                JoinType::LeftSemi => matched,
                JoinType::LeftAnti => matched.into_iter().map(|matched| !matched).collect(),
                JoinType::NullAwareLeftAnti if self.loaded()?.row_count == 0 => vec![true; rows],
                JoinType::NullAwareLeftAnti if self.loaded()?.has_null_key => vec![false; rows],
                JoinType::NullAwareLeftAnti => matched
                    .iter()
                    .zip(&null_probe)
                    .map(|(matched, null)| !matched && !null)
                    .collect(),
                _ => unreachable!("decision-only join selected"),
            };
            return self.keep_probe(&chunk, &keep);
        }
        if null_aware && self.loaded()?.row_count == 0 {
            return self.keep_probe(&chunk, &vec![true; rows]);
        }
        let groups = match table.as_ref().zip(prepared.as_ref()) {
            Some((table, prepared)) => table
                .lookup_groups(prepared, &work)
                .map_err(|cause| self.control.project_required_failure(cause))?,
            None => vec![None; rows],
        };
        self.cursor = Some(HashProbeCursor {
            input: chunk,
            pairs: Some(JoinPairCursor::new(groups, owner.storage_lifetime())),
            _key_storage: owner.storage_lifetime(),
            phase: HashProbePhase::EqualPairs,
            matched: vec![false; rows],
            null_probe,
            matched_null: if null_aware {
                vec![false; rows]
            } else {
                Vec::new()
            },
            matched_any: if null_aware {
                vec![false; rows]
            } else {
                Vec::new()
            },
            probe_position: 0,
            build_position: 0,
        });
        Ok(None)
    }

    /// One bounded candidate page in original probe/group order. Residual
    /// evaluation keeps the original full page boundary and never skips rows
    /// after a semi/anti match, since their required errors still matter.
    fn evaluate_page(
        &mut self,
        probe: &Chunk,
        selection: &mut JoinSelection,
        publish: bool,
    ) -> ExecutionResult<Option<TypedJoinBatch>> {
        if selection.is_empty() {
            return Ok(None);
        }
        let build = self.build_rows()?.ok_or_else(|| {
            hash_join_contract("compiled hash join candidate build rows are absent")
        })?;
        let schema = self.prepared_schemas()?.scope.clone();
        let owner = self.control.construction_owner(&schema)?;
        let scratch = self.control.construction_owner(&schema)?;
        let gathered = gather_probe_build_batches_typed(
            probe,
            &build,
            &selection.probe,
            &selection.build,
            &schema,
            true,
            self.plan.residual.is_none() && is_all_match_one(selection, probe.len()),
            owner.as_ref(),
            scratch.as_ref(),
            &self.control,
        )
        .map_err(JoinGatherError::into_execution)?;
        let [candidates] = <[TypedJoinBatch; 1]>::try_from(gathered.batches).map_err(|_| {
            hash_join_contract("compiled hash join candidate page is not one batch")
        })?;
        let output = if let Some(site) = self.plan.residual {
            let instance = self
                .instances
                .as_mut()
                .and_then(|roots| roots.last_mut())
                .ok_or_else(|| {
                    hash_join_contract("compiled hash join residual instance is missing")
                })?;
            let truth = evaluate_all(instance, site, candidates.record_batch(), &self.control)?;
            let mask = true_mask(&truth)?;
            selection.compact_by_mask(&mask)?;
            if publish {
                Some(
                    candidates
                        .filter(&mask, owner.as_ref())
                        .map_err(JoinGatherError::into_execution)?,
                )
            } else {
                None
            }
        } else if publish {
            Some(candidates)
        } else {
            None
        };
        Ok(output.filter(|batch| batch.num_rows() > 0))
    }

    fn advance_probe(&mut self, cursor: &mut HashProbeCursor) -> ExecutionResult<StepResult> {
        let null_aware = self.plan.join_type == JoinType::NullAwareLeftAnti;
        let publish = matches!(self.plan.join_type, JoinType::Inner | JoinType::LeftOuter);
        match cursor.phase {
            HashProbePhase::EqualPairs => {
                let mut selection = match self.loaded()?.table.as_ref() {
                    Some(table) => cursor
                        .pairs
                        .as_mut()
                        .expect("pair cursor installed")
                        .next_page(
                            table,
                            NonZeroUsize::new(MAX_JOIN_OUTPUT_ROWS_PER_BATCH)
                                .expect("positive output page size"),
                            &self.control,
                        )
                        .map_err(|cause| self.control.project_required_failure(cause))?,
                    None => JoinSelection::new(),
                };
                let output = self.evaluate_page(&cursor.input, &mut selection, publish)?;
                mark(&mut cursor.matched, &selection);
                if self.loaded()?.table.is_none()
                    || cursor
                        .pairs
                        .as_ref()
                        .expect("pair cursor installed")
                        .is_complete()
                {
                    cursor.pairs = None;
                    cursor.phase = if null_aware {
                        HashProbePhase::NullKeys
                    } else {
                        HashProbePhase::Tail
                    };
                }
                let output = output.map(|batch| self.output_of(batch)).transpose()?;
                let output = output.map(|batch| self.publish_typed(batch)).transpose()?;
                Ok(StepResult::new(output, NextStep::Runnable, 0))
            }
            HashProbePhase::NullKeys | HashProbePhase::NullProbe => {
                let null_keys = matches!(cursor.phase, HashProbePhase::NullKeys);
                let null_rows = self.loaded()?.null_key_rows.clone().ok_or_else(|| {
                    hash_join_contract("compiled null-aware anti join requires build null-key rows")
                })?;
                let build_rows = if null_keys {
                    null_rows.len()
                } else {
                    self.loaded()?.row_count
                };
                let mut selection = JoinSelection::new();
                while cursor.probe_position < cursor.input.len()
                    && selection.len() < MAX_JOIN_OUTPUT_ROWS_PER_BATCH
                {
                    if build_rows == 0 || (!null_keys && !cursor.null_probe[cursor.probe_position])
                    {
                        cursor.probe_position += 1;
                        cursor.build_position = 0;
                        continue;
                    }
                    let probe = u32::try_from(cursor.probe_position).map_err(|_| {
                        ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent)
                    })?;
                    let build = if null_keys {
                        null_rows[cursor.build_position]
                    } else {
                        u32::try_from(cursor.build_position).map_err(|_| {
                            ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent)
                        })?
                    };
                    selection.push(probe, build);
                    cursor.build_position += 1;
                    if cursor.build_position == build_rows {
                        cursor.probe_position += 1;
                        cursor.build_position = 0;
                    }
                }
                self.evaluate_page(&cursor.input, &mut selection, false)?;
                mark(
                    if null_keys {
                        &mut cursor.matched_null
                    } else {
                        &mut cursor.matched_any
                    },
                    &selection,
                );
                if cursor.probe_position == cursor.input.len() {
                    cursor.probe_position = 0;
                    cursor.build_position = 0;
                    cursor.phase = if null_keys {
                        HashProbePhase::NullProbe
                    } else {
                        HashProbePhase::Tail
                    };
                }
                Ok(StepResult::new(None, NextStep::Runnable, 0))
            }
            HashProbePhase::Tail => {
                let output = match self.plan.join_type {
                    JoinType::Inner => None,
                    JoinType::LeftOuter => {
                        let unmatched = cursor
                            .matched
                            .iter()
                            .enumerate()
                            .filter(|(_, matched)| !**matched)
                            .map(|(row, _)| {
                                u32::try_from(row).map_err(|_| {
                                    ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent)
                                })
                            })
                            .collect::<ExecutionResult<Vec<_>>>()?;
                        let schemas = self.prepared_schemas()?;
                        let owner = self.control.construction_owner(&schemas.output)?;
                        let scratch = self.control.construction_owner(&schemas.output)?;
                        gather_left_with_null_right_typed(
                            &cursor.input,
                            &unmatched,
                            &schemas.build,
                            &schemas.output,
                            owner.as_ref(),
                            scratch.as_ref(),
                            &self.control,
                        )
                        .map_err(JoinGatherError::into_execution)?
                        .map(|batch| self.publish_typed(batch))
                        .transpose()?
                    }
                    JoinType::LeftSemi => self.keep_probe(&cursor.input, &cursor.matched)?,
                    JoinType::LeftAnti => self.keep_probe(
                        &cursor.input,
                        &cursor
                            .matched
                            .iter()
                            .map(|value| !value)
                            .collect::<Vec<_>>(),
                    )?,
                    JoinType::NullAwareLeftAnti => self.keep_probe(
                        &cursor.input,
                        &(0..cursor.input.len())
                            .map(|row| {
                                if cursor.null_probe[row] {
                                    !cursor.matched_any[row]
                                } else {
                                    !(cursor.matched[row] || cursor.matched_null[row])
                                }
                            })
                            .collect::<Vec<_>>(),
                    )?,
                    other => {
                        return Err(program_binding_failure(
                            PlanBindingKind::InvalidAssignment,
                            format!("compiled hash join {other:?} is not executable yet"),
                        ));
                    }
                };
                Ok(StepResult::new(output, NextStep::NeedInput, 0))
            }
        }
    }

    fn output_of(&self, batch: TypedJoinBatch) -> ExecutionResult<TypedJoinBatch> {
        let schema = self.prepared_schemas()?.output.clone();
        let owner = self.control.construction_owner(&schema)?;
        batch
            .republish(schema, owner.as_ref())
            .map_err(JoinGatherError::into_execution)
    }
    fn publish_typed(&self, batch: TypedJoinBatch) -> ExecutionResult<Chunk> {
        let owner = self
            .control
            .construction_owner(&self.prepared_schemas()?.output)?;
        batch
            .into_chunk(owner.as_ref())
            .map_err(JoinGatherError::into_execution)
    }
    fn keep_probe(&self, probe: &Chunk, keep: &[bool]) -> ExecutionResult<Option<Chunk>> {
        if !keep.iter().any(|keep| *keep) {
            return Ok(None);
        }
        let schema = self.prepared_schemas()?.output.clone();
        let owner = self.control.construction_owner(&schema)?;
        let filtered = filter_probe_typed(
            probe,
            &BooleanArray::from(keep.to_vec()),
            schema,
            owner.as_ref(),
        )
        .map_err(JoinGatherError::into_execution)?;
        self.publish_typed(filtered).map(Some)
    }
}

fn mark(matched: &mut [bool], selection: &JoinSelection) {
    for &row in &selection.probe {
        if let Some(slot) = matched.get_mut(row as usize) {
            *slot = true;
        }
    }
}

/// The join scope is read positionally as the probe columns followed by the
/// build columns, so its frozen layout must be exactly the left layout then
/// the right layout: the same slots and the same complete fields.
pub(crate) fn validate_join_scope(
    node: ProgramNodeId,
    left: &novarocks_local_program::StaticLayout,
    right: &novarocks_local_program::StaticLayout,
    scope: &novarocks_local_program::StaticLayout,
) -> ExecutionResult<()> {
    let slots = scope
        .slots()
        .iter()
        .eq(left.slots().iter().chain(right.slots()));
    let fields = scope.schema().fields().iter().eq(left
        .schema()
        .fields()
        .iter()
        .chain(right.schema().fields()));
    let complete = left.is_typed()
        && right.is_typed()
        && scope.is_typed()
        && scope.slots().len() == left.slots().len() + right.slots().len()
        && (0..scope.slots().len()).all(|index| {
            let source = if index < left.slots().len() {
                left.value_binding_at(index)
            } else {
                right.value_binding_at(index - left.slots().len())
            };
            match (source, scope.value_binding_at(index)) {
                (Some(source), Some(actual)) => source == actual,
                _ => false,
            }
        });
    if slots && fields && complete {
        Ok(())
    } else {
        Err(crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "compiled join at local node {} has a scope other than its left then right layout",
                node.index()
            ),
        ))
    }
}

/// TRUE rows of a Boolean root: FALSE and NULL are both excluded.
pub(crate) fn true_mask(truth: &ArrayRef) -> ExecutionResult<BooleanArray> {
    let truth = truth
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ExecutionFailure::from(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled join predicate is not Boolean",
            )))
        })?;
    Ok(if truth.null_count() == 0 {
        truth.clone()
    } else {
        truth
            .iter()
            .map(|value| Some(value == Some(true)))
            .collect()
    })
}
