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

//! Build input collection for a compiled hash join.

use super::*;

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
