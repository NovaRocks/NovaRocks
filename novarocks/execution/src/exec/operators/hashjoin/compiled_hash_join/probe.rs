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

//! Probe cursors and output for a compiled hash join.

use super::*;

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
