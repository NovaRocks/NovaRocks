// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Installed Q workers over the one immutable paged build. Q completion is a
//! separate continuation after the driver's last output transfer.

use super::super::{
    CanonicalJoinOutput, JoinPairRow, PagedJoinGather, ProjectedJoinGather,
    build_finalize::{BuildLookup, FinalizedBuild, JoinBuildFrame},
    build_projection::IndexedBuild,
    build_state::{ProjectionDomains, build_contract},
    join_completion::{RemainingDemand, funded_vec},
    naa_index::{NaaCandidateStep, NaaCandidateStore, NaaCandidates, NaaProbePage, NaaRowStart},
};
use super::frame::CompiledJoinFrame;
use super::*;
use crate::exec::{
    hash_table::key_index::{
        BulkMatch, BulkPresenceBatch, BulkProbeBatch, BulkWork, IndexPurpose, KeyInput, KeySpec,
        PreparedKeyBatch,
    },
    operators::compiled_expression::{
        evaluate_chunk_all, evaluate_projected_chunk_all, evaluate_projected_chunk_rows,
    },
    pipeline::{operator::OperatorLifecycle, shape::ControlStep, step_context::TurnWork},
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_value_arrays::reachability::ReachableRows;
use std::ops::Range;

// A cooperative working page, never a limit on input, keys or output extent.
const PAIR_PAGE: usize = 512;

pub(crate) struct CompiledJoinProbeProcessorFactory {
    declaration: Arc<CompiledJoinFrame>,
    error: Arc<RuntimeErrorState>,
    name: String,
}
impl CompiledJoinProbeProcessorFactory {
    pub(crate) fn new(declaration: Arc<CompiledJoinFrame>, error: Arc<RuntimeErrorState>) -> Self {
        Self {
            name: format!("COMPILED_JOIN_PROBE (node={})", declaration.node().index()),
            declaration,
            error,
        }
    }
}
impl OperatorFactory for CompiledJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        if usize::try_from(dop).ok() != Some(self.declaration.participants.probes) {
            return Err(build_contract(
                "join probe pipeline differs from its installed Q",
            ));
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let slot = usize::try_from(driver_id)
            .ok()
            .filter(|&id| id < self.declaration.participants.probes);
        let invalid = self.bind_pipeline_dop(dop).err().or_else(|| {
            slot.is_none()
                .then(|| build_contract("join probe has no installed Q slot"))
        });
        Box::new(Transform::hydrating(JoinProbe {
            name: self.name.clone(),
            declaration: self.declaration.clone(),
            error: self.error.clone(),
            slot,
            control: RuntimeKernelControl::new(self.error.clone()),
            construction: None,
            tracker: None,
            frame: None,
            prepared: None,
            roots: None,
            loaded: None,
            cursor: None,
            residual_gather: None,
            prepare_failure: invalid,
            active: false,
            normal: false,
            b0_ready: false,
            conversions: ValueConversionContext::default(),
        }))
    }
}
struct ProbeDefinition {
    kind: JoinType,
    naa: bool,
    payload_only: bool,
    scope_keys: usize,
    key_count: usize,
    sites: Vec<ProgramExpressionRootSite>,
    bindings: Vec<FunctionValueType>,
    predicate: Option<ProgramExpressionRootSite>,
    _storage: ValueTypeStorageLifetime,
}
impl ProbeDefinition {
    fn new(
        declaration: &CompiledJoinFrame,
        owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        let node = declaration.node();
        let graph_node = &declaration.program.graph().nodes()[node.index()];
        let kind;
        let naa;
        let scope_keys;
        let key_count;
        let predicate;
        let mut sites;
        match graph_node.kind() {
            ProgramNodeKind::Join {
                join_type,
                condition,
                ..
            } => {
                kind = match join_type {
                    novarocks_local_program::JoinType::Inner => JoinType::Inner,
                    novarocks_local_program::JoinType::LeftOuter => JoinType::LeftOuter,
                    novarocks_local_program::JoinType::RightOuter => JoinType::RightOuter,
                    novarocks_local_program::JoinType::FullOuter => JoinType::FullOuter,
                    novarocks_local_program::JoinType::LeftSemi => JoinType::LeftSemi,
                    novarocks_local_program::JoinType::RightSemi => JoinType::RightSemi,
                    novarocks_local_program::JoinType::LeftAnti => JoinType::LeftAnti,
                    novarocks_local_program::JoinType::RightAnti => JoinType::RightAnti,
                    novarocks_local_program::JoinType::NullAwareLeftAnti => {
                        JoinType::NullAwareLeftAnti
                    }
                };
                naa = condition.is_null_aware();
                scope_keys = condition.scope_keys().len();
                key_count = scope_keys
                    .checked_add(condition.keys().len())
                    .ok_or_else(super::super::join_completion::arithmetic)?;
                sites = funded_vec(
                    owner,
                    key_count + usize::from(condition.residual_predicate().is_some()),
                )?;
                let site = |role| ProgramExpressionRootSite::Node { node, role };
                for key in 0..scope_keys {
                    let key = u32::try_from(key).map_err(|_| {
                        build_contract("join scope ordinal exceeds its frozen u32 representation")
                    })?;
                    sites.push(site(ProgramNodeExpressionRole::NaaScopeProbeKey { key }));
                }
                sites.extend(declaration.hash_plan()?.probe_sites.iter().copied());
                predicate = condition.residual_predicate().map(|_| {
                    site(if naa {
                        ProgramNodeExpressionRole::NaaEligibility
                    } else {
                        ProgramNodeExpressionRole::JoinResidual
                    })
                });
            }
            ProgramNodeKind::NestedLoopJoin {
                join_type,
                join_conjunct,
                ..
            } => {
                kind = match join_type {
                    novarocks_local_program::NestedLoopJoinType::Inner
                    | novarocks_local_program::NestedLoopJoinType::Cross => JoinType::Inner,
                    novarocks_local_program::NestedLoopJoinType::LeftOuter => JoinType::LeftOuter,
                    novarocks_local_program::NestedLoopJoinType::RightOuter => JoinType::RightOuter,
                    novarocks_local_program::NestedLoopJoinType::FullOuter => JoinType::FullOuter,
                    novarocks_local_program::NestedLoopJoinType::LeftSemi => JoinType::LeftSemi,
                    novarocks_local_program::NestedLoopJoinType::LeftAnti => JoinType::LeftAnti,
                    novarocks_local_program::NestedLoopJoinType::NullAwareLeftAnti => {
                        JoinType::NullAwareLeftAnti
                    }
                };
                naa = false;
                scope_keys = 0;
                key_count = 0;
                predicate = join_conjunct.map(|_| ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::NestedLoopPredicate,
                });
                sites = funded_vec(owner, usize::from(predicate.is_some()))?;
            }
            _ => {
                return Err(build_contract(
                    "probe declaration is not its original join node",
                ));
            }
        }
        let mut bindings = funded_vec(owner, key_count)?;
        for &site in &sites {
            bindings.push(root_value_type(&declaration.program, site)?);
        }
        sites.extend(predicate);
        Ok(Self {
            kind,
            naa,
            payload_only: declaration.payload_only(),
            scope_keys,
            key_count,
            sites,
            bindings,
            predicate,
            _storage: owner.storage_lifetime(),
        })
    }
    fn empty_output_on_b0(&self) -> bool {
        matches!(
            self.kind,
            JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightOuter
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }
    fn emits_pairs(&self) -> bool {
        matches!(
            self.kind,
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter
        )
    }
}
struct ProbePreparation {
    probe: ChunkSchemaRef,
    schemas: PreparedJoinSchemas,
    definition: ProbeDefinition,
    output: PagedJoinGather,
}
enum LoadedLookup {
    PayloadOnly,
    Ordinary(Arc<IndexedBuild>),
    Naa(NaaCandidateStore),
}
struct Loaded {
    data: Arc<FinalizedBuild>,
    lookup: LoadedLookup,
}
enum ProbeLookup {
    PayloadOnly,
    Membership(BulkPresenceBatch),
    Ordinary(BulkProbeBatch),
    Naa(NaaProbePage),
}
enum RowCandidates {
    Empty,
    Csr(Range<usize>),
    Physical(Range<usize>),
    Naa(NaaCandidates),
}
struct ProbeCursor {
    input: Chunk,
    lookup: ProbeLookup,
    row: usize,
    candidate: Option<RowCandidates>,
    matched: Vec<bool>,
    _storage: ValueTypeStorageLifetime,
}
struct JoinProbe {
    name: String,
    declaration: Arc<CompiledJoinFrame>,
    error: Arc<RuntimeErrorState>,
    slot: Option<usize>,
    control: RuntimeKernelControl,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    tracker: Option<Arc<MemTracker>>,
    frame: Option<Arc<JoinBuildFrame>>,
    prepared: Option<ProbePreparation>,
    roots: Option<Vec<CompiledExpressionInstance>>,
    loaded: Option<Loaded>,
    cursor: Option<ProbeCursor>,
    residual_gather: Option<ProjectedJoinGather>,
    prepare_failure: Option<ExecutionFailure>,
    active: bool,
    normal: bool,
    b0_ready: bool,
    conversions: ValueConversionContext,
}
impl JoinProbe {
    fn frame(&self) -> ExecutionResult<&Arc<JoinBuildFrame>> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        self.frame
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    fn preparation(&self) -> ExecutionResult<&ProbePreparation> {
        self.frame()?;
        self.prepared
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    fn failure(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.control.project_required_failure(cause);
        match (&self.frame, self.slot) {
            (Some(frame), Some(slot)) => {
                let cause = frame.fail(cause);
                frame.completion.fail_probe(slot, cause)
            }
            _ => cause,
        }
    }
    fn release(&mut self) {
        self.cursor = None;
        self.loaded = None;
        self.roots = None;
        self.residual_gather = None;
    }
    fn settle_exit(&mut self) -> ExecutionResult<()> {
        if let (Some(frame), Some(slot)) = (&self.frame, self.slot) {
            if let Some(cause) = self.error.error() {
                self.failure(cause);
            } else {
                frame.completion.withdraw_if_pending(slot)?;
            }
        }
        self.release();
        Ok(())
    }
    fn load_if_published(&mut self) -> ExecutionResult<bool> {
        if self.loaded.is_some() {
            return Ok(true);
        }
        let Some(published) = self.frame()?.completion.published()? else {
            return Ok(false);
        };
        let definition = &self.preparation()?.definition;
        let lookup = match &published.artifact.lookup {
            BuildLookup::PayloadOnly if definition.payload_only => LoadedLookup::PayloadOnly,
            BuildLookup::Ordinary(index) if !definition.naa && !definition.payload_only => {
                LoadedLookup::Ordinary(index.clone())
            }
            BuildLookup::Naa { .. } if definition.naa => LoadedLookup::Naa(NaaCandidateStore::new(
                &published.artifact,
                definition.scope_keys..definition.key_count,
                definition.predicate.is_some(),
            )?),
            _ => {
                return Err(build_contract(
                    "probe publication differs from original join roles",
                ));
            }
        };
        self.loaded = Some(Loaded {
            data: published.artifact,
            lookup,
        });
        Ok(true)
    }
    fn normal_after_output(&mut self) -> ExecutionResult<StepResult> {
        if !self.normal {
            self.frame()?.completion.normal_after_output(
                self.slot
                    .ok_or_else(|| build_contract("probe has no original Q slot"))?,
            )?;
            self.normal = true;
        }
        self.release();
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
    fn step_result(
        &mut self,
        ctx: &mut StepContext<'_>,
        result: ExecutionResult<StepResult>,
    ) -> ExecutionResult<StepResult> {
        result.map_err(|cause| {
            let cause = self.failure(cause);
            ctx.report_failure(cause.clone());
            self.release();
            cause
        })
    }
}
impl OperatorLifecycle for JoinProbe {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(tracker.clone());
        self.tracker = Some(tracker);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(first) = &self.prepare_failure {
            return Err(first.clone());
        }
        if self.prepared.is_some() {
            return Ok(());
        }
        let result = (|| -> ExecutionResult<()> {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let probe = factory.create_compiled_chunk_schema(self.declaration.probe_layout()?)?;
            let schemas = prepare_join_schemas(
                factory.as_ref(),
                self.declaration.build_layout(),
                self.declaration.scope_layout()?,
                self.declaration.output_layout(),
            )?;
            let owner = self.control.construction_owner(&probe)?;
            let definition = ProbeDefinition::new(&self.declaration, owner.as_ref())?;
            let side = match definition.kind {
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti => {
                    CanonicalJoinOutput::Probe
                }
                JoinType::RightSemi | JoinType::RightAnti => CanonicalJoinOutput::Build,
                _ => CanonicalJoinOutput::Pairs,
            };
            let output = PagedJoinGather::new_canonical(
                schemas.output.clone(),
                &probe,
                &schemas.build,
                side,
                owner.as_ref(),
            )
            .map_err(JoinGatherError::into_execution)?;
            let work = BulkWork {
                owner: owner.as_ref(),
                allocator: AggregateAllocator::new(
                    self.tracker
                        .clone()
                        .ok_or(ConstructionFailure::MissingOwner)?,
                ),
                host: self
                    .control
                    .allocator()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                control: &self.control,
            };
            let frame = self.declaration.prepare(&work)?;
            self.frame = Some(frame);
            self.prepared = Some(ProbePreparation {
                probe,
                schemas,
                definition,
                output,
            });
            Ok(())
        })();
        if let Err(cause) = &result {
            self.prepare_failure = Some(cause.clone());
        }
        result
    }
    fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
        let owner = self
            .control
            .construction_owner(&self.preparation()?.probe)?;
        let work = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(
                self.tracker
                    .clone()
                    .ok_or(ConstructionFailure::MissingOwner)?,
            ),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        };
        self.frame()?.activate(&work)?;
        self.active = true;
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.settle_exit()
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.settle_exit()
    }
    fn cancel(&mut self) {
        if let Err(cause) = self.settle_exit() {
            self.error.report_cleanup(&cause);
        }
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
}
impl InputEncodingPolicy for JoinProbe {}
impl TransformKernel for JoinProbe {
    fn control_observable(&self) -> Option<Arc<crate::runtime::observable::Observable>> {
        self.frame
            .as_ref()
            .map(|frame| frame.completion.observable())
    }
    fn poll_receiver_control(
        &mut self,
        _: &mut StepContext<'_>,
        receiver_empty: bool,
    ) -> ExecutionResult<ControlStep> {
        self.frame()?.completion.check()?;
        if let Some(cause) = self.error.error() {
            return Err(self.failure(cause));
        }
        if self.normal {
            return Ok(ControlStep::Active);
        }
        if self.frame()?.completion.remaining_demand()? == RemainingDemand::Released {
            self.frame()?.completion.withdraw_if_pending(
                self.slot
                    .ok_or_else(|| build_contract("probe has no original Q slot"))?,
            )?;
            self.release();
            return Ok(ControlStep::Withdrawn);
        }
        if self.load_if_published()?
            && self.preparation()?.definition.empty_output_on_b0()
            && self
                .loaded
                .as_ref()
                .expect("published build")
                .data
                .rows
                .len()
                == 0
        {
            // Accepted hydration/roots must run before this flag can be set.
            self.b0_ready = receiver_empty && self.cursor.is_none();
            return Ok(ControlStep::Runnable);
        }
        Ok(ControlStep::Active)
    }
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        if !self.active {
            return Err(build_contract("join probe is not activated"));
        }
        let frame = self.frame()?;
        let observable = frame.completion.observable();
        let observed_generation = observable.generation();
        if frame.completion.published()?.is_some() {
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
        let mut accepted = match input.into_uncovered_chunk() {
            Ok(input) => Some(input),
            Err(cause) => return self.step_result(ctx, Err(cause)),
        };
        let result = (|| -> ExecutionResult<StepResult> {
            if self.cursor.is_some() {
                return Err(build_contract("join probe overwrote admitted work"));
            }
            if !self.load_if_published()? {
                return Err(build_contract(
                    "join probe accepted input before publication",
                ));
            }
            let chunk = accepted.as_ref().expect("original admitted input");
            let prep = self
                .prepared
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            instances(
                &mut self.roots,
                &self.declaration.program,
                &prep.definition.sites,
                &self.control,
            )?;
            let owner = self.control.construction_owner(&prep.probe)?;
            if prep.definition.predicate.is_some() && self.residual_gather.is_none() {
                let instance = self
                    .roots
                    .as_ref()
                    .and_then(|roots| roots.last())
                    .ok_or_else(|| build_contract("join predicate has no original instance"))?;
                self.residual_gather = Some(
                    ProjectedJoinGather::new(
                        prep.schemas.scope.clone(),
                        &prep.probe,
                        &prep.schemas.build,
                        instance,
                        owner.as_ref(),
                    )
                    .map_err(JoinGatherError::into_execution)?,
                );
            }
            let source_owner = source_key_owner(&chunk, owner.as_ref())?;
            let work = BulkWork {
                owner: source_owner.as_ref(),
                allocator: AggregateAllocator::new(
                    self.tracker
                        .clone()
                        .ok_or(ConstructionFailure::MissingOwner)?,
                ),
                host: self
                    .control
                    .allocator()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                control: &self.control,
            };
            let lookup = prepare_lookup(
                &prep.definition,
                self.roots.as_mut().expect("installed roots"),
                self.loaded.as_ref().expect("published build"),
                self.frame.as_ref().expect("prepared frame"),
                &chunk,
                &mut self.conversions,
                &work,
                &self.control,
            )?;
            let mut matched = funded_vec(owner.as_ref(), chunk.len())?;
            matched.resize(chunk.len(), false);
            let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
            self.cursor = Some(ProbeCursor {
                input: accepted.take().expect("original admitted input"),
                lookup,
                row: 0,
                candidate: None,
                matched,
                _storage: owner.storage_lifetime(),
            });
            Ok(StepResult::new(None, NextStep::Runnable, rows))
        })();
        let result = self.step_result(ctx, result);
        drop(accepted);
        result
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let mut admitted = self.cursor.take();
        let result = (|| -> ExecutionResult<StepResult> {
            if self.b0_ready && admitted.is_none() {
                return self.normal_after_output();
            }
            let Some(cursor) = admitted.as_mut() else {
                return Ok(StepResult::new(None, NextStep::NeedInput, 0));
            };
            let prep = self
                .prepared
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = self.control.construction_owner(&prep.probe)?;
            let work = BulkWork {
                owner: owner.as_ref(),
                allocator: AggregateAllocator::new(
                    self.tracker
                        .clone()
                        .ok_or(ConstructionFailure::MissingOwner)?,
                ),
                host: self
                    .control
                    .allocator()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                control: &self.control,
            };
            advance_probe(
                cursor,
                prep,
                self.loaded.as_ref().expect("published build"),
                self.roots.as_mut().expect("installed roots"),
                self.residual_gather.as_ref(),
                ctx.work(),
                &work,
                &self.control,
            )
        })();
        let result = self.step_result(ctx, result);
        if result.is_ok()
            && admitted
                .as_ref()
                .is_some_and(|cursor| cursor.row < cursor.input.len())
        {
            self.cursor = admitted.take();
        }
        drop(admitted);
        result
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let result = if self.cursor.is_some() {
            Err(build_contract(
                "join probe reached EOS with admitted candidate work",
            ))
        } else {
            self.normal_after_output()
        };
        self.step_result(ctx, result)
    }
}
fn capture_input(
    spec: &KeySpec,
    arrays: &[ArrayRef],
    bindings: &[FunctionValueType],
    rows: usize,
    conversions: &mut ValueConversionContext,
    work: &BulkWork<'_>,
) -> ExecutionResult<KeyInput> {
    let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
    let readable = ReachableRows::all(rows)
        .materialize_observed(
            &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
        )
        .map_err(|cause| match cause {
            novarocks_value_arrays::ValueFailure::Value(cause) => {
                ExecutionFailure::from_domain(cause)
            }
            novarocks_value_arrays::ValueFailure::Caller(cause) => cause,
        })?;
    let mut occurrences = funded_vec(work.owner, rows)?;
    for row in 0..rows {
        if row % 256 == 0 {
            checkpoint()?;
        }
        occurrences.push(row);
    }
    KeyInput::new_for_spec(
        spec,
        arrays,
        bindings,
        &readable,
        &occurrences,
        conversions,
        work.owner,
        &checkpoint,
    )
}
#[allow(clippy::too_many_arguments)]
fn prepare_lookup(
    definition: &ProbeDefinition,
    roots: &mut [CompiledExpressionInstance],
    loaded: &Loaded,
    frame: &JoinBuildFrame,
    input: &Chunk,
    conversions: &mut ValueConversionContext,
    work: &BulkWork<'_>,
    control: &RuntimeKernelControl,
) -> ExecutionResult<ProbeLookup> {
    if definition.payload_only {
        return Ok(ProbeLookup::PayloadOnly);
    }
    let mut arrays = funded_vec(work.owner, definition.key_count)?;
    for (instance, &site) in roots
        .iter_mut()
        .zip(&definition.sites)
        .take(definition.key_count)
    {
        arrays.push(evaluate_chunk_all(instance, site, input, control)?);
    }
    match &loaded.lookup {
        LoadedLookup::PayloadOnly => Err(build_contract("hash probe has payload-only lookup")),
        LoadedLookup::Ordinary(index) => {
            let input = capture_input(
                index.view.spec(),
                &arrays,
                &definition.bindings,
                input.len(),
                conversions,
                work,
            )?;
            let prepared = PreparedKeyBatch::from_captured(
                index.view.spec(),
                &input,
                work.allocator.clone(),
                &|| control.checkpoint(0).map_err(Into::into),
            )?;
            Ok(if index.view.purpose() == IndexPurpose::PresenceOnly {
                ProbeLookup::Membership(index.view.contains(&prepared, None, work.owner, control)?)
            } else {
                ProbeLookup::Ordinary(index.view.probe(&prepared, None, work.owner, control)?)
            })
        }
        LoadedLookup::Naa(store) => {
            let full = capture_input(
                store.full_spec(),
                &arrays,
                &definition.bindings,
                input.len(),
                conversions,
                work,
            )?;
            let ProjectionDomains::Naa {
                group_full_components,
                ..
            } = &frame.domains
            else {
                return Err(build_contract("NAA probe lost its original projection"));
            };
            let group = if let Some(spec) = store.group_spec() {
                let mut group_arrays = funded_vec(work.owner, group_full_components.len())?;
                let mut group_bindings = funded_vec(work.owner, group_full_components.len())?;
                for &column in group_full_components {
                    control.checkpoint(1)?;
                    group_arrays.push(arrays[column].clone());
                    group_bindings.push(definition.bindings[column].clone());
                }
                Some(capture_input(
                    spec,
                    &group_arrays,
                    &group_bindings,
                    input.len(),
                    conversions,
                    work,
                )?)
            } else {
                None
            };
            Ok(ProbeLookup::Naa(store.prepare_probe(full, group, work)?))
        }
    }
}
fn begin_row(cursor: &mut ProbeCursor, loaded: &Loaded) -> ExecutionResult<RowCandidates> {
    let row = cursor.row;
    Ok(match (&cursor.lookup, &loaded.lookup) {
        (ProbeLookup::PayloadOnly, LoadedLookup::PayloadOnly) => {
            RowCandidates::Physical(0..loaded.data.rows.len())
        }
        (ProbeLookup::Membership(present), LoadedLookup::Ordinary(_)) => {
            cursor.matched[row] = present.present()[row] != 0;
            RowCandidates::Empty
        }
        (ProbeLookup::Ordinary(matches), LoadedLookup::Ordinary(index)) => {
            match matches.matches()[row] {
                BulkMatch::Found(key) => RowCandidates::Csr(
                    index
                        .groups
                        .as_ref()
                        .ok_or_else(|| build_contract("row-matching index has no CSR"))?
                        .group(key)?,
                ),
                BulkMatch::Miss | BulkMatch::Unmatchable => RowCandidates::Empty,
            }
        }
        (ProbeLookup::Naa(page), LoadedLookup::Naa(store)) => match store.start(page, row)? {
            NaaRowStart::Decided { suppressed } => {
                cursor.matched[row] = suppressed;
                RowCandidates::Empty
            }
            NaaRowStart::Candidates(cursor) => RowCandidates::Naa(cursor),
        },
        _ => {
            return Err(build_contract(
                "probe page differs from its original published index",
            ));
        }
    })
}
fn next_row(
    candidate: &mut RowCandidates,
    loaded: &Loaded,
    turn: &mut TurnWork,
    work: &BulkWork<'_>,
) -> ExecutionResult<NaaCandidateStep> {
    work.control.checkpoint(0)?;
    match candidate {
        RowCandidates::Empty => Ok(NaaCandidateStep::Finished),
        RowCandidates::Naa(cursor) => match &loaded.lookup {
            LoadedLookup::Naa(store) => cursor.next(store, turn, work),
            _ => Err(build_contract(
                "NAA cursor lost its original candidate store",
            )),
        },
        RowCandidates::Csr(range) | RowCandidates::Physical(range) => {
            if range.start == range.end {
                return Ok(NaaCandidateStep::Finished);
            }
            if !turn.try_consume(1) {
                return Ok(NaaCandidateStep::Yielded);
            }
            let position = range.next().expect("nonempty physical range");
            let row = if let RowCandidates::Csr(_) = candidate {
                let LoadedLookup::Ordinary(index) = &loaded.lookup else {
                    return Err(build_contract("CSR candidate lost its original index"));
                };
                index
                    .groups
                    .as_ref()
                    .expect("validated row CSR")
                    .row(position)?
            } else {
                position
            };
            Ok(NaaCandidateStep::Row(loaded.data.rows.row(row)?))
        }
    }
}
#[allow(clippy::too_many_arguments)]
fn advance_probe(
    cursor: &mut ProbeCursor,
    prep: &ProbePreparation,
    loaded: &Loaded,
    roots: &mut [CompiledExpressionInstance],
    residual: Option<&ProjectedJoinGather>,
    turn: &mut TurnWork,
    work: &BulkWork<'_>,
    control: &RuntimeKernelControl,
) -> ExecutionResult<StepResult> {
    let closed_start = cursor.row;
    let mut pairs = funded_vec(work.owner, PAIR_PAGE.min(turn.remaining() as usize))?;
    let mut yielded = false;
    while cursor.row < cursor.input.len() && pairs.len() < PAIR_PAGE {
        if cursor.candidate.is_none() {
            if !turn.try_consume(1) {
                yielded = true;
                break;
            }
            cursor.candidate = Some(begin_row(cursor, loaded)?);
        }
        match next_row(
            cursor.candidate.as_mut().expect("started original row"),
            loaded,
            turn,
            work,
        )? {
            NaaCandidateStep::Row(build) => pairs.push(JoinPairRow {
                probe: Some(cursor.row),
                build: Some(build),
            }),
            NaaCandidateStep::Yielded => {
                yielded = true;
                break;
            }
            NaaCandidateStep::Finished => {
                cursor.row += 1;
                cursor.candidate = None;
            }
        }
    }
    let mut comparison = funded_vec(work.owner, pairs.len())?;
    comparison.resize(pairs.len(), Some(true));
    if let (ProbeLookup::Naa(page), LoadedLookup::Naa(store)) = (&cursor.lookup, &loaded.lookup) {
        let mut probe_rows = funded_vec(work.owner, pairs.len())?;
        let mut build_rows = funded_vec(work.owner, pairs.len())?;
        for pair in &pairs {
            probe_rows.push(pair.probe.expect("probe pair"));
            build_rows.push(pair.build.expect("build pair"));
        }
        store.compare_pairs(page, &probe_rows, &build_rows, &mut comparison, work)?;
    }
    let mut eligible = funded_vec(work.owner, pairs.len())?;
    for (position, &c) in comparison.iter().enumerate() {
        if c != Some(false) {
            eligible.push(position);
        }
    }
    let predicate = if let Some(site) = prep.definition.predicate {
        if eligible.is_empty() {
            None
        } else {
            let gather = residual.ok_or_else(|| {
                build_contract("join predicate has no original minimal projection")
            })?;
            // Keep the original pair-page row domain. Sparse eligibility
            // changes demand, not the batch-row identity of a required error.
            let input = gather
                .gather(
                    &cursor.input,
                    &loaded.data.rows,
                    &pairs,
                    work.owner,
                    &work.host,
                    control,
                )
                .map_err(JoinGatherError::into_execution)?;
            let instance = roots
                .last_mut()
                .ok_or_else(|| build_contract("join predicate has no original root instance"))?;
            Some(if eligible.len() == pairs.len() {
                evaluate_projected_chunk_all(
                    instance,
                    site,
                    &input,
                    gather.declared(),
                    gather.projection(),
                    control,
                )?
            } else {
                evaluate_projected_chunk_rows(
                    instance,
                    site,
                    &input,
                    gather.declared(),
                    gather.projection(),
                    &eligible,
                    control,
                )?
            })
        }
    } else {
        None
    };
    let truth = predicate
        .as_ref()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| build_contract("original join predicate did not produce Boolean"))
        })
        .transpose()?;
    let mut output = funded_vec(work.owner, pairs.len() + (cursor.row - closed_start))?;
    for (ordinal, &position) in eligible.iter().enumerate() {
        control.checkpoint(1)?;
        let c = comparison[position];
        let on = match truth {
            Some(truth) => {
                if truth.is_null(ordinal) {
                    None
                } else {
                    Some(truth.value(ordinal))
                }
            }
            None => c,
        };
        let suppress = if prep.definition.naa {
            if prep.definition.predicate.is_some() {
                on == Some(true)
            } else {
                c != Some(false)
            }
        } else if prep.definition.kind == JoinType::NullAwareLeftAnti {
            on != Some(false)
        } else {
            on == Some(true)
        };
        if suppress {
            let pair = pairs[position];
            cursor.matched[pair.probe.expect("actual probe row")] = true;
            if let Some(bits) = &loaded.data.matches {
                bits.mark(pair.build.expect("actual build row").index())?;
            }
            if prep.definition.emits_pairs() {
                output.push(pair);
            }
        }
    }
    for row in closed_start..cursor.row {
        control.checkpoint(1)?;
        let matched = cursor.matched[row];
        let emit = match prep.definition.kind {
            JoinType::LeftOuter
            | JoinType::FullOuter
            | JoinType::LeftAnti
            | JoinType::NullAwareLeftAnti => !matched,
            JoinType::LeftSemi => matched,
            _ => false,
        };
        if emit {
            output.push(JoinPairRow {
                probe: Some(row),
                build: None,
            });
        }
    }
    if prep.definition.naa
        && prep.definition.predicate.is_none()
        && cursor.row < cursor.input.len()
        && cursor.matched[cursor.row]
    {
        cursor.candidate = Some(RowCandidates::Empty);
    }
    let output = if output.is_empty() {
        None
    } else {
        Some(
            prep.output
                .gather(
                    Some(&cursor.input),
                    &loaded.data.rows,
                    &output,
                    work.owner,
                    &work.host,
                    control,
                )
                .map_err(JoinGatherError::into_execution)?
                .into_chunk(work.owner)
                .map_err(JoinGatherError::into_execution)?,
        )
    };
    let next = if cursor.row == cursor.input.len() {
        NextStep::NeedInput
    } else if yielded && output.is_none() {
        NextStep::Yield
    } else {
        NextStep::Runnable
    };
    Ok(StepResult::new(output, next, 0))
}
