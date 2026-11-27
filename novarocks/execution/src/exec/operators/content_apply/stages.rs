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

//! The installed E/F/Q/T operators for one owner Task.

mod build;
mod output;

use build::{Finalize, capture_input, capture_work, drive_finalize};
use output::{ProbeCursor, drive_probe, drive_tail};
use std::{num::NonZeroU32, sync::OnceLock};

use arrow::array::ArrayRef;
use novarocks_local_program::{
    LocalProgram, ProgramContentApply, ProgramNodeId, ProgramNodeKind, StaticLayout,
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_types::SlotId;
use novarocks_value_arrays::reachability::SelectionSet;

use super::*;
use crate::{
    exec::{
        chunk::{Chunk, ChunkSchemaRef},
        hash_table::key_index::{BulkDomain, CaptureBuilder, SealedCapture},
        operators::{
            compiled_expression::RuntimeKernelControl,
            hashjoin::native_runtime_filter::NativeRuntimeFilterProducerFactory,
            local_exchanger::LocalExchanger,
            logical_change_summary::{SummaryUnitObligation, TaskSummaryState},
        },
        pipeline::{
            driver_step::DriverStep,
            input::PreparedInput,
            operator::{
                ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
                InputEncodingPolicy, Operator, OperatorLifecycle,
            },
            operator_factory::OperatorFactory,
            shape::{ControlStep, Source, SourceKernel, Transform, TransformKernel},
            step::{NextStep, StepResult, WaitFact, WaitReason},
        },
        shared_stage::{
            build_collection::BuildCollection,
            funded_shared, funded_vec,
            stage_completion::{RemainingDemand, StageCompletion, StageParticipants},
        },
    },
    runtime::{
        mem_tracker::MemTracker,
        observable::Observable,
        runtime_state::{RuntimeErrorState, RuntimeState},
    },
};

type SealedRun = (SealedCapture, CapturedWeights);
type Inputs = BuildCollection<SealedRun, ContentApplyIndex>;

pub(crate) struct ContentApplyStages {
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    participants: StageParticipants,
    handoff: Arc<LocalExchanger>,
    summary: TaskSummaryState,
    tracker: Arc<MemTracker>,
    output_rows: NonZeroUsize,
    error: Arc<RuntimeErrorState>,
    demand_producer: Option<NativeRuntimeFilterProducerFactory>,
    prepared: OnceLock<ExecutionResult<Arc<Frame>>>,
}

struct Frame {
    domain: BulkDomain,
    target_types: Vec<FunctionValueType>,
    inputs: Arc<Inputs>,
    completion: Arc<StageCompletion<ContentApplyIndex>>,
    _storage: ValueTypeStorageLifetime,
}

impl ContentApplyStages {
    #[allow(
        clippy::too_many_arguments,
        reason = "Each installed stage and Task owner is explicit"
    )]
    pub(crate) fn new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        participants: StageParticipants,
        handoff: Arc<LocalExchanger>,
        summary: TaskSummaryState,
        tracker: Arc<MemTracker>,
        output_rows: NonZeroU32,
        error: Arc<RuntimeErrorState>,
    ) -> Self {
        Self {
            program,
            node,
            participants,
            handoff,
            summary,
            tracker,
            output_rows: output_rows.try_into().expect("u32 fits the supported host"),
            error,
            demand_producer: None,
            prepared: OnceLock::new(),
        }
    }

    pub(crate) fn with_demand_producer(
        mut self,
        producer: Option<NativeRuntimeFilterProducerFactory>,
    ) -> Self {
        self.demand_producer = producer;
        self
    }

    fn spec(&self) -> ExecutionResult<&ProgramContentApply> {
        match self.program.graph().nodes()[self.node.index()].kind() {
            ProgramNodeKind::ContentApply(spec) => Ok(spec),
            _ => Err(contract("content stages have no original node")),
        }
    }

    fn prepare(&self, work: &BulkWork<'_>) -> ExecutionResult<Arc<Frame>> {
        self.prepared
            .get_or_init(|| {
                let spec = self.spec()?;
                let nodes = self.program.graph().nodes();
                let types = content_types(
                    nodes[spec.weighted_input.index()].output_layout(),
                    &spec.weighted_content,
                    work,
                )?;
                let target_types = content_types(
                    nodes[spec.target_input.index()].output_layout(),
                    &spec.target_content,
                    work,
                )?;
                let domain =
                    BulkDomain::new(&content_spec(&types, work)?, work.owner, work.control)?;
                let observable = funded_shared(work.owner, Observable::new())?;
                let producers = self
                    .participants
                    .probes
                    .checked_add(self.participants.tails)
                    .ok_or_else(arithmetic)?;
                let completion = StageCompletion::new(
                    self.participants,
                    Some(producers),
                    Some(self.handoff.clone()),
                    observable,
                    work.owner,
                    work.control,
                    contract,
                )?;
                let inputs = BuildCollection::new(completion.clone(), work.owner, work.control)?;
                funded_shared(
                    work.owner,
                    Frame {
                        domain,
                        target_types,
                        inputs,
                        completion,
                        _storage: work.owner.storage_lifetime(),
                    },
                )
            })
            .clone()
    }
}

fn content_types(
    layout: &StaticLayout,
    slots: &[SlotId],
    work: &BulkWork<'_>,
) -> ExecutionResult<Vec<FunctionValueType>> {
    let mut types = funded_vec(work.owner, slots.len())?;
    for &slot in slots {
        work.control.checkpoint(0)?;
        let binding = layout
            .value_binding(slot)
            .ok_or_else(|| contract("content input has no frozen value binding"))?;
        types.push(
            FunctionValueType::try_from_typed_binding(binding.clone())
                .map_err(ExecutionFailure::from_domain)?,
        );
    }
    Ok(types)
}

#[derive(Clone, Copy)]
pub(crate) enum ContentApplyPhase {
    Capture,
    Finalize,
    Probe,
    Tail,
}

impl ContentApplyPhase {
    fn name(self) -> &'static str {
        match self {
            Self::Capture => "CONTENT APPLY CAPTURE",
            Self::Finalize => "CONTENT APPLY FINALIZE",
            Self::Probe => "CONTENT APPLY PROBE",
            Self::Tail => "CONTENT APPLY TAIL",
        }
    }

    fn positions(self, participants: StageParticipants) -> usize {
        match self {
            Self::Capture => participants.inputs,
            Self::Finalize => participants.finalizers.get(),
            Self::Probe => participants.probes,
            Self::Tail => participants.tails,
        }
    }
}

pub(crate) struct ContentApplyFactory {
    stages: Arc<ContentApplyStages>,
    phase: ContentApplyPhase,
}

impl ContentApplyFactory {
    pub(crate) fn new(stages: Arc<ContentApplyStages>, phase: ContentApplyPhase) -> Self {
        Self { stages, phase }
    }
}

impl OperatorFactory for ContentApplyFactory {
    fn name(&self) -> &str {
        self.phase.name()
    }

    fn is_source(&self) -> bool {
        matches!(
            self.phase,
            ContentApplyPhase::Finalize | ContentApplyPhase::Tail
        )
    }

    fn is_sink(&self) -> bool {
        matches!(self.phase, ContentApplyPhase::Capture)
    }

    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        let units = self.phase.positions(self.stages.participants);
        if usize::try_from(dop).ok() != Some(units) {
            return Err(contract(
                "content stage width differs from installed participants",
            ));
        }
        if matches!(self.phase, ContentApplyPhase::Tail) {
            self.stages.summary.validate_units(units)?;
        }
        Ok(())
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let slot = self.bind_pipeline_dop(dop).and_then(|()| {
            usize::try_from(driver_id)
                .ok()
                .filter(|&slot| slot < self.phase.positions(self.stages.participants))
                .ok_or_else(|| contract("content driver has no installed stage slot"))
        });
        let operator = ContentOperator::new(self.stages.clone(), self.phase, slot);
        if self.is_source() {
            Box::new(Source::new(operator))
        } else {
            Box::new(Transform::hydrating(operator))
        }
    }
}

struct Prepared {
    schema: ChunkSchemaRef,
    frame: Arc<Frame>,
}

struct Host {
    stages: Arc<ContentApplyStages>,
    control: RuntimeKernelControl,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    prepared: Option<Prepared>,
    slot: ExecutionResult<usize>,
    #[cfg(novarocks_dev_controls)]
    drop_insert: Option<crate::runtime::runtime_state::ContentApplyInsertControl>,
}

impl Host {
    fn prepared(&self) -> ExecutionResult<&Prepared> {
        self.prepared
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }

    fn owner(&self) -> ExecutionResult<Box<dyn InputConstructionOwner>> {
        self.control.construction_owner(&self.prepared()?.schema)
    }

    fn work<'a>(&'a self, owner: &'a dyn InputConstructionOwner) -> ExecutionResult<BulkWork<'a>> {
        Ok(BulkWork {
            owner,
            allocator: AggregateAllocator::new(self.stages.tracker.clone()),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        })
    }

    fn fail(&self, ctx: &DriverStep<'_>, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.control.project_required_failure(cause);
        let first = match &self.prepared {
            Some(prepared) => prepared.frame.completion.fail(cause),
            None => cause,
        };
        ctx.report_failure(first.clone());
        first
    }

    fn remaining_demand(&self) -> ExecutionResult<RemainingDemand> {
        self.prepared()?.frame.completion.remaining_demand()
    }

    fn should_drop_insert(&self) -> ExecutionResult<bool> {
        #[cfg(novarocks_dev_controls)]
        if let Some(control) = &self.drop_insert {
            return control(i32::try_from(self.slot.clone()?).expect("installed i32 driver id"));
        }
        Ok(false)
    }
}

enum State {
    Capture {
        writer: Option<CaptureBuilder>,
        weights: Option<CapturedWeights>,
    },
    Finalize(Finalize),
    Probe(Option<ProbeCursor>),
    Tail {
        unit: Option<SummaryUnitObligation>,
        tail: Option<ContentApplyTail>,
    },
}

struct ContentOperator {
    name: String,
    host: Host,
    state: State,
    normal: bool,
    exited: bool,
    conversions: ValueConversionContext,
}

impl InputEncodingPolicy for ContentOperator {}

impl ContentOperator {
    fn new(
        stages: Arc<ContentApplyStages>,
        phase: ContentApplyPhase,
        mut slot: ExecutionResult<usize>,
    ) -> Self {
        let state = match phase {
            ContentApplyPhase::Capture => State::Capture {
                writer: None,
                weights: None,
            },
            ContentApplyPhase::Finalize => State::Finalize(Finalize::Collect),
            ContentApplyPhase::Probe => State::Probe(None),
            ContentApplyPhase::Tail => {
                let unit = match slot.as_ref() {
                    Ok(&ordinal) => match stages.summary.contributor(ordinal) {
                        Ok(unit) => Some(unit),
                        Err(cause) => {
                            slot = Err(cause);
                            None
                        }
                    },
                    Err(_) => None,
                };
                State::Tail { unit, tail: None }
            }
        };
        let mut control =
            RuntimeKernelControl::new(stages.error.clone()).at_expression_node(stages.node);
        control.bind_mem_tracker(stages.tracker.clone());
        Self {
            name: format!("{} (node={})", phase.name(), stages.node.index()),
            host: Host {
                stages,
                control,
                construction: None,
                prepared: None,
                slot,
                #[cfg(novarocks_dev_controls)]
                drop_insert: None,
            },
            state,
            normal: false,
            exited: false,
            conversions: ValueConversionContext::default(),
        }
    }

    fn control(&self, ctx: &DriverStep<'_>) -> ExecutionResult<ControlStep> {
        ctx.checkpoint()?;
        if self.host.remaining_demand()? == RemainingDemand::Released {
            Ok(ControlStep::Withdrawn)
        } else {
            Ok(ControlStep::Active)
        }
    }

    fn retire(&mut self) -> ExecutionResult<()> {
        if self.exited {
            return Ok(());
        }
        self.exited = true;
        // Record the Task's existing failure before dropping any pending unit
        // or graph cursor that can report an abandonment.
        let outcome = (|| {
            let Some(prepared) = &self.host.prepared else {
                return Ok(());
            };
            let frame = &prepared.frame;
            if let Some(cause) = self.host.stages.error.error() {
                frame.completion.fail(cause.clone());
                if let State::Finalize(Finalize::Build { graph, .. }) = &self.state {
                    graph.fail(cause);
                }
            } else if !self.normal {
                let slot = self.host.slot.clone()?;
                match &self.state {
                    State::Capture { .. } => {
                        frame.inputs.withdraw_input_if_pending(slot)?;
                    }
                    State::Probe(_) => {
                        frame.completion.withdraw_if_pending(slot)?;
                    }
                    _ => {}
                }
            }
            Ok(())
        })();
        let abandoned_demand = match &mut self.state {
            State::Finalize(Finalize::Demand { producer, .. }) => {
                producer.fail(crate::runtime_filter::RuntimeFilterProducerFailure::Cancelled)
            }
            _ => Ok(()),
        };
        match &mut self.state {
            State::Capture { writer, weights } => {
                *writer = None;
                *weights = None;
            }
            State::Finalize(state) => *state = Finalize::Done,
            State::Probe(cursor) => *cursor = None,
            State::Tail { unit, tail } => {
                *tail = None;
                *unit = None;
            }
        }
        self.host.prepared = None;
        outcome.and(abandoned_demand)
    }
}

impl OperatorLifecycle for ContentOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }

    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.host.control.bind_construction_factory(factory.clone());
        self.host.construction = Some(factory);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.host.control.bind_runtime_memory(state);
        #[cfg(novarocks_dev_controls)]
        if matches!(self.state, State::Tail { .. }) {
            self.host.drop_insert = state.content_apply_insert_control();
        }
        Ok(())
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        self.host.slot.clone()?;
        let factory = self
            .host
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let layout =
            self.host.stages.program.graph().nodes()[self.host.stages.node.index()].output_layout();
        let schema = factory.create_compiled_chunk_schema(layout)?;
        let owner = self.host.control.construction_owner(&schema)?;
        let frame = self.host.stages.prepare(&self.host.work(owner.as_ref())?)?;
        self.host.prepared = Some(Prepared { schema, frame });
        Ok(())
    }

    fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
        let owner = self.host.owner()?;
        let frame = &self.host.prepared()?.frame;
        frame.completion.activate(owner.as_ref())?;
        if let State::Capture { writer, weights } = &mut self.state {
            let work = self.host.work(owner.as_ref())?;
            *writer = Some(frame.domain.begin_capture(&capture_work(&work))?);
            *weights = Some(CapturedWeights::new(work.allocator));
        }
        Ok(())
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.retire()
    }

    fn close(&mut self) -> ExecutionResult<()> {
        self.retire()
    }
}

fn columns(chunk: &Chunk, slots: &[SlotId], work: &BulkWork<'_>) -> ExecutionResult<Vec<ArrayRef>> {
    let mut arrays = funded_vec(work.owner, slots.len())?;
    for &slot in slots {
        work.control.checkpoint(0)?;
        arrays.push(
            chunk
                .column_by_slot_id(slot)
                .map_err(ExecutionFailure::from_domain)?,
        );
    }
    Ok(arrays)
}

fn readable(rows: ReachableRows<'_>, work: &BulkWork<'_>) -> ExecutionResult<SelectionSet> {
    rows.materialize_observed(
        &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
        &mut |_| work.control.checkpoint(0).map_err(ExecutionFailure::from),
    )
    .map_err(|cause| match cause {
        ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
        ValueFailure::Caller(cause) => cause,
    })
}

impl TransformKernel for ContentOperator {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        self.host
            .prepared
            .as_ref()
            .map(|p| p.frame.completion.observable())
    }

    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        self.control(ctx)
    }

    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let result = (|| {
            let input = input.into_uncovered_chunk()?;
            let rows = input.len() as u64;
            let next = match &mut self.state {
                State::Capture { writer, weights } => {
                    capture_input(
                        &self.host,
                        writer.as_mut().ok_or(ConstructionFailure::MissingOwner)?,
                        weights.as_mut().ok_or(ConstructionFailure::MissingOwner)?,
                        &input,
                        &mut self.conversions,
                    )?;
                    NextStep::NeedInput
                }
                State::Probe(cursor) if cursor.is_none() => {
                    *cursor = Some(ProbeCursor { input, next: 0 });
                    NextStep::Runnable
                }
                _ => return Err(contract("content stage accepted an unexpected input")),
            };
            Ok(StepResult::new(None, next, rows))
        })();
        result.map_err(|cause| self.host.fail(ctx, cause))
    }

    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let host = &self.host;
        let result = match &mut self.state {
            State::Probe(cursor) => drive_probe(host, cursor, &mut self.conversions, ctx),
            _ => Err(contract("content stage has no input continuation")),
        };
        result.map_err(|cause| host.fail(ctx, cause))
    }

    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let result = (|| {
            ctx.checkpoint()?;
            let slot = self.host.slot.clone()?;
            let frame = &self.host.prepared()?.frame;
            match &mut self.state {
                State::Capture { writer, weights } => {
                    let owner = self.host.owner()?;
                    let work = self.host.work(owner.as_ref())?;
                    let run = writer
                        .take()
                        .ok_or(ConstructionFailure::MissingOwner)?
                        .seal(&capture_work(&work))?;
                    let weights = weights.take().ok_or(ConstructionFailure::MissingOwner)?;
                    frame.inputs.normal_input(slot, (run, weights))?;
                }
                State::Probe(None) => frame.completion.normal_after_output(slot)?,
                _ => return Err(contract("content input EOS left pending work")),
            }
            self.normal = true;
            Ok(StepResult::new(None, NextStep::Finished, 0))
        })();
        result.map_err(|cause| self.host.fail(ctx, cause))
    }
}

fn wait(observable: Arc<Observable>, generation: u64) -> StepResult {
    let next = if observable.generation() == generation {
        NextStep::Wait(WaitFact {
            reason: WaitReason::Dependency,
            observable,
            observed_generation: generation,
            deadline: None,
        })
    } else {
        NextStep::Runnable
    };
    StepResult::new(None, next, 0)
}

impl SourceKernel for ContentOperator {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        TransformKernel::control_observable(self)
    }

    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        self.control(ctx)
    }

    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let host = &self.host;
        let result = match &mut self.state {
            State::Finalize(state) => drive_finalize(host, state, ctx),
            State::Tail { unit, tail } => drive_tail(host, unit, tail, &mut self.conversions, ctx),
            _ => Err(contract("content stage is not an independent source")),
        };
        result.map_err(|cause| host.fail(ctx, cause))
    }
}
