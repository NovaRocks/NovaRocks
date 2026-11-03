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

//! Compiled ChangeEventExpand: one input row becomes one output row per
//! change event whose predicate selects it.
//!
//! For each input batch the events run in frozen order. An event's
//! `ChangePredicate` root (TruthOnly) is evaluated over every input row and
//! selects exactly its TRUE rows; an event without a predicate selects every
//! row. Each `ChangeAssignment` root is then evaluated over that event's
//! selected rows only, so an unselected row can raise no assignment error.
//! The event's output rows are its selected rows in input order: assigned
//! channels carry their root's values, unassigned channels are NULL, and the
//! effect channel carries the event's frozen mutation effect code.
//!
//! An assignment value must already have its output channel's exact Arrow
//! type; there is no cast, unlike the legacy operator. Expressions are
//! evaluated only through compiled roots.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, new_null_array};
use arrow::datatypes::DataType;
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind,
};

use super::compiled_expression::{
    RuntimeKernelControl, evaluate_all, evaluate_selected, instances,
};
use crate::exec::chunk::{Chunk, ChunkBatchProjection, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

/// One event's frozen facts, resolved to root indices and output ordinals.
#[derive(Clone)]
struct EventPlan {
    /// Index into the root list of the TruthOnly predicate, if any.
    predicate: Option<usize>,
    /// The effect code written to the effect channel.
    effect: i8,
    /// Per output ordinal: `Some(root index)` for an assigned channel.
    assigned: Vec<Option<usize>>,
}

pub struct CompiledChangeEventProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    events: Arc<[EventPlan]>,
    effect_ordinal: usize,
    output_layout: novarocks_local_program::StaticLayout,
    error: Arc<RuntimeErrorState>,
}

impl CompiledChangeEventProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let at = node.index();
        let graph_node = program.graph().nodes().get(at).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled change-event node {at} is absent"),
            )
        })?;
        let ProgramNodeKind::ChangeEventExpand {
            events,
            output_slot_ids,
            effect_slot_id,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {at} is not a ChangeEventExpand"),
            ));
        };
        let invalid = |what: String| {
            program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled change-event node {at} {what}"),
            )
        };
        let layout = graph_node.output_layout();
        if layout.slots() != output_slot_ids.as_slice() {
            return Err(invalid("output slots differ from its layout".to_string()));
        }
        let ordinal_of = |slot| {
            layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
        };
        let effect_ordinal = ordinal_of(*effect_slot_id)
            .ok_or_else(|| invalid(format!("has no effect channel {effect_slot_id}")))?;
        let effect_type = layout.schema().field(effect_ordinal).data_type();
        if effect_type != &DataType::Int8 {
            return Err(invalid(format!(
                "effect channel has type {effect_type:?}, not Int8"
            )));
        }
        let mut sites = Vec::new();
        let mut plans = Vec::with_capacity(events.len());
        for (event_ordinal, event) in events.iter().enumerate() {
            let event_index = u32::try_from(event_ordinal)
                .map_err(|_| invalid("has too many events".to_string()))?;
            let predicate = event.predicate.map(|_| {
                sites.push(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::ChangePredicate { event: event_index },
                });
                sites.len() - 1
            });
            let mut assigned = vec![None; layout.slots().len()];
            let mut covered = vec![false; layout.slots().len()];
            for (assignment_ordinal, assignment) in event.assignments.iter().enumerate() {
                let ordinal = ordinal_of(assignment.output_slot_id).ok_or_else(|| {
                    invalid(format!(
                        "event {event_ordinal} assigns slot {} outside its output",
                        assignment.output_slot_id
                    ))
                })?;
                if ordinal == effect_ordinal || std::mem::replace(&mut covered[ordinal], true) {
                    return Err(invalid(format!(
                        "event {event_ordinal} assigns slot {} twice or assigns the effect",
                        assignment.output_slot_id
                    )));
                }
                if let Some(expression) = assignment.expr {
                    if layout.is_typed() {
                        let Some(novarocks_functions::FunctionArgumentType::Value(source)) =
                            program.checked().channels().expressions().definition_type(
                                novarocks_local_program::ProgramExpressionArena::Main,
                                expression,
                            )
                        else {
                            return Err(invalid(
                                "has an assignment without a complete value declaration".into(),
                            ));
                        };
                        let target = layout.value_binding_at(ordinal).ok_or_else(|| {
                            invalid("has an output without a complete value binding".into())
                        })?;
                        if (source.nullable() && !target.nullable())
                            || !source
                                .typed_binding()
                                .borrowed()
                                .same_physical_binding_ignoring_root_nullability_observed(
                                    target.borrowed(),
                                    || Ok::<(), crate::runtime::task::ExecutionFailure>(()),
                                )?
                        {
                            return Err(invalid(format!(
                                "event {event_ordinal} assignment differs from its complete output binding"
                            )));
                        }
                    }
                    let assignment_index = u32::try_from(assignment_ordinal)
                        .map_err(|_| invalid("has too many assignments".to_string()))?;
                    sites.push(ProgramExpressionRootSite::Node {
                        node,
                        role: ProgramNodeExpressionRole::ChangeAssignment {
                            event: event_index,
                            assignment: assignment_index,
                        },
                    });
                    assigned[ordinal] = Some(sites.len() - 1);
                }
            }
            plans.push(EventPlan {
                predicate,
                effect: event.effect as i8,
                assigned,
            });
        }
        let output_layout = layout.clone();
        Ok(Self {
            name: format!("COMPILED_CHANGE_EVENT_EXPAND (node={at})"),
            program,
            sites,
            events: Arc::from(plans),
            effect_ordinal,
            output_layout,
            error,
        })
    }
}

impl OperatorFactory for CompiledChangeEventProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(CompiledChangeEventProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            events: Arc::clone(&self.events),
            effect_ordinal: self.effect_ordinal,
            output_layout: self.output_layout.clone(),
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            input: None,
            next_event: 0,
            failed: false,
        }))
    }
}

struct CompiledChangeEventProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    events: Arc<[EventPlan]>,
    effect_ordinal: usize,
    output_layout: novarocks_local_program::StaticLayout,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    input: Option<ChunkBatchProjection>,
    next_event: usize,
    failed: bool,
}

#[derive(Debug)]
enum ChangeEventFailure {
    PredicateType {
        event: usize,
    },
    AssignmentType {
        event: usize,
        channel: usize,
        detail: novarocks_failure_contract::BoundedDetail,
    },
    AlreadyFailed,
}
impl std::fmt::Display for ChangeEventFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PredicateType { event } => {
                write!(f, "compiled change-event predicate {event} is not Boolean")
            }
            Self::AssignmentType {
                event,
                channel,
                detail,
            } => write!(
                f,
                "compiled change-event {event} assignment channel {channel}: {detail}"
            ),
            Self::AlreadyFailed => f.write_str("compiled change-event instance has already failed"),
        }
    }
}
impl std::error::Error for ChangeEventFailure {}
impl novarocks_failure_contract::FailureProjection for ChangeEventFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        let reason = match self {
            Self::PredicateType { .. } => ExecutionReason::ChangeEventPredicateType,
            Self::AssignmentType { .. } => ExecutionReason::ChangeEventAssignmentType,
            Self::AlreadyFailed => ExecutionReason::ChangeEventAlreadyFailed,
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}
impl From<ChangeEventFailure> for ExecutionFailure {
    fn from(cause: ChangeEventFailure) -> Self {
        Self::from_domain(cause)
    }
}

impl CompiledChangeEventProcessor {
    fn alive(&self) -> ExecutionResult<()> {
        if self.failed {
            Err(ChangeEventFailure::AlreadyFailed.into())
        } else {
            Ok(())
        }
    }
    fn expand_event(
        &mut self,
        source: &Chunk,
        event_ordinal: usize,
    ) -> ExecutionResult<Option<Chunk>> {
        let input = source.record_batch();
        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let schema = output.arrow_schema_ref();
        let event = &self.events[event_ordinal];
        let rows: Vec<usize> = match event.predicate {
            Some(root) => {
                let truth =
                    evaluate_all(&mut instances[root], self.sites[root], input, &self.control)?;
                let truth = truth.as_any().downcast_ref::<BooleanArray>().ok_or(
                    ChangeEventFailure::PredicateType {
                        event: event_ordinal,
                    },
                )?;
                // TruthOnly: FALSE and NULL both leave the row unselected.
                (0..truth.len())
                    .filter(|&row| truth.is_valid(row) && truth.value(row))
                    .collect()
            }
            None => (0..input.num_rows()).collect(),
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output)?;
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns {
                channels: schema.fields().len(),
            },
        )?;
        let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
        for (ordinal, field) in schema.fields().iter().enumerate() {
            let column = if ordinal == self.effect_ordinal {
                Arc::new(Int8Array::from_value(event.effect, rows.len())) as ArrayRef
            } else if let Some(root) = event.assigned[ordinal] {
                let values = evaluate_selected(
                    &mut instances[root],
                    self.sites[root],
                    input,
                    &rows,
                    &self.control,
                )?;
                if values.data_type() != field.data_type() {
                    return Err(ChangeEventFailure::AssignmentType {
                        event: event_ordinal,
                        channel: ordinal,
                        detail: novarocks_failure_contract::BoundedDetail::truncating_display(
                            format_args!(
                                "{} has type {:?}, not {:?}",
                                field.name(),
                                values.data_type(),
                                field.data_type()
                            ),
                        ),
                    }
                    .into());
                }
                values
            } else {
                new_null_array(field.data_type(), rows.len())
            };
            columns.push(column);
        }
        let storage = if event.assigned.iter().any(Option::is_some) {
            source.source_storage_lifetime_observed::<ConstructionFailure>(
                owner.storage_lifetime(),
                |layout| owner.request(layout),
            )?
        } else {
            owner.storage_lifetime()
        };
        let output = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            Arc::clone(output),
            columns,
            rows.len(),
            storage,
            |step| owner.observe(0, &[], step),
        )?;
        Ok(Some(output))
    }
}

impl OperatorLifecycle for CompiledChangeEventProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        self.alive()?;
        if self.output.is_some() {
            return Ok(());
        }
        let result = (|| -> ExecutionResult<ChunkSchemaRef> {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            Ok(factory.create_compiled_chunk_schema(&self.output_layout)?)
        })();
        match result {
            Ok(output) => {
                self.output = Some(output);
                Ok(())
            }
            Err(cause) => {
                self.failed = true;
                Err(cause)
            }
        }
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.input = None;
        self.instances = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.failed = true;
        self.input = None;
        self.instances = None;
    }
}

impl InputEncodingPolicy for CompiledChangeEventProcessor {}
impl TransformKernel for CompiledChangeEventProcessor {
    fn apply(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.alive()?;
        let chunk = input.into_uncovered_chunk()?;
        self.output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let next = if chunk.is_empty() || self.events.is_empty() {
            NextStep::NeedInput
        } else {
            self.input = Some(chunk.into_batch_projection());
            self.next_event = 0;
            NextStep::Runnable
        };
        Ok(StepResult::new(None, next, rows))
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.alive()?;
        let Some(input) = self.input.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let input = input.clone().into_chunk();
        let output = match self.expand_event(&input, self.next_event) {
            Ok(output) => output,
            Err(cause) => {
                self.failed = true;
                self.input = None;
                self.instances = None;
                return Err(cause);
            }
        };
        self.next_event += 1;
        let next = if self.next_event == self.events.len() {
            self.input = None;
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(output, next, 0))
    }
}
