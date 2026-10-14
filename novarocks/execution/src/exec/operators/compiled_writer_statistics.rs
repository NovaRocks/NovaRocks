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

//! Compiled writer statistics (collect-on-write aggregates).
//!
//! * A writer's partial processor owns one state per prepared partial call
//!   for its one driver: it updates each state from the call's projected
//!   column of every page and emits exactly one intermediate row at finish,
//!   even on empty input. The writer packs that row into sparse
//!   `AGGREGATE_PARTIAL` rows with its own packer, guard and limits.
//! * A finish's final aggregate merges every non-null auxiliary channel of
//!   each `AGGREGATE_PARTIAL` row into one state per call of that row's
//!   target. It groups strictly by target, never by channel, and emits every
//!   target group once at finish in ascending target order.
//! * A finish's grouped Unpivot expands each target's final values through
//!   that target's own mappings with the shared compiled Unpivot core.
//!   Scalar constants are the program's `FinishUnpivotConstant` roots,
//!   evaluated once per final batch over one row of the Root port.
//!
//! Every call runs through the exact prepared aggregate handle its compiler
//! attached at its writer site: no implementation is selected by name, no
//! expression arena is read, and the process function set is never
//! consulted. Coverage, completeness and channel-mapping checks stay with the
//! finish operator, identical for every statistics owner.

use std::alloc::Layout;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, Int32Array, RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use novarocks_functions::{
    AggregateCallContract, AggregateKernelPhase, AggregateStateAllocator, AggregateStateColumn,
    EvaluatedArgument, KernelDiagnostic, KernelFailure, PreparedAggregateHandle,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput, Selection,
};
use novarocks_local_program::{
    LocalProgram, ProgramCallSite, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, ProgramStateTemplate, StaticLayout, UnpivotConstant,
    WriterGroupedUnpivotPlan,
};
use novarocks_spi::connector::write_stack::{
    RootRowKind, target_ordinal_from_wire, target_ordinal_to_wire,
};
use novarocks_types::SlotId;

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use super::compiled_unpivot::{
    LiteralSource, Producer, UnpivotExpansion, list_item_field, map_entries_field,
};
use super::table_finish::{
    FinishStatisticsCoverage, FinishStatisticsFactory, GroupedUnpivotSource, root_artifact_chunk,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Target bytes of one state backing block of the finish's target groups.
const STATE_BLOCK_BYTES: usize = 64 * 1024;
const MAX_STATES_PER_BLOCK: usize = 4096;

/// State backing charged to the operator's memory tracker before it is
/// allocated and released after it is returned.
struct TrackedStateAllocator {
    tracker: Arc<MemTracker>,
}

impl novarocks_functions::opaque_memory::OpaqueAllocationHost for TrackedStateAllocator {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        if self.tracker.consume_and_check_limit(bytes).is_err() {
            self.tracker.release(bytes);
            return Err(KernelFailure::ResourceExhausted);
        }
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        if bytes != 0 {
            // Successful reservation was represented exactly as i64.
            self.tracker
                .release(i64::try_from(bytes).expect("successful opaque reservation size"));
        }
    }
}
impl AggregateStateAllocator for TrackedStateAllocator {
    fn opaque_allocation_host(
        &self,
    ) -> Option<&dyn novarocks_functions::opaque_memory::OpaqueAllocationHost> {
        Some(self)
    }

    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if layout.size() == 0 {
            return Err(KernelFailure::Internal(KernelDiagnostic::new(
                "writer aggregate state block has no size",
            )));
        }
        let bytes = i64::try_from(layout.size()).map_err(|_| KernelFailure::ResourceExhausted)?;
        if self.tracker.consume_and_check_limit(bytes).is_err() {
            self.tracker.release(bytes);
            return Err(KernelFailure::ResourceExhausted);
        }
        // SAFETY: the layout has a nonzero size.
        match NonNull::new(unsafe { std::alloc::alloc(layout) }) {
            Some(block) => Ok(block),
            None => {
                self.tracker.release(bytes);
                Err(KernelFailure::ResourceExhausted)
            }
        }
    }

    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the column returns a block this allocator produced with
        // exactly this layout, after destroying every state inside it.
        unsafe { std::alloc::dealloc(block.as_ptr(), layout) };
        self.tracker
            .release(i64::try_from(layout.size()).unwrap_or(i64::MAX));
    }
}

/// The operator's bound memory tracker. A unit test may drive an operator
/// without a pipeline; it charges a child of the process tracker.
fn bound_tracker(
    tracker: Option<&Arc<MemTracker>>,
    label: &str,
) -> ExecutionResult<Arc<MemTracker>> {
    if let Some(tracker) = tracker {
        return Ok(Arc::clone(tracker));
    }
    #[cfg(test)]
    {
        Ok(MemTracker::new_child(
            label,
            &crate::runtime::mem_tracker::process_mem_tracker(),
        ))
    }
    #[cfg(not(test))]
    {
        Err(ExecutionFailure::from(format!(
            "{label} memory tracker must be bound before its first state"
        )))
    }
}

/// One prepared writer call: its exact handle and the slot it reads.
#[derive(Clone)]
struct WriterCall {
    handle: PreparedAggregateHandle,
    contract: Arc<AggregateCallContract>,
    input: SlotId,
}

/// The prepared handle of one writer call site, checked against its phase.
fn writer_call(
    program: &LocalProgram,
    site: ProgramCallSite,
    phase: AggregateKernelPhase,
    input: SlotId,
) -> Result<WriterCall, String> {
    let Some(ProgramStateTemplate::Aggregate { kernel, .. }) = program.state_template(site) else {
        return Err(format!(
            "compiled writer call {site:?} has no prepared aggregate"
        ));
    };
    let handle = kernel.clone();
    let contract = Arc::clone(handle.contract());
    if contract.phase() != phase
        || contract.logical_argument_types().len() != 1
        || contract.distinct()
        || !contract.order_keys().is_empty()
    {
        return Err(format!(
            "compiled writer call {site:?} is not a single-channel {phase:?} aggregate"
        ));
    }
    Ok(WriterCall {
        handle,
        contract,
        input,
    })
}

/// The ordinal of `slot` in `layout`.
fn ordinal(layout: &StaticLayout, slot: SlotId) -> Option<usize> {
    layout
        .slots()
        .iter()
        .position(|candidate| *candidate == slot)
}

/// Each driver's partial statistics of one compiled writer.
pub(crate) struct CompiledWriterPartialFactory {
    name: String,
    calls: Arc<[WriterCall]>,
    /// One field per call: its auxiliary multiplex channel.
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}

impl CompiledWriterPartialFactory {
    /// The partial statistics of local writer `node`, or `None` when it
    /// collects none.
    pub(crate) fn try_new(
        program: &LocalProgram,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Option<Self>, String> {
        let at = node.index();
        let graph_node = program
            .graph()
            .nodes()
            .get(at)
            .ok_or_else(|| format!("missing compiled table writer node {at}"))?;
        let ProgramNodeKind::TableWriter {
            projection,
            writer_multiplex_layout,
            partial_aggregates,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled node {at} is not a TableWriter"));
        };
        if partial_aggregates.is_empty() {
            return Ok(None);
        }
        let mut calls = Vec::with_capacity(partial_aggregates.len());
        let mut fields = Vec::with_capacity(partial_aggregates.len());
        let mut slots = Vec::with_capacity(partial_aggregates.len());
        for (ordinal_in_node, call) in partial_aggregates.iter().enumerate() {
            let site = ProgramCallSite::WriterPartial {
                node,
                call: u32::try_from(ordinal_in_node)
                    .map_err(|_| format!("compiled writer {at} has too many partial calls"))?,
            };
            if ordinal(&projection.layout, call.input_slot_id).is_none() {
                return Err(format!(
                    "compiled writer partial call {site:?} reads slot {} outside its projection",
                    call.input_slot_id
                ));
            }
            let output = ordinal(writer_multiplex_layout, call.intermediate_slot_id)
                .filter(|ordinal| *ordinal > 3)
                .ok_or_else(|| {
                    format!(
                        "compiled writer partial call {site:?} writes slot {} outside its auxiliary channels",
                        call.intermediate_slot_id
                    )
                })?;
            let prepared = writer_call(
                program,
                site,
                AggregateKernelPhase::Partial,
                call.input_slot_id,
            )?;
            let field = writer_multiplex_layout.schema().field(output).clone();
            if field.data_type() != &prepared.contract.intermediate_type().data_type {
                return Err(format!(
                    "compiled writer partial call {site:?} state differs from its channel"
                ));
            }
            fields.push(field);
            slots.push(call.intermediate_slot_id);
            calls.push(prepared);
        }
        let output = ChunkSchema::try_ref_from_schema_and_slot_ids(&Schema::new(fields), &slots)?;
        Ok(Some(Self {
            name: format!("COMPILED_WRITER_PARTIAL_AGGREGATE (node={at})"),
            calls: Arc::from(calls),
            output,
            error,
        }))
    }
}

impl OperatorFactory for CompiledWriterPartialFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledWriterPartialProcessor {
            name: self.name.clone(),
            calls: Arc::clone(&self.calls),
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            states: None,
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        })
    }
}

/// One driver's partial statistics: one state per call, all in one group.
struct CompiledWriterPartialProcessor {
    name: String,
    calls: Arc<[WriterCall]>,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    /// One column per call, each holding exactly its driver's one state.
    states: Option<Vec<AggregateStateColumn>>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl CompiledWriterPartialProcessor {
    fn ensure_states(&mut self) -> ExecutionResult<()> {
        if self.states.is_some() {
            return Ok(());
        }
        let tracker = bound_tracker(self.tracker.as_ref(), "CompiledWriterPartialAggregate")?;
        let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(TrackedStateAllocator {
            tracker: MemTracker::new_child("CompiledWriterPartialStates", &tracker),
        });
        let mut states = Vec::with_capacity(self.calls.len());
        for call in self.calls.iter() {
            let mut column = AggregateStateColumn::try_new(
                call.handle.clone(),
                Arc::clone(&allocator),
                NonZeroUsize::MIN,
            )?;
            column.push(&self.control)?;
            states.push(column);
        }
        self.states = Some(states);
        Ok(())
    }

    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        self.ensure_states()?;
        let rows = chunk.len();
        let mapping = vec![0usize; rows];
        let states = self.states.as_mut().expect("states were created");
        for (call, column) in self.calls.iter().zip(states.iter_mut()) {
            let values = chunk.column_by_slot_id(call.input)?;
            let arguments = [EvaluatedArgument::Column(&values)];
            let input = SelectedAggregateUpdateInput::try_new(
                call.contract.as_ref(),
                Selection::all(rows),
                &arguments,
                &[],
                &self.control,
            )?;
            let mut frame =
                column.prepare_update_batch_evaluation(&mapping, input, &self.control)?;
            frame.run(&self.control)?;
        }
        Ok(())
    }

    /// The driver's one intermediate row; the empty input's is each call's
    /// initial state.
    fn emit(&mut self) -> ExecutionResult<()> {
        self.ensure_states()?;
        let states = self.states.as_ref().expect("states were created");
        let mut columns = Vec::with_capacity(states.len());
        for column in states {
            columns.push(column.emit_evaluation(&[0], 1, &self.control)?);
        }
        let batch =
            RecordBatch::try_new(self.output.arrow_schema_ref(), columns).map_err(|error| {
                ExecutionFailure::from(format!("compiled writer partial output: {error}"))
            })?;
        self.pending.push_back(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?);
        Ok(())
    }
}

impl Operator for CompiledWriterPartialProcessor {
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.tracker = Some(tracker);
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for CompiledWriterPartialProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled writer partial aggregate received input after finishing".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        self.consume(&chunk)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.pop_front();
        if self.finishing && self.pending.is_empty() {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        self.emit()?;
        self.states = None;
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
}

/// One grouped Unpivot mapping: the final channel it reads and, per literal,
/// its frozen constant (a scalar's root index into the evaluated constants).
#[derive(Clone)]
struct GroupedMapping {
    target: u32,
    value: SlotId,
    literals: Vec<LiteralSource>,
}

/// The compiled statistics of one finish: its prepared final calls, its
/// grouped Unpivot plan and the roots of the plan's scalar constants.
pub(crate) struct CompiledFinishStatistics {
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    calls: Arc<[WriterCall]>,
    /// The finish's final aggregate output: the grouping channel followed
    /// by one final channel per call.
    final_output: ChunkSchemaRef,
    plan: WriterGroupedUnpivotPlan,
    mappings: Vec<GroupedMapping>,
    /// Every scalar constant root, in mapping-then-literal order.
    constant_sites: Vec<ProgramExpressionRootSite>,
    /// The Root port each scalar constant root reads.
    root_port: SchemaRef,
    error: Arc<RuntimeErrorState>,
}

impl CompiledFinishStatistics {
    /// The statistics of local finish `node` with their coverage facts, or
    /// `None` when it carries none.
    pub(crate) fn try_new(
        program: &Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Option<(FinishStatisticsCoverage, Arc<Self>)>, String> {
        let at = node.index();
        let graph_node = program
            .graph()
            .nodes()
            .get(at)
            .ok_or_else(|| format!("missing compiled table finish node {at}"))?;
        let ProgramNodeKind::TableFinish {
            writer_multiplex_layout,
            root_result_layout,
            final_aggregates,
            ..
        } = graph_node.kind()
        else {
            return Err(format!("compiled node {at} is not a TableFinish"));
        };
        let invalid = |what: String| format!("compiled table finish at local node {at} {what}");
        let unpivot = match (&final_aggregates.unpivot, final_aggregates.calls.is_empty()) {
            (None, true) => return Ok(None),
            (Some(unpivot), false) => unpivot,
            _ => {
                return Err(invalid(
                    "has final aggregates and a grouped Unpivot that are not both present"
                        .to_string(),
                ));
            }
        };
        let grouping = ordinal(writer_multiplex_layout, unpivot.grouping_input_slot_id)
            .ok_or_else(|| invalid("groups by a slot outside its writer relation".to_string()))?;
        if writer_multiplex_layout.schema().field(grouping).data_type() != &DataType::Int32 {
            return Err(invalid("groups by a non-Int32 channel".to_string()));
        }
        let mut calls = Vec::with_capacity(final_aggregates.calls.len());
        let mut fields = vec![Field::new("write_target_ordinal", DataType::Int32, false)];
        let mut slots = vec![unpivot.grouping_output_slot_id];
        for (call_ordinal, call) in final_aggregates.calls.iter().enumerate() {
            let site = ProgramCallSite::WriterFinal {
                node,
                call: u32::try_from(call_ordinal)
                    .map_err(|_| invalid("has too many final calls".to_string()))?,
            };
            let input = ordinal(writer_multiplex_layout, call.intermediate_input_slot_id)
                .filter(|ordinal| *ordinal > 3)
                .ok_or_else(|| {
                    invalid(format!(
                        "final call {call_ordinal} reads slot {} outside its auxiliary channels",
                        call.intermediate_input_slot_id
                    ))
                })?;
            let prepared = writer_call(
                program,
                site,
                AggregateKernelPhase::Final,
                call.intermediate_input_slot_id,
            )?;
            if writer_multiplex_layout.schema().field(input).data_type()
                != &prepared.contract.intermediate_type().data_type
            {
                return Err(invalid(format!(
                    "final call {call_ordinal} reads a channel of another state type"
                )));
            }
            let final_type = prepared.contract.final_type();
            fields.push(
                final_type
                    .try_to_field(format!("final_aggregate_{call_ordinal}"))
                    .map_err(|error| error.to_string())?,
            );
            slots.push(call.final_output_slot_id);
            calls.push(prepared);
        }
        let final_output =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&Schema::new(fields), &slots)?;
        let root_field = |slot: SlotId| {
            ordinal(root_result_layout, slot)
                .map(|ordinal| {
                    root_result_layout
                        .schema()
                        .field(ordinal)
                        .data_type()
                        .clone()
                })
                .ok_or_else(|| invalid(format!("expands into slot {slot} outside its Root")))
        };
        if root_field(unpivot.passthrough_output_slot_id)? != DataType::Int32 {
            return Err(invalid(
                "passes its target through a non-Int32 channel".to_string(),
            ));
        }
        let value_type = root_field(unpivot.value_output_slot_id)?;
        let literal_types = unpivot
            .literal_output_slot_ids
            .iter()
            .map(|slot| root_field(*slot))
            .collect::<Result<Vec<_>, _>>()?;
        let mut mappings = Vec::with_capacity(unpivot.mappings.len());
        let mut constant_sites = Vec::new();
        for (mapping_ordinal, mapping) in unpivot.mappings.iter().enumerate() {
            let value = final_aggregates
                .calls
                .iter()
                .position(|call| call.final_output_slot_id == mapping.input_value_slot_id)
                .ok_or_else(|| {
                    invalid(format!(
                        "mapping {mapping_ordinal} reads a slot outside its final aggregates"
                    ))
                })?;
            if calls[value].contract.final_type().data_type != value_type {
                return Err(invalid(format!(
                    "mapping {mapping_ordinal} final value differs from its Root value channel"
                )));
            }
            if mapping.constants.len() != literal_types.len() {
                return Err(invalid(format!(
                    "mapping {mapping_ordinal} has another literal width"
                )));
            }
            let mut literals = Vec::with_capacity(mapping.constants.len());
            for (constant_ordinal, (constant, literal_type)) in
                mapping.constants.iter().zip(&literal_types).enumerate()
            {
                literals.push(match constant {
                    UnpivotConstant::Scalar { .. } => {
                        constant_sites.push(ProgramExpressionRootSite::Node {
                            node,
                            role: ProgramNodeExpressionRole::FinishUnpivotConstant {
                                mapping: u32::try_from(mapping_ordinal)
                                    .map_err(|_| invalid("has too many mappings".to_string()))?,
                                constant: u32::try_from(constant_ordinal)
                                    .map_err(|_| invalid("has too many literals".to_string()))?,
                            },
                        });
                        LiteralSource::Root(constant_sites.len() - 1)
                    }
                    UnpivotConstant::Int32List(values) => {
                        if list_item_field(literal_type).is_none() {
                            return Err(invalid(format!(
                                "expands an Int32List constant into {literal_type:?}"
                            )));
                        }
                        LiteralSource::Int32List(Arc::from(values.as_slice()))
                    }
                    UnpivotConstant::Utf8Map(entries) => {
                        if map_entries_field(literal_type).is_none() {
                            return Err(invalid(format!(
                                "expands a Utf8Map constant into {literal_type:?}"
                            )));
                        }
                        LiteralSource::Utf8Map(Arc::from(entries.as_slice()))
                    }
                });
            }
            mappings.push(GroupedMapping {
                target: mapping.grouping_key,
                value: mapping.input_value_slot_id,
                literals,
            });
        }
        let coverage = FinishStatisticsCoverage::new(
            final_aggregates
                .calls
                .iter()
                .map(|call| (call.intermediate_input_slot_id, call.final_output_slot_id))
                .collect(),
            Some(
                unpivot
                    .mappings
                    .iter()
                    .map(|mapping| (mapping.grouping_key, mapping.input_value_slot_id))
                    .collect(),
            ),
        );
        Ok(Some((
            coverage,
            Arc::new(Self {
                program: Arc::clone(program),
                node,
                calls: Arc::from(calls),
                final_output,
                plan: unpivot.clone(),
                mappings,
                constant_sites,
                root_port: Arc::clone(root_result_layout.schema()),
                error,
            }),
        )))
    }

    /// Evaluate every scalar constant root once over one row of the Root
    /// port: an `ARTIFACT_DRAFT` kind and NULL in every other field. A
    /// constant reads no channel, so the row only gives it its one row.
    fn evaluate_constants(
        &self,
        state: &RuntimeState,
        tracker: Option<&Arc<MemTracker>>,
    ) -> ExecutionResult<Vec<ArrayRef>> {
        if self.constant_sites.is_empty() {
            return Ok(Vec::new());
        }
        let mut columns = Vec::with_capacity(self.root_port.fields().len());
        for (ordinal, field) in self.root_port.fields().iter().enumerate() {
            if ordinal == 0 {
                columns.push(
                    Arc::new(Int8Array::from(vec![RootRowKind::ArtifactDraft.to_wire()]))
                        as ArrayRef,
                );
            } else if field.is_nullable() {
                columns.push(new_null_array(field.data_type(), 1));
            } else {
                return Err(ExecutionFailure::from(format!(
                    "compiled grouped Unpivot at local node {} has a NOT NULL Root field {}",
                    self.node.index(),
                    field.name()
                )));
            }
        }
        let row = RecordBatch::try_new(Arc::clone(&self.root_port), columns).map_err(|error| {
            ExecutionFailure::from(format!("compiled grouped Unpivot constant row: {error}"))
        })?;
        let mut control = RuntimeKernelControl::new(Arc::clone(&self.error));
        control.bind_runtime_memory(state);
        if let Some(tracker) = tracker {
            control.bind_mem_tracker(Arc::clone(tracker));
        }
        let mut created: Option<Vec<CompiledExpressionInstance>> = None;
        instances(&mut created, &self.program, &self.constant_sites, &control)?;
        let created = created.as_mut().expect("instances were created");
        let mut constants = Vec::with_capacity(self.constant_sites.len());
        for (instance, site) in created.iter_mut().zip(&self.constant_sites) {
            constants.push(evaluate_all(instance, *site, &row, &control)?);
        }
        Ok(constants)
    }
}

impl FinishStatisticsFactory for CompiledFinishStatistics {
    fn final_aggregate(&self, _state: &RuntimeState) -> Result<Box<dyn Operator>, String> {
        Ok(Box::new(CompiledWriterFinalAggregate {
            name: format!(
                "COMPILED_WRITER_FINAL_AGGREGATE (node={})",
                self.node.index()
            ),
            calls: Arc::clone(&self.calls),
            grouping: self.plan.grouping_input_slot_id,
            output: Arc::clone(&self.final_output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            states: None,
            groups: BTreeMap::new(),
            pending: VecDeque::new(),
            finishing: false,
            finished: false,
        }))
    }

    fn grouped_unpivot(
        &self,
        state: &RuntimeState,
        mut final_chunk: Chunk,
        root_schema: ChunkSchemaRef,
        tracker: Option<Arc<MemTracker>>,
    ) -> ExecutionResult<Box<dyn GroupedUnpivotSource>> {
        if let Some(tracker) = tracker.as_ref() {
            final_chunk.try_transfer_to(tracker).map_err(|error| {
                format!(
                    "ResourceExhausted: table finish final aggregate output memory admission failed: {error}"
                )
            })?;
        }
        let groups = final_chunk
            .column_by_slot_id(self.plan.grouping_output_slot_id)?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "table finish final aggregate grouping output is not Int32".to_string())?
            .clone();
        let mut rows = Vec::with_capacity(groups.len());
        let mut seen = std::collections::HashSet::with_capacity(groups.len());
        for row in 0..groups.len() {
            if groups.is_null(row) {
                return Err("table finish final aggregate produced a null grouping key"
                    .to_string()
                    .into());
            }
            let target = target_ordinal_from_wire(groups.value(row))
                .map_err(|error| format!("table finish final aggregate grouping key: {error}"))?;
            if !seen.insert(target.get()) {
                return Err(format!(
                    "table finish final aggregate produced duplicate target {}",
                    target.get()
                )
                .into());
            }
            if !self
                .mappings
                .iter()
                .any(|mapping| mapping.target == target.get())
            {
                return Err(format!(
                    "table finish final aggregate produced target {} with no grouped Unpivot mapping",
                    target.get()
                ).into());
            }
            rows.push((target.get(), row));
        }
        let constants = self.evaluate_constants(state, tracker.as_ref())?;
        // Expanded channels: the target ordinal, the value, then each literal.
        let mut output_slots = vec![
            self.plan.passthrough_output_slot_id,
            self.plan.value_output_slot_id,
        ];
        output_slots.extend(self.plan.literal_output_slot_ids.iter().copied());
        let output_fields = output_slots
            .iter()
            .map(|slot| {
                root_schema
                    .slot(*slot)
                    .cloned()
                    .ok_or_else(|| format!("table finish Root schema is missing slot {slot}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = Arc::new(ChunkSchema::try_new(output_fields)?);
        let final_ordinal = |slot: SlotId| {
            final_chunk
                .chunk_schema()
                .index_of(slot)
                .ok_or_else(|| format!("table finish final aggregate is missing value slot {slot}"))
        };
        let grouping_ordinal = final_ordinal(self.plan.grouping_output_slot_id)?;
        let mut targets = Vec::with_capacity(rows.len());
        for (target, row) in rows {
            let mappings = self
                .mappings
                .iter()
                .filter(|mapping| mapping.target == target)
                .collect::<Vec<_>>();
            let mut producers = vec![
                Producer::Passthrough {
                    input: grouping_ordinal,
                },
                Producer::Value {
                    inputs: mappings
                        .iter()
                        .map(|mapping| final_ordinal(mapping.value))
                        .collect::<Result<Vec<_>, _>>()?,
                },
            ];
            for literal in 0..self.plan.literal_output_slot_ids.len() {
                producers.push(Producer::Literal {
                    sources: mappings
                        .iter()
                        .map(|mapping| mapping.literals[literal].clone())
                        .collect(),
                });
            }
            targets.push(GroupedTarget {
                target,
                row,
                expansion: UnpivotExpansion::new(
                    Arc::from(producers),
                    Arc::clone(&output),
                    mappings.len(),
                    self.plan.max_output_rows,
                    self.plan.max_output_bytes,
                ),
            });
        }
        Ok(Box::new(CompiledGroupedUnpivot {
            final_chunk,
            constants,
            root_schema,
            targets,
            next: 0,
            started: false,
            tracker,
            source_observable: Arc::new(Observable::new()),
        }))
    }
}

/// The finish's final aggregate: one state per call of each target group.
struct CompiledWriterFinalAggregate {
    name: String,
    calls: Arc<[WriterCall]>,
    grouping: SlotId,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    /// One column per call; every column holds one state per target group.
    states: Option<Vec<AggregateStateColumn>>,
    /// Each target's group index, in ascending target order.
    groups: BTreeMap<u32, usize>,
    pending: VecDeque<Chunk>,
    finishing: bool,
    finished: bool,
}

impl CompiledWriterFinalAggregate {
    fn ensure_states(&mut self) -> ExecutionResult<()> {
        if self.states.is_some() {
            return Ok(());
        }
        let tracker = bound_tracker(self.tracker.as_ref(), "CompiledWriterFinalAggregate")?;
        let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(TrackedStateAllocator {
            tracker: MemTracker::new_child("CompiledWriterFinalStates", &tracker),
        });
        let mut states = Vec::with_capacity(self.calls.len());
        for call in self.calls.iter() {
            let stride = call.handle.state_layout().pad_to_align().size().max(1);
            let per_block = (STATE_BLOCK_BYTES / stride).clamp(1, MAX_STATES_PER_BLOCK);
            states.push(AggregateStateColumn::try_new(
                call.handle.clone(),
                Arc::clone(&allocator),
                NonZeroUsize::new(per_block).expect("clamped to at least one state"),
            )?);
        }
        self.states = Some(states);
        Ok(())
    }

    /// Each row's target group, creating a target's group with one initial
    /// state per call on its first row.
    fn assign_groups(&mut self, chunk: &Chunk) -> ExecutionResult<Vec<usize>> {
        let targets = chunk.column_by_slot_id(self.grouping)?;
        let targets = targets
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ExecutionFailure::from("table finish target ordinal is not Int32"))?;
        let mut mapping = Vec::with_capacity(targets.len());
        for row in 0..targets.len() {
            if targets.is_null(row) {
                return Err(ExecutionFailure::from(
                    "table finish received an aggregate partial with a null target ordinal",
                ));
            }
            let target = target_ordinal_from_wire(targets.value(row))
                .map_err(|error| format!("table finish aggregate partial target: {error}"))?
                .get();
            let next = self.groups.len();
            let group = *self.groups.entry(target).or_insert(next);
            if group == next {
                let states = self.states.as_mut().expect("states were created");
                for column in states.iter_mut() {
                    column.push(&self.control)?;
                }
            }
            mapping.push(group);
        }
        Ok(mapping)
    }

    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        self.ensure_states()?;
        let mapping = self.assign_groups(chunk)?;
        let rows = chunk.len();
        let states = self.states.as_mut().expect("states were created");
        for (call, column) in self.calls.iter().zip(states.iter_mut()) {
            let values = chunk.column_by_slot_id(call.input)?;
            // A NULL channel carries no state of this call on that row.
            let selected = (0..rows)
                .filter(|row| !values.is_null(*row))
                .collect::<Vec<_>>();
            if selected.is_empty() {
                continue;
            }
            let groups = selected.iter().map(|row| mapping[*row]).collect::<Vec<_>>();
            let selection = Selection::try_sparse(rows, &selected).map_err(|_| {
                ExecutionFailure::from("table finish aggregate partial selection is not ordered")
            })?;
            let input = SelectedAggregateMergeInput::try_new(
                call.contract.as_ref(),
                selection,
                EvaluatedArgument::Column(&values),
                &self.control,
            )?;
            let mut frame = column.prepare_merge_batch_evaluation(&groups, input, &self.control)?;
            frame.run(&self.control)?;
        }
        Ok(())
    }

    /// Every target group's final values, in ascending target order.
    fn emit(&mut self) -> ExecutionResult<()> {
        let count = self.groups.len();
        if count == 0 {
            return Ok(());
        }
        let states = self.states.as_ref().expect("a group created the states");
        let mut targets = Vec::with_capacity(count);
        let mut indices = Vec::with_capacity(count);
        for (target, group) in &self.groups {
            let target =
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(*target)
                    .map_err(|error| format!("table finish final target: {error}"))?;
            targets.push(
                target_ordinal_to_wire(target)
                    .map_err(|error| format!("table finish final target: {error}"))?,
            );
            indices.push(*group);
        }
        let mut columns = vec![Arc::new(Int32Array::from(targets)) as ArrayRef];
        for column in states {
            columns.push(column.emit_evaluation(&indices, count, &self.control)?);
        }
        let batch =
            RecordBatch::try_new(self.output.arrow_schema_ref(), columns).map_err(|error| {
                ExecutionFailure::from(format!("compiled writer final output: {error}"))
            })?;
        self.pending.push_back(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?);
        Ok(())
    }
}

impl Operator for CompiledWriterFinalAggregate {
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.tracker = Some(tracker);
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for CompiledWriterFinalAggregate {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled writer final aggregate received input after finishing".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        self.consume(&chunk)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.pop_front();
        if self.finishing && self.pending.is_empty() {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        self.emit()?;
        self.states = None;
        self.groups.clear();
        if self.pending.is_empty() {
            self.finished = true;
        }
        Ok(())
    }
}

/// One target's expansion over its one final row.
struct GroupedTarget {
    target: u32,
    row: usize,
    expansion: UnpivotExpansion,
}

/// The Root artifact rows of one final batch, target by target in the
/// batch's row order.
struct CompiledGroupedUnpivot {
    final_chunk: Chunk,
    constants: Vec<ArrayRef>,
    root_schema: ChunkSchemaRef,
    targets: Vec<GroupedTarget>,
    next: usize,
    started: bool,
    tracker: Option<Arc<MemTracker>>,
    source_observable: Arc<Observable>,
}

impl GroupedUnpivotSource for CompiledGroupedUnpivot {
    fn targets(&self) -> Vec<u32> {
        self.targets.iter().map(|target| target.target).collect()
    }

    fn pull(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        loop {
            let Some(target) = self.targets.get_mut(self.next) else {
                return Ok(None);
            };
            if !self.started {
                let row = Chunk::try_new_like(
                    self.final_chunk.batch.slice(target.row, 1),
                    &self.final_chunk,
                )?;
                let constants = self
                    .constants
                    .iter()
                    .map(|constant| constant.slice(0, 1))
                    .collect();
                target.expansion.start(row, constants)?;
                self.started = true;
            }
            if let Some(chunk) = target.expansion.next(self.tracker.as_ref())? {
                if let Some(output) =
                    root_artifact_chunk(chunk, &self.root_schema, self.tracker.as_ref())?
                {
                    return Ok(Some(output));
                }
                continue;
            }
            // The target's expansion is exhausted.
            self.next += 1;
            self.started = false;
        }
    }

    fn is_finished(&self) -> bool {
        self.next == self.targets.len()
    }

    fn source_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.source_observable)
    }
}

#[cfg(test)]
#[path = "opaque_memory_host_tests.rs"]
mod opaque_memory_host_tests;
