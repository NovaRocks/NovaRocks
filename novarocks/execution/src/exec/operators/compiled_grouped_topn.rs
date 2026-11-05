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

//! Complete-key TopN over all intermediate contributions of one task.
//!
//! The builder gathers the original child drivers immediately before this
//! consumer. Sorted candidates own at most N distinct keys; duplicate keys
//! merge through the original exact Intermediate handles. An evicted key
//! cannot reenter because the full-key boundary only improves. Every final
//! retained key therefore keeps all its contributions. No function math,
//! expression arena, arrival tie or all-groups table lives here.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::{cmp::Ordering, sync::Arc};

use arrow::array::{Array, Decimal128Array};
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, RowParser, SortField};
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{
    AggregateCallContract, AggregateKernelPhase, AggregateStateAllocator, EvaluatedArgument,
    EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    OwnedAggregateState, PreparedAggregateHandle, SelectedAggregateMergeInput, Selection,
};
use novarocks_local_program::{
    LocalProgram, ProgramAggregateCallScope, ProgramCallSite, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate,
};

use super::compiled_expression::{
    RuntimeKernelControl, evaluate_chunk_all, instances, root_value_type,
};
use super::compiled_grouped_topn_resources::{
    ArrowWorkBudget, ArrowWorkShape, retain_new_key_array,
};
use super::sort::creation::SortCreation;
use crate::exec::chunk::{Chunk, ChunkSchemaRef, OutputColumnMetadata};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::allocation::{AggregateAllocator, AggregateVec};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{HydrateInput, Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

fn internal(detail: &str) -> ExecutionFailure {
    KernelFailure::Internal(KernelDiagnostic::new(detail)).into()
}
fn arrow_failure(error: impl std::fmt::Display) -> ExecutionFailure {
    internal(&error.to_string())
}
fn reserve<T>(values: &mut AggregateVec<T>, additional: usize) -> ExecutionResult<()> {
    values
        .try_reserve(additional)
        .map_err(|_| KernelFailure::ResourceExhausted.into())
}
fn add(left: usize, right: usize) -> ExecutionResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| KernelFailure::ResourceExhausted.into())
}
fn multiply(left: usize, right: usize) -> ExecutionResult<usize> {
    left.checked_mul(right)
        .ok_or_else(|| KernelFailure::ResourceExhausted.into())
}
fn retain_observation(charge: &mut OpaqueRetainedCharge, bytes: usize) -> ExecutionResult<()> {
    let mut reservation = charge.reserve_operation(bytes)?;
    charge.reconcile_under_reservation(bytes, &mut reservation)?;
    Ok(())
}

#[derive(Clone)]
struct CallPlan {
    handle: PreparedAggregateHandle,
    contract: Arc<AggregateCallContract>,
    root: usize,
}
#[derive(Clone)]
struct Plan {
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    groups: usize,
    permutation: Vec<usize>,
    rank_types: Vec<novarocks_type_contract::FunctionValueType>,
    fields: Vec<SortField>,
    limit: usize,
    calls: Vec<CallPlan>,
    output_layout: novarocks_local_program::StaticLayout,
}

pub(crate) struct CompiledGroupedTopNProcessorFactory {
    name: String,
    plan: Plan,
    error: Arc<RuntimeErrorState>,
}
impl CompiledGroupedTopNProcessorFactory {
    fn processor(&self) -> GroupedTopNStep {
        Transform::hydrating(CompiledGroupedTopNProcessor {
            name: self.name.clone(),
            plan: self.plan.clone(),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            construction: None,
            output: None,
            instances: None,
            candidates: None,
            emission: None,
            failed: None,
        })
    }
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                "compiled GroupedTopN node is absent",
            )
        })?;
        let ProgramNodeKind::GroupedTopN {
            group_by,
            order_by,
            rank_group_ordinals,
            limit,
            comparator,
            functions,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled node is not a GroupedTopN",
            ));
        };
        if !graph_node.output_layout().is_typed() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled GroupedTopN requires its complete output layout",
            ));
        }
        let output_layout = graph_node.output_layout().clone();
        let mut sites = Vec::new();
        for group in 0..group_by.len() {
            sites.push(ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::TopNGroup {
                    group: novarocks_local_program::checked_program_ordinal(group)
                        .map_err(crate::runtime::task::ExecutionFailure::from_domain)?,
                },
            });
        }
        let mut rank_types = Vec::new();
        let mut fields = Vec::new();
        for (ordinal, key) in order_by.iter().enumerate() {
            let site = ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::TopNOrder {
                    key: novarocks_local_program::checked_program_ordinal(ordinal)
                        .map_err(crate::runtime::task::ExecutionFailure::from_domain)?,
                },
            };
            let ty = root_value_type(&program, site)?;
            if !comparator.supports_value_type(&ty) {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "GroupedTopN key is outside its exact comparison domain",
                ));
            }
            let normalized =
                if ty.logical_type() == novarocks_type_contract::ValueLogicalType::LargeInt {
                    DataType::Decimal128(38, 0)
                } else {
                    ty.data_type().clone()
                };
            fields.push(SortField::new_with_options(
                normalized,
                SortOptions {
                    descending: !key.asc,
                    nulls_first: key.nulls_first,
                },
            ));
            rank_types.push(ty);
            sites.push(site);
        }
        let mut calls = Vec::new();
        for (ordinal, source) in functions.iter().enumerate() {
            let call = novarocks_local_program::checked_program_ordinal(ordinal)
                .map_err(crate::runtime::task::ExecutionFailure::from_domain)?;
            let Some(ProgramStateTemplate::Aggregate { scope, kernel }) =
                program.state_template(ProgramCallSite::TopNState { node, call })
            else {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "GroupedTopN call has no exact prepared aggregate",
                ));
            };
            if scope != (ProgramAggregateCallScope::TopNState { node, call })
                || kernel.contract().phase() != AggregateKernelPhase::Intermediate
                || source.inputs.len() != 1
            {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "GroupedTopN call differs from its Intermediate state scope",
                ));
            }
            calls.push(CallPlan {
                handle: kernel.clone(),
                contract: Arc::clone(kernel.contract()),
                root: sites.len(),
            });
            sites.push(ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::TopNStateArgument { call, argument: 0 },
            });
        }
        let plan = Plan {
            groups: group_by.len(),
            permutation: rank_group_ordinals.clone(),
            rank_types,
            fields,
            limit: *limit,
            calls,
            output_layout,
            sites,
            program,
        };
        Ok(Self {
            name: format!("COMPILED_GROUPED_TOPN (node={})", node.index()),
            plan,
            error,
        })
    }
}
impl OperatorFactory for CompiledGroupedTopNProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.processor())
    }
}

#[cfg(test)]
use super::{aggregate_fixture, family_fixture};
#[cfg(test)]
#[path = "compiled_grouped_topn_tests.rs"]
mod tests;

struct Candidate {
    key: AggregateVec<u8>,
    states: AggregateVec<OwnedAggregateState>,
}
struct Encoder {
    converter: RowConverter,
    parser: RowParser,
    // Both codec owners are destroyed before this public-size stock charge.
    _stock: OpaqueRetainedCharge,
}
struct Candidates {
    entries: AggregateVec<Candidate>,
    encoder: Encoder,
    allocator: AggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}

/// Compare full encoded ranks under the original bounded control observer.
/// Equal bytes denote the same complete key, with no arrival tie-breaker.
fn compare_keys(
    left: &[u8],
    right: &[u8],
    control: &dyn KernelEvaluationControl,
) -> Result<Ordering, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    for (a, b) in left.iter().zip(right) {
        work.step()?;
        if a != b {
            work.finish()?;
            return Ok(a.cmp(b));
        }
    }
    work.finish()?;
    Ok(left.len().cmp(&right.len()))
}
fn locate(
    entries: &[Candidate],
    key: &[u8],
    control: &dyn KernelEvaluationControl,
) -> Result<Result<usize, usize>, KernelFailure> {
    let (mut lo, mut hi) = (0, entries.len());
    while lo < hi {
        control.checkpoint(0)?;
        let mid = lo + (hi - lo) / 2;
        match compare_keys(&entries[mid].key, key, control)? {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(lo))
}

type GroupedTopNStep = Transform<CompiledGroupedTopNProcessor, HydrateInput>;

struct CompiledGroupedTopNProcessor {
    name: String,
    plan: Plan,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    output: Option<ChunkSchemaRef>,
    instances: Option<Vec<CompiledExpressionInstance>>,
    candidates: Option<Candidates>,
    emission: Option<usize>,
    failed: Option<ExecutionFailure>,
}
impl CompiledGroupedTopNProcessor {
    fn release_owned(&mut self) {
        self.candidates = None;
        self.emission = None;
        self.instances = None;
    }
    fn latch<T>(&mut self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.failed = Some(error.clone());
                self.release_owned();
                Err(error)
            }
        }
    }
    fn ensure_candidates(&mut self) -> ExecutionResult<()> {
        if self.candidates.is_some() {
            return Ok(());
        }
        let tracker = self
            .tracker
            .as_ref()
            .ok_or_else(|| internal("GroupedTopN tracker must be bound before execution"))?;
        let host = self.control.allocator().ok_or_else(|| {
            internal("GroupedTopN allocation host must be bound before execution")
        })?;
        let library = ArrowWorkBudget::admit(
            Arc::clone(&host),
            ArrowWorkShape {
                rows: 0,
                cells: 0,
                payload_bytes: 0,
                fields: self.plan.groups,
                buffers: 0,
            },
        )?;
        let mut stock = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        self.control.checkpoint(0)?;
        let converter = RowConverter::new(self.plan.fields.clone()).map_err(arrow_failure)?;
        let parser = converter.parser();
        retain_observation(&mut stock, converter.size())?;
        self.control.checkpoint(0)?;
        let allocator = AggregateAllocator::new(Arc::clone(tracker));
        self.candidates = Some(Candidates {
            entries: AggregateVec::new_in(allocator.clone()),
            encoder: Encoder {
                converter,
                parser,
                _stock: stock,
            },
            allocator,
            host,
        });
        drop(library);
        Ok(())
    }
    fn consume(&mut self, input: &Chunk) -> ExecutionResult<()> {
        self.control.checkpoint(0)?;
        instances(
            &mut self.instances,
            &self.plan.program,
            &self.plan.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("compiled roots initialized");
        // Complete keys remain required even at N=0. State payloads need no
        // evaluation/decoding/merge when no result can be retained.
        let needed = if self.plan.limit == 0 {
            self.plan.groups * 2
        } else {
            self.plan.sites.len()
        };
        let tracker = self.tracker.as_ref().ok_or_else(|| {
            internal("GroupedTopN tracker must be bound before evaluating its input")
        })?;
        let mut values = AggregateVec::new_in(AggregateAllocator::new(Arc::clone(tracker)));
        reserve(&mut values, needed)?;
        for (instance, site) in instances.iter_mut().zip(&self.plan.sites).take(needed) {
            values.push(evaluate_chunk_all(instance, *site, input, &self.control)?);
        }
        if self.plan.limit == 0 {
            return Ok(());
        }
        self.ensure_candidates()?;
        let set = self
            .candidates
            .as_mut()
            .expect("candidate storage initialized");
        let mut payload = 0;
        for array in &values[self.plan.groups..self.plan.groups * 2] {
            payload = add(payload, array.get_array_memory_size())?;
        }
        let library = ArrowWorkBudget::admit(
            Arc::clone(&set.host),
            ArrowWorkShape {
                rows: input.len(),
                cells: multiply(input.len(), self.plan.groups)?,
                payload_bytes: payload,
                fields: self.plan.groups,
                buffers: multiply(self.plan.groups, 3)?,
            },
        )?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let input_schema = input.chunk_schema_ref();
        let creation = SortCreation::new(factory.as_ref(), &input_schema);
        let owner = creation.begin(input)?;
        let types = creation.begin(input)?;
        let mut normalized = AggregateVec::new_in(set.allocator.clone());
        reserve(&mut normalized, self.plan.groups)?;
        for (rank, array) in values[self.plan.groups..self.plan.groups * 2]
            .iter()
            .enumerate()
        {
            self.control.checkpoint(0)?;
            normalized.push(creation.normalize_key(
                array,
                self.plan.rank_types[rank].value_type(),
                owner.as_ref(),
                types.as_ref(),
            )?);
            self.control.checkpoint(0)?;
        }
        let mut row_stock = OpaqueRetainedCharge::try_new(Arc::clone(&set.host))?;
        self.control.checkpoint(0)?;
        let rows = set
            .encoder
            .converter
            .convert_columns(&normalized)
            .map_err(arrow_failure)?;
        retain_observation(&mut row_stock, rows.size())?;
        self.control.checkpoint(0)?;
        for row in 0..input.len() {
            self.control.checkpoint(0)?;
            let rank = rows.row(row);
            let found = locate(&set.entries, rank.as_ref(), &self.control)?;
            let index = match found {
                Ok(index) => index,
                Err(index) => {
                    if set.entries.len() == self.plan.limit {
                        if index == set.entries.len() {
                            continue;
                        }
                        // Destroy the worst key AND every typed state before
                        // requesting a replacement state or key allocation.
                        drop(set.entries.pop().expect("full candidate set"));
                    }
                    let mut key = AggregateVec::new_in(set.allocator.clone());
                    reserve(&mut key, rank.as_ref().len())?;
                    key.extend_from_slice(rank.as_ref());
                    let mut states = AggregateVec::new_in(set.allocator.clone());
                    reserve(&mut states, self.plan.calls.len())?;
                    for call in &self.plan.calls {
                        states.push(OwnedAggregateState::try_new(
                            call.handle.clone(),
                            Arc::clone(&set.host),
                            &self.control,
                        )?);
                    }
                    reserve(&mut set.entries, 1)?;
                    set.entries.insert(index, Candidate { key, states });
                    index
                }
            };
            let selected = [row];
            let selection = Selection::try_sparse(input.len(), &selected)
                .map_err(|_| internal("GroupedTopN state row is outside its actual input"))?;
            for (call, state) in self.plan.calls.iter().zip(&mut set.entries[index].states) {
                let merge = SelectedAggregateMergeInput::try_new(
                    call.contract.as_ref(),
                    selection,
                    EvaluatedArgument::Column(&values[call.root]),
                    &self.control,
                )?;
                let mut frame = state.prepare_merge_batch_evaluation(&[0], merge, &self.control)?;
                frame.run(&self.control)?;
            }
        }
        // Rows/normalized aliases are real temporary owners; their stock and
        // policy leases stay live through Drop, rather than just the call.
        drop(rows);
        drop(row_stock);
        drop(normalized);
        drop(library);
        Ok(())
    }
    fn emit_candidate(&self, index: usize) -> ExecutionResult<Chunk> {
        self.control.checkpoint(0)?;
        let set = self
            .candidates
            .as_ref()
            .expect("an emission cursor retains candidates");
        let candidate = &set.entries[index];
        let output = self
            .output
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let creation = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let library = ArrowWorkBudget::admit(
            Arc::clone(&set.host),
            ArrowWorkShape {
                rows: 1,
                cells: add(self.plan.groups, self.plan.calls.len())?,
                payload_bytes: candidate.key.len(),
                fields: add(self.plan.groups, self.plan.calls.len())?,
                buffers: multiply(add(self.plan.groups, self.plan.calls.len())?, 3)?,
            },
        )?;
        self.control.checkpoint(0)?;
        let parsed = set.encoder.parser.parse(&candidate.key);
        let decoded = set
            .encoder
            .converter
            .convert_rows([parsed])
            .map_err(arrow_failure)?;
        self.control.checkpoint(0)?;
        let mut keys = AggregateVec::new_in(set.allocator.clone());
        reserve(&mut keys, self.plan.groups)?;
        keys.resize_with(self.plan.groups, || None);
        for (rank, array) in decoded.into_iter().enumerate() {
            let array = if self.plan.rank_types[rank].logical_type()
                == novarocks_type_contract::ValueLogicalType::LargeInt
            {
                let decimal = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        internal("GroupedTopN LARGEINT rank did not restore signed i128")
                    })?;
                let value = [if decimal.is_null(0) {
                    None
                } else {
                    Some(decimal.value(0))
                }];
                novarocks_types::largeint::array_from_i128_observed(&value, &mut |_| {
                    self.control.checkpoint(0)
                })?
            } else {
                array
            };
            let group = self.plan.permutation[rank];
            keys[group] = Some(retain_new_key_array(array, &library, &self.control)?);
        }
        let width = add(self.plan.groups, self.plan.calls.len())?;
        // Establish the keeper before unpublished columns/batch. On any
        // error, reverse local Drop frees the actual container first.
        let column_metadata =
            OutputColumnMetadata::try_new(width, Arc::clone(&set.host), &self.control)?;
        let mut columns = Vec::new();
        columns
            .try_reserve_exact(width)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for key in keys {
            columns.push(key.ok_or_else(|| internal("GroupedTopN restored key is absent"))?);
        }
        for state in &candidate.states {
            columns.push(state.emit_evaluation(1, &self.control)?);
        }
        let batch =
            RecordBatch::try_new(output.arrow_schema_ref(), columns).map_err(arrow_failure)?;
        self.control.checkpoint(0)?;
        let result = Chunk::try_new_with_output_column_metadata(
            batch,
            Arc::clone(output),
            column_metadata,
            creation.begin_typed_construction(output)?.as_ref(),
        )?;
        // Only this complete candidate is published; later candidates remain
        // in the operator continuation and cannot be replayed after failure.
        drop(library);
        Ok(result)
    }
}
impl Drop for CompiledGroupedTopNProcessor {
    fn drop(&mut self) {
        self.release_owned();
    }
}
impl OperatorLifecycle for CompiledGroupedTopNProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        self.output = Some(factory.create_compiled_chunk_schema(&self.plan.output_layout)?);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.tracker = Some(tracker);
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
impl InputEncodingPolicy for CompiledGroupedTopNProcessor {}
impl TransformKernel for CompiledGroupedTopNProcessor {
    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let result = if chunk.is_empty() {
            self.control.checkpoint(0).map_err(Into::into)
        } else {
            self.consume(&chunk)
        };
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        self.latch(result)?;
        Ok(StepResult::new(None, NextStep::NeedInput, rows))
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let Some(index) = self.emission else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let result = ctx.perform(crate::runtime::task::PipelineOperation::Finishing, |_| {
            self.emit_candidate(index)
        });
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        let output = self.latch(result)?;
        let next = if index + 1
            == self
                .candidates
                .as_ref()
                .expect("candidate cursor is live")
                .entries
                .len()
        {
            self.release_owned();
            NextStep::Finished
        } else {
            self.emission = Some(index + 1);
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let result: ExecutionResult<()> = self.control.checkpoint(0).map_err(Into::into);
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        self.latch(result)?;
        self.instances = None;
        if self
            .candidates
            .as_ref()
            .is_none_or(|set| set.entries.is_empty())
        {
            self.release_owned();
            Ok(StepResult::new(None, NextStep::Finished, 0))
        } else {
            self.emission = Some(0);
            Ok(StepResult::new(None, NextStep::Runnable, 0))
        }
    }
}
