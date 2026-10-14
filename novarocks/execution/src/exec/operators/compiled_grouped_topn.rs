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

use std::{cmp::Ordering, sync::Arc};

use arrow::array::{Array, ArrayRef, Decimal128Array};
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
use super::sort::normalize_sort_key_array;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, OutputColumnMetadata};
use crate::exec::expr::agg::{AggregateAllocator, AggregateVec};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

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
    rank_types: Vec<DataType>,
    fields: Vec<SortField>,
    limit: usize,
    calls: Vec<CallPlan>,
    output: ChunkSchemaRef,
}

pub(crate) struct CompiledGroupedTopNProcessorFactory {
    name: String,
    plan: Plan,
    error: Arc<RuntimeErrorState>,
}
impl CompiledGroupedTopNProcessorFactory {
    fn processor(&self) -> CompiledGroupedTopNProcessor {
        CompiledGroupedTopNProcessor {
            name: self.name.clone(),
            plan: self.plan.clone(),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            instances: None,
            candidates: None,
            pending: None,
            finishing: false,
            finished: false,
            failed: None,
        }
    }
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or_else(|| "compiled GroupedTopN node is absent".to_owned())?;
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
            return Err("compiled node is not a GroupedTopN".into());
        };
        let output = ChunkSchema::from_compiled_layout(graph_node.output_layout())?;
        let mut sites = Vec::new();
        for group in 0..group_by.len() {
            sites.push(ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::TopNGroup {
                    group: u32::try_from(group)
                        .map_err(|_| "GroupedTopN group ordinal exhausted")?,
                },
            });
        }
        let mut rank_types = Vec::new();
        let mut fields = Vec::new();
        for (ordinal, key) in order_by.iter().enumerate() {
            let site = ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::TopNOrder {
                    key: u32::try_from(ordinal)
                        .map_err(|_| "GroupedTopN rank ordinal exhausted")?,
                },
            };
            let ty = root_value_type(&program, site)?;
            if !comparator.supports_value_type(&ty) {
                return Err("GroupedTopN key is outside its exact comparison domain".into());
            }
            let normalized = if novarocks_types::largeint::is_largeint_data_type(&ty.data_type) {
                DataType::Decimal128(38, 0)
            } else {
                ty.data_type.clone()
            };
            fields.push(SortField::new_with_options(
                normalized,
                SortOptions {
                    descending: !key.asc,
                    nulls_first: key.nulls_first,
                },
            ));
            rank_types.push(ty.data_type);
            sites.push(site);
        }
        let mut calls = Vec::new();
        for (ordinal, source) in functions.iter().enumerate() {
            let call = u32::try_from(ordinal).map_err(|_| "GroupedTopN call ordinal exhausted")?;
            let Some(ProgramStateTemplate::Aggregate { scope, kernel }) =
                program.state_template(ProgramCallSite::TopNState { node, call })
            else {
                return Err("GroupedTopN call has no exact prepared aggregate".into());
            };
            if scope != (ProgramAggregateCallScope::TopNState { node, call })
                || kernel.contract().phase() != AggregateKernelPhase::Intermediate
                || source.inputs.len() != 1
            {
                return Err("GroupedTopN call differs from its Intermediate state scope".into());
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
            output,
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
#[path = "../pipeline/builder/compiled_aggregate_fixture.rs"]
mod aggregate_fixture;
#[cfg(test)]
#[path = "../pipeline/builder/compiled_family_fixture.rs"]
mod family_fixture;
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

struct CompiledGroupedTopNProcessor {
    name: String,
    plan: Plan,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    instances: Option<Vec<CompiledExpressionInstance>>,
    candidates: Option<Candidates>,
    pending: Option<AggregateVec<Chunk>>,
    finishing: bool,
    finished: bool,
    failed: Option<ExecutionFailure>,
}
impl CompiledGroupedTopNProcessor {
    fn release_owned(&mut self) {
        self.candidates = None;
        self.pending = None;
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
                self.finishing = true;
                self.finished = true;
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
        let mut normalized = AggregateVec::new_in(set.allocator.clone());
        reserve(&mut normalized, self.plan.groups)?;
        for array in &values[self.plan.groups..self.plan.groups * 2] {
            self.control.checkpoint(0)?;
            normalized.push(normalize_sort_key_array(array).map_err(arrow_failure)?);
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
    fn emit(&mut self) -> ExecutionResult<()> {
        self.control.checkpoint(0)?;
        let Some(set) = self.candidates.as_ref() else {
            return Ok(());
        };
        let mut pending = AggregateVec::new_in(set.allocator.clone());
        reserve(&mut pending, set.entries.len())?;
        for candidate in &set.entries {
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
                let array = if novarocks_types::largeint::is_largeint_data_type(
                    &self.plan.rank_types[rank],
                ) {
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
                    .map_err(arrow_failure)?
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
            let batch = RecordBatch::try_new(self.plan.output.arrow_schema_ref(), columns)
                .map_err(arrow_failure)?;
            self.control.checkpoint(0)?;
            pending.push(Chunk::try_new_with_output_column_metadata(
                batch,
                Arc::clone(&self.plan.output),
                column_metadata,
            )?);
            // Output's actual source/call custody survives this temporary loan.
            drop(library);
        }
        // Publish only after ALL emissions succeed. A changed prefix cannot
        // be exposed or resumed after a later call/key error.
        self.pending = Some(pending);
        Ok(())
    }
}
impl Drop for CompiledGroupedTopNProcessor {
    fn drop(&mut self) {
        self.release_owned();
    }
}
impl Operator for CompiledGroupedTopNProcessor {
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
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.finishing = true;
        self.finished = true;
        self.release_owned();
        self.failed.clone().map_or(Ok(()), Err)
    }
    fn cancel(&mut self) {
        if self.failed.is_none() {
            self.failed = Some(KernelFailure::Cancelled.into());
        }
        self.finishing = true;
        self.finished = true;
        self.release_owned();
    }
    fn on_driver_failure(&mut self) {
        self.cancel();
    }
}
impl ProcessorOperator for CompiledGroupedTopNProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.failed.is_none()
    }
    fn has_output(&self) -> bool {
        self.failed.is_none()
            && self
                .pending
                .as_ref()
                .is_some_and(|pending| !pending.is_empty())
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let result = if self.finishing || self.finished {
            Err(internal("GroupedTopN received input after finishing"))
        } else if chunk.is_empty() {
            self.control.checkpoint(0).map_err(Into::into)
        } else {
            self.consume(&chunk)
        };
        self.latch(result)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let result = self.control.checkpoint(0).map_err(Into::into);
        self.latch(result)?;
        let output = self.pending.as_mut().and_then(|pending| {
            if pending.is_empty() {
                None
            } else {
                Some(pending.remove(0))
            }
        });
        if self.finishing && !self.has_output() {
            self.pending = None;
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        let result = self.emit();
        self.latch(result)?;
        self.candidates = None;
        self.instances = None;
        if !self.has_output() {
            self.pending = None;
            self.finished = true;
        }
        Ok(())
    }
}
