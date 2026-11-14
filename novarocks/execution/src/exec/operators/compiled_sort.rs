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

//! Compiled global and analytic Sort and ordinary row-count TopN.
//!
//! The local compiler emits exactly three shapes of the local Sort
//! vocabulary: a global sort (`use_top_n = false`, no limit, offset 0), an
//! analytic sort (the same with partition keys and no partition limit) and an
//! ordinary row-count TopN (`use_top_n = true`, `limit`, `offset`, RowNumber,
//! no partition keys). None has a buffering cap. An analytic sort orders by
//! its partition keys first, each with its own direction and NULL placement,
//! and then by its order keys; it emits every row, so its partitions arrive
//! contiguously. Every other Sort shape is an explicit refusal here.
//!
//! The TopN shape serves every phase of a `Rows` reduction alike, because
//! each is the window of its own instance input. Single and Final read the
//! whole relation through a Singleton input. A Partial carries offset 0 and
//! the whole final window (`final.limit + final.offset`) as its limit, so it
//! keeps its instance's top rows and the gathered Final selects the window.
//! A grouped-state partial never reaches this operator.
//!
//! Evaluation boundary: each partition key is a compiled `SortPartition`
//! root and each ORDER BY key a compiled `SortOrder` root, evaluated by this
//! driver's own instance exactly once per input row, on
//! arrival, in arrival order. Key columns travel with their payload rows from
//! then on; pruning and the final sort never re-evaluate a key. The reused
//! legacy pieces are pure array code only: `normalize_sort_key_array`, the
//! stable row-index tiebreaker, and Arrow concat/lexsort/take. The legacy
//! ExprArena sorters are not used.
//!
//! Semantics: rows are ordered by the keys with each key's direction and
//! NULL placement, ties keep arrival order (stable), and the output is the
//! sorted rows `[offset, offset + limit)`. TopN bounds its buffer by keeping
//! only the first `offset + limit` sorted rows once enough rows arrive; this
//! is exact because a row outside that prefix can never re-enter it. A full
//! sort buffers its whole input in memory (no spill, ADR-0162).

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{SortColumn, SortOptions};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, SortTopNType,
};

use super::compiled_expression::{
    RuntimeKernelControl, evaluate_chunk_all, instances, root_value_type,
};
use super::sort::creation::{CreatedSortColumns, SortCreation};
use crate::exec::chunk::{Chunk, ChunkBatchProjection, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{ConstructionFailure, InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::ExecutionResult;

/// TopN prunes its buffer to `offset + limit` rows once it holds at least
/// `max(PRUNE_FACTOR * kept, PRUNE_MIN_ROWS)` rows, as the legacy TopN does.
const PRUNE_FACTOR: usize = 4;
const PRUNE_MIN_ROWS: usize = 4096;

pub struct CompiledSortProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    options: Vec<SortOptions>,
    /// `offset + limit` for a TopN; `None` for a full sort.
    keep: Option<usize>,
    limit: Option<usize>,
    offset: usize,
    prune_rows: usize,
    output_node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
}

impl CompiledSortProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled Sort node {} is absent", node.index()),
            )
        })?;
        let ProgramNodeKind::Sort {
            use_top_n,
            order_by,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
            partition_exprs,
            partition_limit,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {} is not a Sort", node.index()),
            ));
        };
        let at = node.index();
        if partition_limit.is_some() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled Sort at local node {at} with a partition limit is not executable"
                ),
            ));
        }
        if *use_top_n && !partition_exprs.is_empty() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled TopN at local node {at} with partition keys is not executable"),
            ));
        }
        if *topn_type != SortTopNType::RowNumber {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled TopN at local node {at} with {topn_type:?} ranking is not executable"
                ),
            ));
        }
        if max_buffered_rows.is_some() || max_buffered_bytes.is_some() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled Sort at local node {at} carries a buffering cap the compiler never authors"
                ),
            ));
        }
        if order_by.is_empty() && partition_exprs.is_empty() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled Sort at local node {at} has no ordering key"),
            ));
        }
        let keep = if *use_top_n {
            let limit = limit.ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    format!("compiled TopN at local node {at} has no row limit"),
                )
            })?;
            Some(offset.checked_add(limit).ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    format!("compiled TopN at local node {at} limit and offset overflow"),
                )
            })?)
        } else {
            None
        };
        let mut sites = Vec::with_capacity(partition_exprs.len() + order_by.len());
        let mut options = Vec::with_capacity(partition_exprs.len() + order_by.len());
        // Partition keys lead, then the order keys within each partition.
        let keys = partition_exprs
            .iter()
            .enumerate()
            .map(|(ordinal, key)| (ordinal, key, true))
            .chain(
                order_by
                    .iter()
                    .enumerate()
                    .map(|(ordinal, key)| (ordinal, key, false)),
            );
        for (ordinal, key, partition) in keys {
            let key_ordinal = novarocks_local_program::checked_program_ordinal(ordinal)
                .map_err(crate::runtime::task::ExecutionFailure::from_domain)?;
            sites.push(ProgramExpressionRootSite::Node {
                node,
                role: if partition {
                    ProgramNodeExpressionRole::SortPartition { key: key_ordinal }
                } else {
                    ProgramNodeExpressionRole::SortOrder { key: key_ordinal }
                },
            });
            // NULL placement is independent of direction, as in Arrow.
            options.push(SortOptions {
                descending: !key.asc,
                nulls_first: key.nulls_first,
            });
        }
        let prune_rows = keep.map_or(usize::MAX, |keep| {
            keep.saturating_mul(PRUNE_FACTOR).max(PRUNE_MIN_ROWS)
        });
        let (limit, offset) = (*limit, *offset);
        let name = if *use_top_n {
            format!("COMPILED_TOP_N (node={at})")
        } else {
            format!("COMPILED_SORT (node={at})")
        };
        Ok(Self {
            name,
            program,
            sites,
            options,
            keep,
            limit,
            offset,
            prune_rows,
            output_node: node,
            error,
        })
    }

    /// Prune at a smaller buffered row count, so a test can exercise TopN
    /// pruning with few rows. The kept prefix is unchanged.
    #[cfg(test)]
    pub(crate) fn with_prune_rows(mut self, rows: usize) -> Self {
        if self.keep.is_some() {
            self.prune_rows = rows;
        }
        self
    }
}

impl OperatorFactory for CompiledSortProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(CompiledSortProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            options: self.options.clone(),
            keep: self.keep,
            limit: self.limit,
            offset: self.offset,
            prune_rows: self.prune_rows,
            output_node: self.output_node,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            runs: Vec::new(),
            buffered_rows: 0,
        }))
    }
}

/// Payload rows with their already evaluated, normalized key columns.
struct SortedRun {
    batch: ChunkBatchProjection,
    keys: CreatedSortColumns,
}

struct CompiledSortProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    options: Vec<SortOptions>,
    keep: Option<usize>,
    limit: Option<usize>,
    offset: usize,
    prune_rows: usize,
    output_node: ProgramNodeId,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    runs: Vec<SortedRun>,
    buffered_rows: usize,
}

impl CompiledSortProcessor {
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output.as_ref().ok_or_else(|| {
            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into()
        })
    }

    fn creation(&self) -> ExecutionResult<SortCreation<'_>> {
        Ok(SortCreation::new(
            self.construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?
                .as_ref(),
            self.prepared_output()?,
        ))
    }

    /// Concatenate the runs and return their first `take_rows` rows in sorted
    /// order (all rows when `None`). The sorted keys are kept only when the
    /// result is buffered again (`keep_keys`).
    fn sort_runs(
        &self,
        take_rows: Option<usize>,
        keep_keys: bool,
    ) -> ExecutionResult<Option<SortedRun>> {
        if self.runs.is_empty() {
            return Ok(None);
        }
        let creation = self.creation()?;
        // The borrowed projection clones only existing Arcs/finite storage.
        // No typed source is reconstructed from RecordBatch fields.
        let source = self.runs[0].batch.clone().into_chunk();
        let temporary = creation.begin(&source)?;
        let payload = creation.begin(&source)?;
        let owner = creation.begin(&source)?;
        let key_concat_owner = creation.begin(&source)?;
        let key_owner = creation.begin(&source)?;
        let key_types = creation.begin(&source)?;
        let mut chunks = creation.vector::<Chunk>(self.runs.len(), temporary.as_ref())?;
        for run in &self.runs {
            chunks.push(run.batch.clone().into_chunk());
        }
        let batch = creation.concat(&chunks, payload.as_ref())?;
        let mut keys = creation.vector::<ArrayRef>(self.options.len(), temporary.as_ref())?;
        for key in 0..self.options.len() {
            let mut parts = creation.vector::<&ArrayRef>(self.runs.len(), temporary.as_ref())?;
            parts.extend(self.runs.iter().map(|run| &run.keys.columns()[key]));
            keys.push(creation.concat_array(&parts, &source, key_concat_owner.as_ref())?);
        }
        let mut columns = creation.vector::<SortColumn>(keys.len(), temporary.as_ref())?;
        columns.extend(
            keys.iter()
                .zip(&self.options)
                .map(|(values, options)| SortColumn {
                    values: Arc::clone(values),
                    options: Some(*options),
                }),
        );
        // Arrival order is the same final key and limit as the original body.
        creation.stable_index(&mut columns, batch.num_rows(), temporary.as_ref())?;
        let indices = creation.lexsort(&columns, take_rows, &source)?;
        let batch =
            creation.take_batch(&batch, indices.array().as_ref(), &source, owner.as_ref())?;
        let mut kept_keys = creation
            .vector::<ArrayRef>(if keep_keys { keys.len() } else { 0 }, key_owner.as_ref())?;
        if keep_keys {
            for key in &keys {
                kept_keys.push(creation.take_array(
                    key,
                    indices.array().as_ref(),
                    &source,
                    key_owner.as_ref(),
                )?);
            }
        }
        // Keep only source type/array responsibility, never the old runs,
        // temporary sort keys, factory, or runtime controller.
        let mut type_storage = key_types.storage_lifetime();
        let mut key_storage = if keep_keys {
            key_owner
                .storage_lifetime()
                .retaining_observed::<ConstructionFailure>(
                    &key_concat_owner.storage_lifetime(),
                    |layout| key_owner.request(layout),
                )?
        } else {
            key_owner.storage_lifetime()
        };
        // Dictionary/View take can reuse the concat result's private owners.
        // This is its finite result scope, not concat/preflight scratch.
        let mut payload_storage = owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(&payload.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        for run in &self.runs {
            if keep_keys {
                type_storage = type_storage.retaining_observed::<ConstructionFailure>(
                    &run.keys.storage_lifetime(),
                    |layout| key_types.request(layout),
                )?;
                key_storage = key_storage.retaining_observed::<ConstructionFailure>(
                    &run.keys.array_storage_lifetime(),
                    |layout| key_owner.request(layout),
                )?;
            }
            payload_storage = payload_storage.retaining_observed::<ConstructionFailure>(
                &run.batch.array_type_storage_lifetime(),
                |layout| owner.request(layout),
            )?;
        }
        let batch = creation
            .publish_with_storage(batch, &source, payload_storage, owner.as_ref())?
            .into_batch_projection();
        Ok(Some(SortedRun {
            batch,
            keys: CreatedSortColumns::new(kept_keys, type_storage, key_storage),
        }))
    }

    fn prune(&mut self) -> ExecutionResult<()> {
        let Some(keep) = self.keep else {
            return Ok(());
        };
        if self.buffered_rows < self.prune_rows || self.buffered_rows <= keep {
            return Ok(());
        }
        let kept = self.sort_runs(Some(keep), true)?;
        self.runs.clear();
        self.buffered_rows = 0;
        if let Some(run) = kept {
            self.buffered_rows = run.batch.len();
            self.runs.push(run);
        }
        Ok(())
    }

    fn final_output(&self) -> ExecutionResult<Option<Chunk>> {
        let Some(sorted) = self.sort_runs(self.keep, false)? else {
            return Ok(None);
        };
        let rows = sorted.batch.len();
        if self.offset >= rows {
            return Ok(None);
        }
        let len = self
            .limit
            .map_or(rows - self.offset, |limit| limit.min(rows - self.offset));
        if len == 0 {
            return Ok(None);
        }
        let source = sorted.batch.into_chunk();
        Ok(Some(self.creation()?.slice(&source, self.offset, len)?))
    }
}

impl OperatorLifecycle for CompiledSortProcessor {
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
        use crate::exec::pipeline::operator::ConstructionFailure;
        if self.output.is_some() {
            return Ok(());
        }
        let layout = self.program.graph().nodes()[self.output_node.index()].output_layout();
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        self.output = Some(factory.create_compiled_chunk_schema(layout)?);
        Ok(())
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
        self.runs.clear();
        self.buffered_rows = 0;
        self.instances = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.runs.clear();
        self.buffered_rows = 0;
        self.instances = None;
    }
}

impl InputEncodingPolicy for CompiledSortProcessor {}
impl TransformKernel for CompiledSortProcessor {
    fn apply(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        self.prepared_output()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if chunk.is_empty() {
            return Ok(StepResult::new(None, NextStep::NeedInput, rows));
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let output = self.prepared_output()?.clone();
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?
            .clone();
        let creation = SortCreation::new(factory.as_ref(), &output);
        creation.verify_chunks(std::slice::from_ref(&chunk))?;
        let owner = creation.begin(&chunk)?;
        let types = creation.begin(&chunk)?;
        let mut type_storage = creation.source_type_storage(&chunk, types.as_ref())?;
        let array_storage = owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &chunk.array_type_storage_lifetime(),
                |layout| owner.request(layout),
            )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut keys = creation.vector::<ArrayRef>(self.sites.len(), owner.as_ref())?;
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            let values = evaluate_chunk_all(instance, *site, &chunk, &self.control)?;
            let declared = root_value_type(&self.program, *site)?;
            type_storage = type_storage.retaining_observed::<ConstructionFailure>(
                &declared
                    .typed_binding()
                    .layout()
                    .arrow_backing_storage_lifetime(),
                |layout| types.request(layout),
            )?;
            keys.push(creation.normalize_key(
                &values,
                declared.value_type(),
                owner.as_ref(),
                types.as_ref(),
            )?);
        }
        let keys = CreatedSortColumns::new(keys, type_storage, array_storage);
        // Every arriving row's keys are evaluated, even when no row can be
        // kept, so a key's row error never depends on the limit.
        if self.keep == Some(0) {
            return Ok(StepResult::new(None, NextStep::NeedInput, rows));
        }
        self.buffered_rows = self.buffered_rows.saturating_add(chunk.len());
        self.runs.push(SortedRun {
            batch: chunk.into_batch_projection(),
            keys,
        });
        self.prune()?;
        Ok(StepResult::new(None, NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.prepared_output()?;
        let output = self.final_output()?;
        self.runs.clear();
        self.buffered_rows = 0;
        Ok(StepResult::new(output, NextStep::Finished, 0))
    }
}
