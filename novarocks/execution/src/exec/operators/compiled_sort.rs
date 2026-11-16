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

//! Compiled full/partitioned Sort and row-count/partition-rank TopN.
//!
//! LocalProgram carries a closed ordering mode. Full and partitioned sorts
//! emit the complete relation; row-count TopN emits its exact offset window.
//! Partition TopN keeps each partition's rows whose selected rank is at most K.
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
//! sorted rows. Row-count TopN emits `[offset, offset + limit)` and retains
//! its first `offset + limit` rows. Partition TopN retains rows whose selected
//! rank is at most K, including complete boundary peer groups for Rank and
//! DenseRank. Retained rows precede new arrivals in later sorts. Rank ties can
//! retain arbitrarily many rows; K is not a memory bound. Full sorts buffer
//! their whole input in memory (no spill, ADR-0162).

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{SortColumn, SortOptions};
use novarocks_local_program::{
    LocalProgram, LocalSortMode, PartitionRank, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind,
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

#[derive(Clone, Copy)]
struct RankRetention {
    partition_keys: usize,
    rank: PartitionRank,
    keep: usize,
}

/// TopN prunes its buffer to `offset + limit` rows once it holds at least
/// `max(PRUNE_FACTOR * kept, PRUNE_MIN_ROWS)` rows, as the legacy TopN does.
const PRUNE_FACTOR: usize = 4;
const PRUNE_MIN_ROWS: usize = 4096;

pub struct CompiledSortProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    options: Vec<SortOptions>,
    retention: Option<RankRetention>,
    limit: Option<usize>,
    offset: usize,
    prune_rows: usize,
    prune_min_rows: usize,
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
        let ProgramNodeKind::Sort { order_by, mode, .. } = graph_node.kind() else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {} is not a Sort", node.index()),
            ));
        };
        let at = node.index();
        let partition_exprs = mode.partition_by();
        let (limit, offset, retention) = match mode {
            LocalSortMode::Full | LocalSortMode::Partitioned { .. } => (None, 0, None),
            LocalSortMode::RowsTopN { limit, offset } => {
                let keep = offset.checked_add(*limit).ok_or_else(|| {
                    program_binding_failure(
                        PlanBindingKind::InvalidAssignment,
                        format!("compiled TopN at local node {at} limit and offset overflow"),
                    )
                })?;
                (
                    Some(*limit),
                    *offset,
                    Some(RankRetention {
                        partition_keys: 0,
                        rank: PartitionRank::RowNumber,
                        keep,
                    }),
                )
            }
            LocalSortMode::PartitionTopN {
                partition_by,
                limit,
                rank,
            } => (
                None,
                0,
                Some(RankRetention {
                    partition_keys: partition_by.len(),
                    rank: *rank,
                    keep: limit.get(),
                }),
            ),
        };
        if order_by.is_empty() && partition_exprs.is_empty() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled Sort at local node {at} has no ordering key"),
            ));
        }
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
        let prune_rows = retention.map_or(usize::MAX, |retention| {
            retention
                .keep
                .saturating_mul(PRUNE_FACTOR)
                .max(PRUNE_MIN_ROWS)
        });
        let name = if retention.is_some() {
            format!("COMPILED_TOP_N (node={at})")
        } else {
            format!("COMPILED_SORT (node={at})")
        };
        Ok(Self {
            name,
            program,
            sites,
            options,
            retention,
            limit,
            offset,
            prune_rows,
            prune_min_rows: PRUNE_MIN_ROWS,
            output_node: node,
            error,
        })
    }

    /// Prune at a smaller buffered row count, so a test can exercise TopN
    /// pruning with few rows. The kept prefix is unchanged.
    #[cfg(test)]
    pub(crate) fn with_prune_rows(mut self, rows: usize) -> Self {
        if self.retention.is_some() {
            self.prune_rows = rows;
            self.prune_min_rows = rows;
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
            retention: self.retention,
            limit: self.limit,
            offset: self.offset,
            prune_rows: self.prune_rows,
            prune_min_rows: self.prune_min_rows,
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
    retention: Option<RankRetention>,
    limit: Option<usize>,
    offset: usize,
    prune_rows: usize,
    prune_min_rows: usize,
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

    /// One rank-retention author for pruning and final output. Row-count
    /// TopN is the unpartitioned RowNumber fast path.
    // Design: ADR-0236 (docs/adr/ADR-0236-installed-optimization-domains-and-partition-rank-retention.md)
    fn retain(&self, keep_keys: bool) -> ExecutionResult<Option<SortedRun>> {
        let Some(retention) = self.retention else {
            return self.sort_runs(None, keep_keys);
        };
        if retention.partition_keys == 0 && retention.rank == PartitionRank::RowNumber {
            return self.sort_runs(Some(retention.keep), keep_keys);
        }
        let Some(sorted) = self.sort_runs(None, true)? else {
            return Ok(None);
        };
        let creation = self.creation()?;
        let source = sorted.batch.clone().into_chunk();
        let temporary = creation.begin(&source)?;
        let mut partition_converter = None;
        let partitions = creation.encode_keys(
            &mut partition_converter,
            &sorted.keys[..retention.partition_keys],
            sorted.keys.storage_lifetime(),
            super::sort::creation::encoding_failure,
        )?;
        let mut peer_converter = None;
        let peers = if retention.partition_keys < sorted.keys.len() {
            Some(creation.encode_keys(
                &mut peer_converter,
                &sorted.keys[retention.partition_keys..],
                sorted.keys.storage_lifetime(),
                super::sort::creation::encoding_failure,
            )?)
        } else {
            None
        };
        let mut selected = creation.vector::<u64>(source.len(), temporary.as_ref())?;
        let (mut position, mut group_first, mut groups) = (0, 0, 0);
        for row in 0..source.len() {
            creation.observe_work(1)?;
            let new_partition = row == 0 || partitions.row(row) != partitions.row(row - 1);
            if new_partition {
                position = 0;
                groups = 0;
            }
            position += 1;
            if new_partition
                || peers
                    .as_ref()
                    .is_some_and(|peers| peers.row(row) != peers.row(row - 1))
            {
                group_first = position;
                groups += 1;
            }
            let rank = match retention.rank {
                PartitionRank::RowNumber => position,
                PartitionRank::Rank => group_first,
                PartitionRank::DenseRank => groups,
            };
            if rank <= retention.keep {
                selected.push(u64::try_from(row).expect("supported row ordinal fits u64"));
            }
        }
        if selected.len() == source.len() {
            return Ok(Some(sorted));
        }
        creation.buffer_adoption(temporary.as_ref())?;
        let indices = arrow::array::UInt64Array::from(selected);
        let payload = creation.begin(&source)?;
        let key_owner = creation.begin(&source)?;
        let batch =
            creation.take_batch(source.record_batch(), &indices, &source, payload.as_ref())?;
        let mut keys = creation.vector::<ArrayRef>(
            if keep_keys { sorted.keys.len() } else { 0 },
            key_owner.as_ref(),
        )?;
        if keep_keys {
            for key in sorted.keys.columns() {
                keys.push(creation.take_array(key, &indices, &source, key_owner.as_ref())?);
            }
        }
        let key_storage = if keep_keys {
            key_owner
                .storage_lifetime()
                .retaining_observed::<ConstructionFailure>(
                    &sorted.keys.array_storage_lifetime(),
                    |layout| key_owner.request(layout),
                )?
        } else {
            key_owner.storage_lifetime()
        };
        let storage = payload
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &source.array_type_storage_lifetime(),
                |layout| payload.request(layout),
            )?;
        let batch = creation
            .publish_with_storage(batch, &source, storage, payload.as_ref())?
            .into_batch_projection();
        Ok(Some(SortedRun {
            batch,
            keys: CreatedSortColumns::new(keys, sorted.keys.storage_lifetime(), key_storage),
        }))
    }

    fn prune(&mut self) -> ExecutionResult<()> {
        let Some(retention) = self.retention else {
            return Ok(());
        };
        if self.buffered_rows < self.prune_rows
            || (retention.partition_keys == 0 && self.buffered_rows <= retention.keep)
        {
            return Ok(());
        }
        let kept = self.retain(true)?;
        self.runs.clear();
        self.buffered_rows = 0;
        if let Some(run) = kept {
            self.buffered_rows = run.batch.len();
            self.runs.push(run);
        }
        if retention.partition_keys > 0 {
            self.prune_rows = self
                .buffered_rows
                .saturating_mul(PRUNE_FACTOR)
                .max(self.prune_min_rows);
        }
        Ok(())
    }

    fn final_output(&self) -> ExecutionResult<Option<Chunk>> {
        let Some(sorted) = self.retain(false)? else {
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
        if self.retention.is_some_and(|retention| retention.keep == 0) {
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
