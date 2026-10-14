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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::{
    SortColumn, SortOptions, concat, concat_batches, lexsort_to_indices, take, take_record_batch,
};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, SortTopNType,
};

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use super::sort::{append_stable_row_index_sort_column, normalize_sort_key_array};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

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
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}

impl CompiledSortProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or_else(|| format!("compiled Sort node {} is absent", node.index()))?;
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
            return Err(format!(
                "compiled local node {} is not a Sort",
                node.index()
            ));
        };
        let at = node.index();
        if partition_limit.is_some() {
            return Err(format!(
                "compiled Sort at local node {at} with a partition limit is not executable"
            ));
        }
        if *use_top_n && !partition_exprs.is_empty() {
            return Err(format!(
                "compiled TopN at local node {at} with partition keys is not executable"
            ));
        }
        if *topn_type != SortTopNType::RowNumber {
            return Err(format!(
                "compiled TopN at local node {at} with {topn_type:?} ranking is not executable"
            ));
        }
        if max_buffered_rows.is_some() || max_buffered_bytes.is_some() {
            return Err(format!(
                "compiled Sort at local node {at} carries a buffering cap the compiler never authors"
            ));
        }
        if order_by.is_empty() && partition_exprs.is_empty() {
            return Err(format!(
                "compiled Sort at local node {at} has no ordering key"
            ));
        }
        let keep = if *use_top_n {
            let limit = limit
                .ok_or_else(|| format!("compiled TopN at local node {at} has no row limit"))?;
            Some(offset.checked_add(limit).ok_or_else(|| {
                format!("compiled TopN at local node {at} limit and offset overflow")
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
            let key_ordinal = u32::try_from(ordinal)
                .map_err(|_| format!("compiled Sort at local node {at} has too many keys"))?;
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
        let output = ChunkSchema::from_compiled_layout(graph_node.output_layout())?;
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
            output,
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
        Box::new(CompiledSortProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            options: self.options.clone(),
            keep: self.keep,
            limit: self.limit,
            offset: self.offset,
            prune_rows: self.prune_rows,
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            runs: Vec::new(),
            buffered_rows: 0,
            pending: None,
            finishing: false,
            finished: false,
        })
    }
}

/// Payload rows with their already evaluated, normalized key columns.
struct SortedRun {
    chunk: Chunk,
    keys: Vec<ArrayRef>,
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
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    runs: Vec<SortedRun>,
    buffered_rows: usize,
    pending: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

fn failure(context: &str, error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from(format!("{context}: {error}"))
}

impl CompiledSortProcessor {
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
        let schema = self.output.arrow_schema_ref();
        let batch = concat_batches(&schema, self.runs.iter().map(|run| &run.chunk.batch))
            .map_err(|error| failure("compiled Sort payload concat", error))?;
        let mut keys = Vec::with_capacity(self.options.len());
        for key in 0..self.options.len() {
            let parts = self
                .runs
                .iter()
                .map(|run| run.keys[key].as_ref())
                .collect::<Vec<&dyn Array>>();
            keys.push(concat(&parts).map_err(|error| failure("compiled Sort key concat", error))?);
        }
        let mut columns = keys
            .iter()
            .zip(&self.options)
            .map(|(values, options)| SortColumn {
                values: Arc::clone(values),
                options: Some(*options),
            })
            .collect::<Vec<_>>();
        // The arrival-order row index is the last key, so ties are stable and
        // a bounded prefix selects exactly the same rows as a full sort.
        append_stable_row_index_sort_column(&mut columns, batch.num_rows());
        let indices = lexsort_to_indices(&columns, take_rows)
            .map_err(|error| failure("compiled Sort order", error))?;
        let batch = take_record_batch(&batch, &indices)
            .map_err(|error| failure("compiled Sort payload take", error))?;
        let keys = if keep_keys {
            keys.iter()
                .map(|key| take(key.as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| failure("compiled Sort key take", error))?
        } else {
            Vec::new()
        };
        Ok(Some(SortedRun {
            chunk: Chunk::try_new_with_chunk_schema(batch, Arc::clone(&self.output))?,
            keys,
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
            self.buffered_rows = run.chunk.len();
            self.runs.push(run);
        }
        Ok(())
    }

    fn final_output(&self) -> ExecutionResult<Option<Chunk>> {
        let Some(sorted) = self.sort_runs(self.keep, false)? else {
            return Ok(None);
        };
        let rows = sorted.chunk.len();
        if self.offset >= rows {
            return Ok(None);
        }
        let len = self
            .limit
            .map_or(rows - self.offset, |limit| limit.min(rows - self.offset));
        if len == 0 {
            return Ok(None);
        }
        let batch = sorted.chunk.batch.slice(self.offset, len);
        Ok(Some(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?))
    }
}

impl Operator for CompiledSortProcessor {
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

impl ProcessorOperator for CompiledSortProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Err("compiled Sort received input after finishing".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut keys = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            let values = evaluate_all(instance, *site, &chunk.batch, &self.control)?;
            keys.push(normalize_sort_key_array(&values).map_err(ExecutionFailure::from)?);
        }
        // Every arriving row's keys are evaluated, even when no row can be
        // kept, so a key's row error never depends on the limit.
        if self.keep == Some(0) {
            return Ok(());
        }
        self.buffered_rows = self.buffered_rows.saturating_add(chunk.len());
        self.runs.push(SortedRun { chunk, keys });
        self.prune()
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.take();
        if self.finishing {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        self.pending = self.final_output()?;
        self.runs.clear();
        self.buffered_rows = 0;
        if self.pending.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}
