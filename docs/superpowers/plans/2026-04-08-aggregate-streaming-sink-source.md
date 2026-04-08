# Aggregate Streaming Sink/Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the blocking `AggregateProcessorOperator` into Streaming Sink + Source operators connected by a bounded chunk buffer, enabling pipeline-level yield points required for TopN runtime filters to take effect.

**Architecture:** New `AggregateStreamingSinkOperator` (owns hash table, publishes TopN RF) writes result chunks to a shared `AggregateStreamingState` buffer. A new `AggregateStreamingSourceOperator` pulls from that buffer. The pipeline builder creates two pipelines when `streaming_preaggregation_mode` is set on the AGG node. Both operators implement the existing `ProcessorOperator` trait following the AnalyticSink/Source convention.

**Tech Stack:** Rust, Arrow RecordBatch, existing pipeline/operator framework, thrift-generated plan types.

**Spec:** `docs/superpowers/specs/2026-04-08-aggregate-streaming-sink-source-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/exec/node/aggregate.rs` | Add `StreamingPreaggregationMode` enum and field to `AggregateNode` |
| `src/lower/node/aggregate.rs` | Parse `streaming_preaggregation_mode` from thrift into `AggregateNode` |
| `src/exec/operators/aggregate/streaming_state.rs` | **New**: `AggregateStreamingState` — bounded chunk buffer with Observable |
| `src/exec/operators/aggregate/streaming_sink.rs` | **New**: `AggregateStreamingSinkFactory` + `AggregateStreamingSinkOperator` |
| `src/exec/operators/aggregate/streaming_source.rs` | **New**: `AggregateStreamingSourceFactory` + `AggregateStreamingSourceOperator` |
| `src/exec/operators/aggregate/mod.rs` | Add submodule declarations, re-export new factories |
| `src/exec/operators/mod.rs` | Re-export new factories |
| `src/exec/pipeline/builder.rs` | Route streaming AGG nodes through Sink/Source pipeline split |

---

### Task 1: Add `StreamingPreaggregationMode` to `AggregateNode`

**Files:**
- Modify: `src/exec/node/aggregate.rs:59-71`

- [ ] **Step 1: Add the enum and field**

In `src/exec/node/aggregate.rs`, add the enum before `AggregateNode` and a new field:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamingPreaggregationMode {
    Auto,
    ForceStreaming,
    ForcePreaggregation,
    LimitedMem,
}
```

Add field to `AggregateNode`:

```rust
pub struct AggregateNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub group_by: Vec<ExprId>,
    pub functions: Vec<AggFunction>,
    pub need_finalize: bool,
    pub input_is_intermediate: bool,
    pub output_chunk_schema: ChunkSchemaRef,
    pub topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    pub streaming_preaggregation_mode: Option<StreamingPreaggregationMode>,
}
```

- [ ] **Step 2: Fix compilation errors from new field**

Every place that constructs `AggregateNode` must add `streaming_preaggregation_mode`. There is one construction site in the lowering code at `src/lower/node/aggregate.rs:212-226`. Add `streaming_preaggregation_mode: None` for now — we'll fill it with real parsing in the next task.

Also fix the destructure in `src/exec/pipeline/builder.rs:648-657` — add the new field:

```rust
ExecNodeKind::Aggregate(AggregateNode {
    input,
    node_id,
    group_by,
    functions,
    need_finalize,
    input_is_intermediate: _input_is_intermediate,
    output_chunk_schema,
    topn_rf_specs,
    streaming_preaggregation_mode,
}) => {
```

And in `src/sql/physical/nodes.rs` if there is a construction site — search for `AggregateNode {` and add the field.

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | tail -20`
Expected: BUILD SUCCESS (or only unrelated warnings)

- [ ] **Step 4: Commit**

```bash
git add src/exec/node/aggregate.rs src/lower/node/aggregate.rs src/exec/pipeline/builder.rs src/sql/physical/nodes.rs
git commit -m "Add StreamingPreaggregationMode enum to AggregateNode"
```

---

### Task 2: Parse `streaming_preaggregation_mode` from thrift

**Files:**
- Modify: `src/lower/node/aggregate.rs:212-226`

- [ ] **Step 1: Parse the thrift field**

In `lower_aggregate_node()`, before the final `Ok(Lowered { ... })` block, add parsing of the streaming mode from `TAggregationNode`. The thrift-generated type is `plan_nodes::TStreamingPreaggregationMode` with constants `AUTO`, `FORCE_STREAMING`, `FORCE_PREAGGREGATION`, `LIMITED_MEM`.

Add this import at the top of the file alongside existing imports:

```rust
use crate::exec::node::aggregate::StreamingPreaggregationMode;
```

Add this parsing code before the `Ok(Lowered { ... })` block (after the `topn_rf_specs` parsing, around line 210):

```rust
    let streaming_preaggregation_mode = agg
        .streaming_preaggregation_mode
        .map(|mode| {
            use crate::plan_nodes::TStreamingPreaggregationMode;
            match mode {
                TStreamingPreaggregationMode::AUTO => StreamingPreaggregationMode::Auto,
                TStreamingPreaggregationMode::FORCE_STREAMING => {
                    StreamingPreaggregationMode::ForceStreaming
                }
                TStreamingPreaggregationMode::FORCE_PREAGGREGATION => {
                    StreamingPreaggregationMode::ForcePreaggregation
                }
                TStreamingPreaggregationMode::LIMITED_MEM => {
                    StreamingPreaggregationMode::LimitedMem
                }
                _ => StreamingPreaggregationMode::Auto,
            }
        });
```

- [ ] **Step 2: Wire the parsed value into AggregateNode**

Update the `AggregateNode` construction in the `Ok(Lowered { ... })` block:

```rust
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                group_by,
                functions,
                need_finalize: agg.need_finalize,
                input_is_intermediate,
                output_chunk_schema,
                topn_rf_specs,
                streaming_preaggregation_mode,
            }),
        },
        layout: out_layout.clone(),
    })
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | tail -20`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/lower/node/aggregate.rs
git commit -m "Parse streaming_preaggregation_mode from thrift TAggregationNode"
```

---

### Task 3: Create `AggregateStreamingState` (shared buffer)

**Files:**
- Create: `src/exec/operators/aggregate/streaming_state.rs`
- Modify: `src/exec/operators/aggregate/mod.rs` (add submodule)

- [ ] **Step 1: Create the streaming state module**

Create `src/exec/operators/aggregate/streaming_state.rs`:

```rust
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::common::config::operator_buffer_chunks;
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::schedule::observer::Observable;

struct BufferState {
    chunks: VecDeque<Chunk>,
    sink_finished: bool,
}

#[derive(Clone)]
pub(crate) struct AggregateStreamingState {
    inner: Arc<Mutex<BufferState>>,
    observable: Arc<Observable>,
    buffer_limit: usize,
}

impl AggregateStreamingState {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BufferState {
                chunks: VecDeque::new(),
                sink_finished: false,
            })),
            observable: Arc::new(Observable::new()),
            buffer_limit: operator_buffer_chunks().max(1),
        }
    }

    /// Push a chunk into the buffer. Called by the Sink operator.
    /// Notifies the Source via the observable.
    pub(crate) fn offer_chunk(&self, chunk: Chunk) {
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("streaming state lock");
        if !chunk.is_empty() {
            guard.chunks.push_back(chunk);
        }
        drop(guard);
        notify.arm();
    }

    /// Mark the Sink as finished. No more chunks will be offered.
    /// Notifies the Source.
    pub(crate) fn mark_sink_finished(&self) {
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("streaming state lock");
        guard.sink_finished = true;
        drop(guard);
        notify.arm();
    }

    /// Pull a chunk from the buffer. Called by the Source operator.
    pub(crate) fn poll_chunk(&self) -> Option<Chunk> {
        let mut guard = self.inner.lock().expect("streaming state lock");
        guard.chunks.pop_front()
    }

    /// Returns true if the buffer has chunks available.
    pub(crate) fn has_chunks(&self) -> bool {
        let guard = self.inner.lock().expect("streaming state lock");
        !guard.chunks.is_empty()
    }

    /// Returns true if the buffer is full (at or above the limit).
    pub(crate) fn is_buffer_full(&self) -> bool {
        let guard = self.inner.lock().expect("streaming state lock");
        guard.chunks.len() >= self.buffer_limit
    }

    /// Returns true when the Sink has finished AND the buffer is drained.
    pub(crate) fn is_done(&self) -> bool {
        let guard = self.inner.lock().expect("streaming state lock");
        guard.sink_finished && guard.chunks.is_empty()
    }

    /// Returns the Observable for driver scheduling (Source side).
    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }
}
```

- [ ] **Step 2: Register the submodule**

In `src/exec/operators/aggregate/mod.rs`, add at the top (after the module-level comment, before existing `use` statements):

```rust
pub(crate) mod streaming_state;
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | tail -20`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/exec/operators/aggregate/streaming_state.rs src/exec/operators/aggregate/mod.rs
git commit -m "Add AggregateStreamingState bounded chunk buffer"
```

---

### Task 4: Create `AggregateStreamingSinkOperator`

**Files:**
- Create: `src/exec/operators/aggregate/streaming_sink.rs`
- Modify: `src/exec/operators/aggregate/mod.rs` (add submodule + re-export)

This is the largest task. The Sink operator owns the hash table and aggregate state — it reuses the
core aggregation logic from the existing `AggregateProcessorOperator`.

- [ ] **Step 1: Create the streaming sink module**

Create `src/exec/operators/aggregate/streaming_sink.rs`. The operator is structurally identical to
`AggregateProcessorOperator` but follows the sink convention (`has_output = false`,
`pull_chunk = None`), and on `set_finishing()` pushes results to the shared buffer instead of
`pending_output`.

```rust
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::common::failpoint;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::agg;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_builder::{GroupKeyArrayView, build_group_key_views};
use crate::exec::hash_table::key_column::build_output_schema_from_kernels;
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
use crate::exec::node::aggregate::{AggFunction, TopNRuntimeFilterSpec};
use crate::exec::operators::aggregate::streaming_state::AggregateStreamingState;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::runtime_filter::min_max::RuntimeMinMaxFilter;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_state::RuntimeState;

use super::{ENABLE_GROUP_KEY_OPTIMIZATIONS, build_agg_views};

pub struct AggregateStreamingSinkFactory {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    output_intermediate: bool,
    output_chunk_schema: ChunkSchemaRef,
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    state: AggregateStreamingState,
}

impl AggregateStreamingSinkFactory {
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        group_by: Vec<ExprId>,
        functions: Vec<AggFunction>,
        output_intermediate: bool,
        output_chunk_schema: ChunkSchemaRef,
        topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
        runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
        state: AggregateStreamingState,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("AGG_STREAMING_SINK (id={node_id})")
        } else {
            "AGG_STREAMING_SINK".to_string()
        };
        Self {
            name,
            arena,
            group_by,
            functions,
            output_intermediate,
            output_chunk_schema,
            topn_rf_specs,
            runtime_filter_hub,
            state,
        }
    }
}

impl OperatorFactory for AggregateStreamingSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AggregateStreamingSinkOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            group_by: self.group_by.clone(),
            functions: self.functions.clone(),
            key_table: None,
            state_arena: agg::AggStateArena::new(64 * 1024),
            group_states: Vec::new(),
            state_ptrs: Vec::new(),
            kernels: None,
            output_intermediate: self.output_intermediate,
            initialized: false,
            data_initialized: false,
            finishing: false,
            finished: false,
            output_schema: None,
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            observed_group_key_nullable: vec![false; self.group_by.len()],
            key_table_mem_tracker: None,
            topn_rf_specs: self.topn_rf_specs.clone(),
            runtime_filter_hub: self.runtime_filter_hub.clone(),
            topn_rf_rows_since_publish: 0,
            streaming_state: self.state.clone(),
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct AggregateStreamingSinkOperator {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    key_table: Option<KeyTable>,
    state_arena: agg::AggStateArena,
    group_states: Vec<agg::AggStatePtr>,
    state_ptrs: Vec<agg::AggStatePtr>,
    kernels: Option<agg::AggKernelSet>,
    output_intermediate: bool,
    initialized: bool,
    data_initialized: bool,
    finishing: bool,
    finished: bool,
    output_schema: Option<SchemaRef>,
    output_chunk_schema: ChunkSchemaRef,
    observed_group_key_nullable: Vec<bool>,
    key_table_mem_tracker: Option<Arc<MemTracker>>,
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    topn_rf_rows_since_publish: usize,
    streaming_state: AggregateStreamingState,
}

impl Operator for AggregateStreamingSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let arena = MemTracker::new_child("AggStateArena", &tracker);
        self.state_arena.set_mem_tracker(Arc::clone(&arena));
        let key_table = MemTracker::new_child("KeyTable", &tracker);
        if let Some(table) = self.key_table.as_mut() {
            table.set_mem_tracker(Arc::clone(&key_table));
        }
        self.key_table_mem_tracker = Some(key_table);
    }

    fn prepare(&mut self) -> Result<(), String> {
        self.init_from_plan()
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

impl ProcessorOperator for AggregateStreamingSinkOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }

    fn has_output(&self) -> bool {
        false // Sink convention
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished || self.finishing {
            return Ok(());
        }
        let num_rows = chunk.len();
        self.process(chunk)?;
        self.topn_rf_rows_since_publish += num_rows;
        self.try_publish_topn_runtime_filter();
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None) // Sink convention
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished || self.finishing {
            return Ok(());
        }
        self.finishing = true;
        // Materialize all groups from hash table and push into shared buffer.
        let chunks = self.finish_to_chunks()?;
        for chunk in chunks {
            self.streaming_state.offer_chunk(chunk);
        }
        self.streaming_state.mark_sink_finished();
        self.finished = true;
        Ok(())
    }
}

// ----- Private implementation methods -----
// These are extracted/adapted from AggregateProcessorOperator's private methods.
// The core hash-table update logic (process) and finalization (finish_to_chunks)
// are structurally identical to the existing operator.

impl AggregateStreamingSinkOperator {
    fn try_publish_topn_runtime_filter(&mut self) {
        const PUBLISH_THRESHOLD: usize = 4096;

        if self.topn_rf_specs.is_empty() {
            return;
        }
        let Some(hub) = &self.runtime_filter_hub else {
            return;
        };
        let Some(key_table) = &self.key_table else {
            return;
        };
        if self.topn_rf_rows_since_publish < PUBLISH_THRESHOLD {
            return;
        }

        for spec in &self.topn_rf_specs {
            if key_table.group_count() < spec.limit {
                continue;
            }
            let key_columns = key_table.key_columns();
            let Some(key_col) = key_columns.get(spec.expr_order) else {
                continue;
            };
            let column_array = match key_col.to_array() {
                Ok(arr) => arr,
                Err(_) => continue,
            };
            let filter = match RuntimeMinMaxFilter::from_array(spec.build_type, &column_array) {
                Ok(f) => f,
                Err(_) => continue,
            };
            hub.publish_min_max_filter(spec.filter_id, filter);
        }
        self.topn_rf_rows_since_publish = 0;
    }

    /// Core hash-table aggregation logic — identical to AggregateProcessorOperator::process().
    fn process(&mut self, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }

        if chunk.is_empty() && chunk.schema().fields().is_empty() {
            return Ok(());
        }

        let group_arrays = self.eval_group_by_arrays(&chunk)?;
        let agg_arrays = self.eval_agg_arrays(&chunk)?;

        self.ensure_data_initialized(&group_arrays, &agg_arrays)?;
        self.refresh_output_schema_for_group_arrays(&group_arrays)?;

        if chunk.is_empty() {
            return Ok(());
        }

        if !self.group_by.is_empty() {
            let failpoint_name = if self.functions.is_empty() {
                failpoint::AGG_HASH_SET_BAD_ALLOC
            } else {
                failpoint::AGGREGATE_BUILD_HASH_MAP_BAD_ALLOC
            };
            failpoint::maybe_error(
                failpoint_name,
                "Mem usage has exceed the limit of BE: BE:10004",
            )?;
        }

        let num_rows = chunk.len();
        if self.group_by.is_empty() {
            self.ensure_scalar_group()?;
            let state_ptr = *self
                .group_states
                .get(0)
                .ok_or_else(|| "aggregate scalar state missing".to_string())?;
            self.state_ptrs.clear();
            self.state_ptrs.resize(num_rows, state_ptr);
            let kernels = self
                .kernels
                .as_ref()
                .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
            let agg_views = build_agg_views(&kernels.entries, &self.functions, &agg_arrays)?;
            for (idx, (kernel, view)) in kernels.entries.iter().zip(agg_views.iter()).enumerate() {
                if self
                    .functions
                    .get(idx)
                    .map(|f| f.input_is_intermediate)
                    .unwrap_or(false)
                {
                    kernel.merge_batch(&self.state_ptrs, view)?;
                } else {
                    kernel.update_batch(&self.state_ptrs, view)?;
                }
            }
            return Ok(());
        }

        let key_views = build_group_key_views(&group_arrays)?;
        let mut key_table = self
            .key_table
            .take()
            .ok_or_else(|| "aggregate key table missing".to_string())?;
        let result: Result<(), String> = (|| {
            let mut group_ids = Vec::with_capacity(num_rows);
            match key_table.key_strategy() {
                GroupKeyStrategy::Serialized => {
                    let rows_result = key_table.build_rows(&group_arrays);
                    let fallback_rows = match &rows_result {
                        Ok(_) => None,
                        Err(err) if err.contains("row converter not initialized") => Some(
                            key_table.build_rows_fallback(&group_arrays)?,
                        ),
                        Err(err) => return Err(err.to_string()),
                    };
                    let rows = rows_result.ok();
                    let hashes = key_table.build_group_hashes(&key_views, num_rows)?;
                    for row in 0..num_rows {
                        let row_bytes = if let Some(rows) = rows.as_ref() {
                            rows.row(row).data()
                        } else {
                            fallback_rows
                                .as_ref()
                                .and_then(|all| all.get(row))
                                .map(|v| v.as_slice())
                                .ok_or_else(|| {
                                    format!(
                                        "fallback serialized group row missing at row={} (rows={})",
                                        row, num_rows
                                    )
                                })?
                        };
                        let lookup = key_table
                            .find_or_insert_from_row(&key_views, row, row_bytes, hashes[row])?;
                        self.ensure_group_state(&lookup)?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::Scalar => {
                    return Err(
                        "group key strategy Scalar is invalid for group by".to_string(),
                    );
                }
                GroupKeyStrategy::OneNumber => {
                    let view = key_views
                        .get(0)
                        .ok_or_else(|| "one number key view missing".to_string())?;
                    let hashes = key_table.build_one_number_hashes(view, num_rows)?;
                    for row in 0..num_rows {
                        let lookup =
                            key_table.find_or_insert_one_number(view, row, hashes[row])?;
                        self.ensure_group_state(&lookup)?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::OneString => {
                    let view = key_views
                        .get(0)
                        .ok_or_else(|| "one string key view missing".to_string())?;
                    let GroupKeyArrayView::Utf8(arr) = view else {
                        return Err("one string key expects Utf8 view".to_string());
                    };
                    let hashes = key_table.build_group_hashes(&key_views, num_rows)?;
                    for row in 0..num_rows {
                        let lookup = if arr.is_null(row) {
                            key_table
                                .find_or_insert_one_string(view, row, None, hashes[row])?
                        } else {
                            let key = arr.value(row);
                            key_table
                                .find_or_insert_one_string(view, row, Some(key), hashes[row])?
                        };
                        self.ensure_group_state(&lookup)?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::FixedSize => {
                    let hashes = key_table.build_group_hashes(&key_views, num_rows)?;
                    for row in 0..num_rows {
                        let lookup =
                            key_table.find_or_insert_fixed_size(&key_views, row, hashes[row])?;
                        self.ensure_group_state(&lookup)?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::CompressedFixed => {
                    let keys = key_table.build_compressed_flags(&key_views, num_rows)?;
                    let hashes = key_table.build_group_hashes(&key_views, num_rows)?;
                    let mut rows_opt = None;
                    for row in 0..num_rows {
                        let lookup = if keys[row] {
                            key_table
                                .find_or_insert_compressed(&key_views, row, hashes[row])?
                        } else {
                            if rows_opt.is_none() {
                                rows_opt = Some(key_table.build_rows(&group_arrays)?);
                            }
                            let rows = rows_opt.as_ref().expect("group rows");
                            let row_bytes = rows.row(row).data();
                            key_table
                                .find_or_insert_from_row(&key_views, row, row_bytes, hashes[row])?
                        };
                        self.ensure_group_state(&lookup)?;
                        group_ids.push(lookup.group_id);
                    }
                }
            }

            if group_ids.len() != num_rows {
                return Err("aggregate group id count mismatch".to_string());
            }

            self.state_ptrs.clear();
            self.state_ptrs.reserve(num_rows);
            for &group_id in &group_ids {
                let state_ptr = *self
                    .group_states
                    .get(group_id)
                    .ok_or_else(|| "aggregate state missing".to_string())?;
                self.state_ptrs.push(state_ptr);
            }
            let kernels = self
                .kernels
                .as_ref()
                .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
            let agg_views = build_agg_views(&kernels.entries, &self.functions, &agg_arrays)?;
            for (idx, (kernel, view)) in kernels.entries.iter().zip(agg_views.iter()).enumerate() {
                if self
                    .functions
                    .get(idx)
                    .map(|f| f.input_is_intermediate)
                    .unwrap_or(false)
                {
                    kernel.merge_batch(&self.state_ptrs, view)?;
                } else {
                    kernel.update_batch(&self.state_ptrs, view)?;
                }
            }
            Ok(())
        })();
        self.key_table = Some(key_table);
        result?;
        Ok(())
    }

    /// Materialize all groups from the hash table into output chunks.
    /// Returns a Vec of Chunks (may be empty if no groups).
    fn finish_to_chunks(&mut self) -> Result<Vec<Chunk>, String> {
        if !self.initialized {
            return Err("aggregate operator not prepared".to_string());
        }

        if !self.group_by.is_empty() {
            let observed = self
                .key_table
                .as_ref()
                .map(|table| {
                    table
                        .key_columns()
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| {
                            self.observed_group_key_nullable
                                .get(idx)
                                .copied()
                                .unwrap_or(false)
                                || col.has_nulls()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| self.observed_group_key_nullable.clone());
            self.rebuild_output_schema(Some(&observed))?;
        }

        if self.group_states.is_empty() {
            if self.group_by.is_empty() {
                self.ensure_scalar_group()?;
            } else {
                let schema = self
                    .output_schema
                    .clone()
                    .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())));
                let batch = RecordBatch::new_empty(schema);
                let chunk = self.output_chunk_from_batch(batch)?;
                return Ok(if chunk.is_empty() { vec![] } else { vec![chunk] });
            }
        }

        let schema = self
            .output_schema
            .clone()
            .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())));
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        let key_count = self
            .key_table
            .as_ref()
            .map(|table| table.key_columns().len())
            .unwrap_or(0);
        let mut arrays = Vec::with_capacity(key_count + kernels.entries.len());
        if let Some(table) = self.key_table.as_ref() {
            for col in table.key_columns() {
                arrays.push(col.to_array()?);
            }
        }
        for kernel in &kernels.entries {
            arrays.push(kernel.build_array(&self.group_states, self.output_intermediate)?);
        }

        let batch = if arrays.is_empty() {
            let options = arrow::array::RecordBatchOptions::new()
                .with_row_count(Some(self.group_states.len()));
            RecordBatch::try_new_with_options(schema, arrays, &options)
        } else {
            RecordBatch::try_new(schema, arrays)
        }
        .map_err(|e| e.to_string())?;
        self.drop_group_states();
        let chunk = self.output_chunk_from_batch(batch)?;
        Ok(if chunk.is_empty() { vec![] } else { vec![chunk] })
    }

    // --- Helper methods below are identical to AggregateProcessorOperator ---

    fn init_from_plan(&mut self) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }
        let expected_group_types = self.expected_group_types()?;
        let expected_agg_types = self.expected_agg_input_types()?;
        if !expected_group_types.is_empty() {
            self.key_table = Some(KeyTable::new(
                expected_group_types.clone(),
                ENABLE_GROUP_KEY_OPTIMIZATIONS,
            )?);
        }
        let kernels = agg::build_kernel_set(&self.functions, &expected_agg_types)?;
        self.kernels = Some(kernels);
        if self.kernels.is_some() {
            self.rebuild_output_schema(None)?;
        }
        self.initialized = true;
        Ok(())
    }

    fn rebuild_output_schema(&mut self, group_key_nullable: Option<&[bool]>) -> Result<(), String> {
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        let key_columns = self
            .key_table
            .as_ref()
            .map(|table| table.key_columns())
            .unwrap_or(&[]);
        self.output_schema = Some(build_output_schema_from_kernels(
            key_columns,
            &kernels.entries,
            self.output_intermediate,
            &self.output_chunk_schema,
            group_key_nullable,
        )?);
        Ok(())
    }

    fn eval_group_by_arrays(&self, chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
        let mut arrays = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let array = self.arena.eval(*expr, chunk)?;
            arrays.push(array);
        }
        Ok(arrays)
    }

    fn eval_agg_arrays(&self, chunk: &Chunk) -> Result<Vec<Option<ArrayRef>>, String> {
        let mut arrays = Vec::with_capacity(self.functions.len());
        for func in &self.functions {
            let array = if func.inputs.is_empty() {
                None
            } else if func.inputs.len() == 1 {
                Some(self.arena.eval(func.inputs[0], chunk)?)
            } else {
                return Err(format!(
                    "aggregate inputs must be packed into a single struct expression: {} has {} inputs",
                    func.name,
                    func.inputs.len()
                ));
            };
            arrays.push(array);
        }
        Ok(arrays)
    }

    fn ensure_data_initialized(
        &mut self,
        group_arrays: &[ArrayRef],
        agg_arrays: &[Option<ArrayRef>],
    ) -> Result<(), String> {
        if !self.initialized {
            return Err("aggregate operator not prepared".to_string());
        }
        if self.data_initialized {
            return Ok(());
        }
        if !self.group_by.is_empty() && group_arrays.len() != self.group_by.len() {
            return Err("group_by arrays length mismatch".to_string());
        }
        if agg_arrays.len() != self.functions.len() {
            return Err("aggregate arrays length mismatch".to_string());
        }
        let expected_group_types = self.expected_group_types()?;
        let expected_agg_types = self.expected_agg_input_types()?;
        self.validate_group_array_types(&expected_group_types, group_arrays)?;
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        self.validate_agg_array_types(&expected_agg_types, &kernels.entries, agg_arrays)?;
        if let Some(table) = self.key_table.as_mut() {
            if table.key_strategy() == GroupKeyStrategy::CompressedFixed
                && table.compressed_ctx().is_none()
            {
                if group_arrays.first().map_or(0, |array| array.len()) == 0 {
                    return Ok(());
                }
                let views = build_group_key_views(group_arrays)?;
                table.ensure_compressed_ctx(&views)?;
            }
        }
        self.data_initialized = true;
        Ok(())
    }

    fn refresh_output_schema_for_group_arrays(
        &mut self,
        group_arrays: &[ArrayRef],
    ) -> Result<(), String> {
        if self.group_by.is_empty() {
            return Ok(());
        }
        let group_key_nullable: Vec<bool> = group_arrays
            .iter()
            .map(|array| array.null_count() > 0)
            .collect();
        if self.observed_group_key_nullable.len() != group_key_nullable.len() {
            self.observed_group_key_nullable = vec![false; group_key_nullable.len()];
        }
        let mut changed = false;
        for (observed, current) in self
            .observed_group_key_nullable
            .iter_mut()
            .zip(group_key_nullable.iter())
        {
            let next = *observed || *current;
            changed |= next != *observed;
            *observed = next;
        }
        if !changed
            && !self
                .observed_group_key_nullable
                .iter()
                .any(|nullable| *nullable)
        {
            return Ok(());
        }
        let observed = self.observed_group_key_nullable.clone();
        self.rebuild_output_schema(Some(&observed))
    }

    fn output_chunk_from_batch(&self, batch: RecordBatch) -> Result<Chunk, String> {
        let output_len = batch.num_columns();
        if self.output_chunk_schema.slot_ids().len() < output_len {
            return Err(format!(
                "aggregate output slot count mismatch: batch_columns={} output_slots={}",
                output_len,
                self.output_chunk_schema.slot_ids().len()
            ));
        }
        let batch_schema = batch.schema();
        let slot_schemas = self.output_chunk_schema.slot_ids()[..output_len]
            .iter()
            .enumerate()
            .map(|(idx, slot_id)| {
                let slot_schema = self.output_chunk_schema.slot(*slot_id).ok_or_else(|| {
                    format!(
                        "aggregate explicit output chunk schema missing slot {}",
                        slot_id
                    )
                })?;
                let field = batch_schema.field(idx);
                slot_schema.with_field_and_slot_id(*slot_id, field.as_ref().clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::try_new(slot_schemas)?))
    }

    fn expected_group_types(&self) -> Result<Vec<DataType>, String> {
        let mut types = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let data_type = self
                .arena
                .data_type(*expr)
                .ok_or_else(|| "group by type missing".to_string())?
                .clone();
            if matches!(data_type, DataType::Null) {
                return Err("group by type is null".to_string());
            }
            types.push(data_type);
        }
        Ok(types)
    }

    fn expected_agg_input_types(&self) -> Result<Vec<Option<DataType>>, String> {
        let mut types = Vec::with_capacity(self.functions.len());
        for func in &self.functions {
            if func.input_is_intermediate {
                if let Some(sig) = func.types.as_ref() {
                    if let Some(intermediate) = sig.intermediate_type.as_ref() {
                        if matches!(intermediate, DataType::Null) {
                            return Err("aggregate intermediate type is null".to_string());
                        }
                        types.push(Some(intermediate.clone()));
                        continue;
                    }
                }
            }
            let data_type = match (func.name.as_str(), func.inputs.as_slice()) {
                ("count", []) => None,
                (_, [expr]) => Some(
                    self.arena
                        .data_type(*expr)
                        .ok_or_else(|| "aggregate input type missing".to_string())?
                        .clone(),
                ),
                (_, []) => return Err("aggregate input missing".to_string()),
                (_, _) => {
                    return Err(format!(
                        "aggregate inputs must be packed into a single struct expression: {} has {} inputs",
                        func.name,
                        func.inputs.len()
                    ));
                }
            };
            if matches!(data_type, Some(DataType::Null)) {
                return Err("aggregate input type is null".to_string());
            }
            types.push(data_type);
        }
        Ok(types)
    }

    fn validate_group_array_types(
        &self,
        expected: &[DataType],
        arrays: &[ArrayRef],
    ) -> Result<(), String> {
        if expected.len() != arrays.len() {
            return Err("group by type length mismatch".to_string());
        }
        for (idx, (expected_type, array)) in expected.iter().zip(arrays.iter()).enumerate() {
            if expected_type != array.data_type() {
                return Err(format!(
                    "group by type mismatch at {}: expected {:?}, got {:?}",
                    idx, expected_type, array.data_type()
                ));
            }
        }
        Ok(())
    }

    fn validate_agg_array_types(
        &self,
        expected_input_types: &[Option<DataType>],
        kernels: &[agg::AggKernelEntry],
        arrays: &[Option<ArrayRef>],
    ) -> Result<(), String> {
        if expected_input_types.len() != arrays.len() || kernels.len() != arrays.len() {
            return Err("aggregate type length mismatch".to_string());
        }
        for (idx, array_opt) in arrays.iter().enumerate() {
            if self
                .functions
                .get(idx)
                .map(|f| f.input_is_intermediate)
                .unwrap_or(false)
            {
                let array = array_opt
                    .as_ref()
                    .ok_or_else(|| "aggregate intermediate input missing".to_string())?;
                let expected_type = kernels[idx].output_type(true);
                let actual_type = array.data_type();
                let is_struct_wrapped = match actual_type {
                    DataType::Struct(fields) if !fields.is_empty() => {
                        fields[0].data_type() == &expected_type
                    }
                    _ => false,
                };
                if actual_type != &expected_type && !is_struct_wrapped {
                    return Err(format!(
                        "aggregate intermediate type mismatch at {}: expected {:?}, got {:?}",
                        idx, expected_type, actual_type
                    ));
                }
                continue;
            }
            if self.functions[idx].name == "count" && self.functions[idx].inputs.is_empty() {
                if array_opt.is_some() {
                    return Err("count input should be none".to_string());
                }
                continue;
            }
            let expected_type = expected_input_types
                .get(idx)
                .and_then(|t| t.as_ref())
                .ok_or_else(|| "aggregate input type missing".to_string())?;
            let array = array_opt
                .as_ref()
                .ok_or_else(|| "aggregate input missing".to_string())?;
            if expected_type != array.data_type() {
                return Err(format!(
                    "aggregate input type mismatch at {}: expected {:?}, got {:?}",
                    idx, expected_type, array.data_type()
                ));
            }
        }
        Ok(())
    }

    fn ensure_scalar_group(&mut self) -> Result<(), String> {
        if !self.group_states.is_empty() {
            return Ok(());
        }
        self.alloc_group_state(0)
    }

    fn ensure_group_state(&mut self, lookup: &KeyLookup) -> Result<(), String> {
        if lookup.is_new {
            self.alloc_group_state(lookup.group_id)?;
        }
        Ok(())
    }

    fn alloc_group_state(&mut self, group_id: usize) -> Result<(), String> {
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        if group_id != self.group_states.len() {
            return Err("aggregate group id out of bounds".to_string());
        }
        let align = kernels
            .entries
            .iter()
            .map(|entry| entry.state_align())
            .max()
            .unwrap_or(1);
        let state_ptr = self.state_arena.alloc(kernels.layout.total_size, align);
        for kernel in &kernels.entries {
            kernel.init_state(state_ptr);
        }
        self.group_states.push(state_ptr);
        Ok(())
    }

    fn drop_group_states(&mut self) {
        let Some(kernels) = self.kernels.as_ref() else {
            self.group_states.clear();
            return;
        };
        for &state in &self.group_states {
            for kernel in &kernels.entries {
                kernel.drop_state(state);
            }
        }
        self.group_states.clear();
    }
}

impl Drop for AggregateStreamingSinkOperator {
    fn drop(&mut self) {
        self.drop_group_states();
    }
}
```

- [ ] **Step 2: Register submodule and re-export**

In `src/exec/operators/aggregate/mod.rs`, add the submodule declaration (after `streaming_state`):

```rust
pub(crate) mod streaming_sink;
```

Also make `ENABLE_GROUP_KEY_OPTIMIZATIONS` and `build_agg_views` accessible to sibling modules. They are currently private. Change:
- `const ENABLE_GROUP_KEY_OPTIMIZATIONS` → `pub(super) const ENABLE_GROUP_KEY_OPTIMIZATIONS`
- `fn build_agg_views` → `pub(super) fn build_agg_views`

- [ ] **Step 3: Re-export factory from operators/mod.rs**

In `src/exec/operators/mod.rs`, add to the re-exports:

```rust
pub use aggregate::streaming_sink::AggregateStreamingSinkFactory;
```

- [ ] **Step 4: Verify compilation**

Run: `cargo build 2>&1 | tail -30`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add src/exec/operators/aggregate/streaming_sink.rs src/exec/operators/aggregate/mod.rs src/exec/operators/mod.rs
git commit -m "Add AggregateStreamingSinkOperator with shared buffer output"
```

---

### Task 5: Create `AggregateStreamingSourceOperator`

**Files:**
- Create: `src/exec/operators/aggregate/streaming_source.rs`
- Modify: `src/exec/operators/aggregate/mod.rs` (add submodule)
- Modify: `src/exec/operators/mod.rs` (re-export)

- [ ] **Step 1: Create the streaming source module**

Create `src/exec/operators/aggregate/streaming_source.rs`:

```rust
use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::operators::aggregate::streaming_state::AggregateStreamingState;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;

pub struct AggregateStreamingSourceFactory {
    name: String,
    state: AggregateStreamingState,
}

impl AggregateStreamingSourceFactory {
    pub fn new(node_id: i32, state: AggregateStreamingState) -> Self {
        let name = if node_id >= 0 {
            format!("AGG_STREAMING_SOURCE (id={node_id})")
        } else {
            "AGG_STREAMING_SOURCE".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for AggregateStreamingSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AggregateStreamingSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct AggregateStreamingSourceOperator {
    name: String,
    state: AggregateStreamingState,
}

impl Operator for AggregateStreamingSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.state.is_done()
    }
}

impl ProcessorOperator for AggregateStreamingSourceOperator {
    fn need_input(&self) -> bool {
        false // Source convention
    }

    fn has_output(&self) -> bool {
        self.state.has_chunks()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("aggregate streaming source does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(self.state.poll_chunk())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(self.state.observable())
    }
}
```

- [ ] **Step 2: Register submodule and re-export**

In `src/exec/operators/aggregate/mod.rs`, add:

```rust
pub(crate) mod streaming_source;
```

In `src/exec/operators/mod.rs`, add:

```rust
pub use aggregate::streaming_source::AggregateStreamingSourceFactory;
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | tail -20`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/exec/operators/aggregate/streaming_source.rs src/exec/operators/aggregate/mod.rs src/exec/operators/mod.rs
git commit -m "Add AggregateStreamingSourceOperator pulling from shared buffer"
```

---

### Task 6: Wire pipeline builder to use Streaming Sink/Source

**Files:**
- Modify: `src/exec/pipeline/builder.rs`

This task adds the pipeline split logic. When the `AggregateNode` has
`streaming_preaggregation_mode = Some(ForcePreaggregation)`, the builder creates two pipelines
instead of a single processor. The existing blocking path is preserved for all other cases.

- [ ] **Step 1: Add imports**

Add these imports to the top of `src/exec/pipeline/builder.rs`:

```rust
use crate::exec::node::aggregate::StreamingPreaggregationMode;
use crate::exec::operators::aggregate::streaming_state::AggregateStreamingState;
use crate::exec::operators::{
    AggregateStreamingSinkFactory, AggregateStreamingSourceFactory,
};
```

(Add `AggregateStreamingSinkFactory` and `AggregateStreamingSourceFactory` to the existing `use crate::exec::operators::{...}` block, and add the other two as separate imports.)

- [ ] **Step 2: Add the streaming pipeline split**

In the `ExecNodeKind::Aggregate` match arm, right after the destructure and before the existing
pipeline logic (around line 661, after `let all_update = ...`), add a check for streaming mode.
Insert this block:

```rust
            // Streaming pre-aggregation: split into Sink (Pipeline 1) and Source (Pipeline 2).
            // This creates a pipeline boundary that lets TopN runtime filters take effect
            // between Scan and AGG.
            if let Some(StreamingPreaggregationMode::ForcePreaggregation) =
                streaming_preaggregation_mode
            {
                let streaming_state = AggregateStreamingState::new();
                let sink_factory = Box::new(AggregateStreamingSinkFactory::new(
                    *node_id,
                    Arc::clone(&ctx.arena),
                    group_by.clone(),
                    functions.clone(),
                    true, // output_intermediate: streaming pre-agg always outputs intermediate
                    output_chunk_schema.clone(),
                    topn_rf_specs.clone(),
                    Some(Arc::clone(&ctx.runtime_filter_hub)),
                    streaming_state.clone(),
                ));
                build.pipeline.factories.push(sink_factory);
                build.pipeline.needs_sink = false;

                let source_factory = Box::new(AggregateStreamingSourceFactory::new(
                    *node_id,
                    streaming_state,
                ));
                let source_dop = build.pipeline.dop;
                let downstream =
                    new_source_pipeline_with_dop(ctx, source_factory, source_dop);

                let mut extra_pipelines = build.extra_pipelines;
                extra_pipelines.push(build.pipeline);

                return Ok(PipelineBuildResult {
                    pipeline: downstream,
                    extra_pipelines,
                    stream: StreamDesc::any(source_dop),
                });
            }
```

**Important placement:** This block must go right after `let all_update = ...;` (around line 662)
and before the `if !*need_finalize && !group_by.is_empty() && dop > 1 {` check (line 664).
This ensures streaming mode is checked first and takes priority.

- [ ] **Step 3: Verify compilation**

Run: `cargo build 2>&1 | tail -20`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/exec/pipeline/builder.rs
git commit -m "Wire pipeline builder to split streaming AGG into Sink/Source pipelines"
```

---

### Task 7: Build and run basic verification

**Files:** None (verification only)

- [ ] **Step 1: Run cargo test**

Run: `cargo test 2>&1 | tail -30`
Expected: All existing tests pass. No regressions.

- [ ] **Step 2: Run cargo clippy**

Run: `cargo clippy 2>&1 | tail -30`
Expected: No new warnings in the changed files.

- [ ] **Step 3: Run cargo fmt**

Run: `cargo fmt`
Expected: No formatting changes needed (or apply them).

- [ ] **Step 4: Fix any issues found, then commit**

If any fixes were needed:

```bash
git add -A
git commit -m "Fix clippy/fmt issues in aggregate streaming operators"
```

---

### Task 8: End-to-end verification with standalone server

**Files:** None (verification only)

This verifies the new code path works with actual FE-generated plans. Queries that use
streaming pre-aggregation (most GROUP BY queries with `need_finalize=false` in the first phase)
should now flow through the new Sink/Source pipeline split.

- [ ] **Step 1: Start standalone server in debug mode**

Run: `NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030`
Expected: Server starts successfully.

- [ ] **Step 2: Run a simple GROUP BY query**

Connect with MySQL client and run:

```sql
SELECT lo_orderpriority, count(*) FROM ssb.lineorder GROUP BY lo_orderpriority;
```

Expected: Query returns correct results (same as before the change).

- [ ] **Step 3: Run SSB suite as smoke test**

In another terminal:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite ssb --mode verify --query-timeout 120
```

Expected: Same pass/fail as before. No regressions.

- [ ] **Step 4: Verify TopN RF takes effect (if applicable)**

Run a query with ORDER BY + LIMIT on a grouped result:

```sql
SELECT lo_orderpriority, count(*) as cnt
FROM ssb.lineorder
GROUP BY lo_orderpriority
ORDER BY cnt
LIMIT 3;
```

Expected: Correct results. In the future, with TopN RF fully wired, the Scan should observe
filter updates during execution (observable via debug logging or profiling, not a hard test).
