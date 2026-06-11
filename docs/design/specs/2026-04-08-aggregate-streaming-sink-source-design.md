# Aggregate Streaming Sink/Source Operator for NovaRocks

## Status: Spec Finalized — Ready for Implementation

## Problem

NovaRocks currently has a single `AggregateProcessorOperator` that operates in **blocking mode**: it
collects all input via `push_chunk()`, builds the full hash table, then outputs results on
`pull_chunk()` after `set_finishing()`. Scan and AGG are in the same pipeline and execute
synchronously — Scan pushes all data, then AGG processes and outputs.

This prevents **AGG TopN runtime filters** from working. In StarRocks, the streaming pre-aggregation
operator (`AggregateStreamingSinkOperator`) publishes MinMax filters incrementally as data flows
through, and the Scan operator picks them up via a shared `RuntimeFilterHub` between pipeline
stages. The bounded chunk buffer between Sink and Source provides natural backpressure, ensuring the
Scan doesn't race ahead of the AGG's filter publishing.

## StarRocks Architecture Reference

StarRocks has multiple aggregate operator variants, all following a **Sink + Source** pattern:

```
Pipeline 1: ScanSource → ... → AggStreamingSink
                                    │ writes to bounded chunk buffer
Pipeline 2: AggStreamingSource → ... → ExchangeSink
                reads from buffer
```

### Operator Types

| Variant | Sink | Source | When Used |
|---------|------|--------|-----------|
| Streaming | AggregateStreamingSinkOperator | AggregateStreamingSourceOperator | Phase 1 pre-aggregation |
| Blocking | AggregateBlockingSinkOperator | AggregateBlockingSourceOperator | Phase 2 merge/finalize |
| SortedStreaming | SortedAggregateStreamingSinkOperator | SortedAggregateStreamingSourceOperator | Pre-sorted input |
| Spillable | SpillableAggregateBlockingSinkOperator | SpillableAggregateBlockingSourceOperator | Memory pressure |

### Streaming Sink Behavior

The streaming sink has multiple modes controlled by `streaming_preaggregation_mode`:
- **FORCE_PREAGGREGATION**: Always aggregate in hash table, output when buffer full
- **FORCE_STREAMING**: Pass through without aggregation (just hash for distribution)
- **AUTO**: Dynamically switch between streaming and preaggregation based on aggregation ratio
- **LIMITED_MEM**: Memory-bounded preaggregation

Key behaviors:
- `push_chunk()`: Aggregates input, may produce output chunks into a bounded buffer
- `need_input()`: Returns false when chunk buffer is full (backpressure)
- `_build_topn_runtime_filter()`: Called per-chunk during preaggregation, publishes MinMax filter
- `_build_in_runtime_filters()`: Called to build AGG In-Filter (separate feature)

### Streaming Source Behavior

- Drains chunk buffer first (streaming results)
- Then outputs remaining groups from hash table
- Finishes when both buffer and hash table are exhausted

### Blocking Sink/Source

- Sink: Accumulates all input into hash table, no output during `push_chunk()`
- Source: Iterates over hash table to produce output chunks
- Used for merge/finalize phase (phase 2)
- Builds In-Filter (not TopN filter) at finalization

## Design Decisions

### Operator Trait: Reuse `ProcessorOperator` (Approach A)

NovaRocks does not have separate `SinkOperator` / `SourceOperator` traits. All operators implement
`ProcessorOperator`. The new streaming operators follow the established **AnalyticSink/Source
pattern**:

- **Sink convention**: `has_output()` → `false`, `pull_chunk()` → `Ok(None)`.
  `push_chunk()` / `need_input()` / `set_finishing()` carry the real logic.
- **Source convention**: `need_input()` → `false`, `push_chunk()` → error/no-op.
  `has_output()` / `pull_chunk()` carry the real logic.
- Factory: `OperatorFactory` with `is_sink() = true` / `is_source() = true`.

This requires zero changes to pipeline/driver infrastructure.

### Shared State: `AggregateStreamingState` (Mutex + VecDeque + Observable)

Following the `AnalyticSharedState` pattern (`analytic_shared.rs`), shared state between Sink and
Source uses:

```rust
pub(crate) struct AggregateStreamingState {
    inner: Arc<Mutex<BufferState>>,
    observable: Arc<Observable>,
    buffer_limit: usize,  // from operator_buffer_chunks() config
}

struct BufferState {
    chunks: VecDeque<Chunk>,
    sink_finished: bool,
    has_remaining_groups: bool,  // hash table not yet fully drained
}
```

- **Sink writes**: `offer_chunk(chunk)` — pushes to `chunks`, notifies via `observable`
- **Sink signals finish**: `mark_sink_finished()` — sets flag, notifies source
- **Source reads**: `poll_chunk()` → `Option<Chunk>` from front of deque
- **Backpressure**: `is_buffer_full()` → `chunks.len() >= buffer_limit`
- **Source observable**: returned by `source_observable()` for driver scheduling

The hash table, key table, aggregate states, and kernels remain **inside the Sink operator** (not
in shared state). When the Sink finishes, it materializes remaining groups into chunks and pushes
them into the buffer. The Source only reads from the buffer — it never touches the hash table
directly.

This is simpler than StarRocks's design (where Source can iterate the hash table directly), but
avoids complex shared mutable access to the hash table across pipeline boundaries. The tradeoff is
that the Sink must serialize remaining groups into the buffer at finish time, which adds one
materialization pass but keeps the concurrency model clean.

### Streaming Mode: FORCE_PREAGGREGATION Only (Initial Scope)

Parse `streaming_preaggregation_mode` from thrift but only implement `FORCE_PREAGGREGATION`:
- Always aggregate into hash table
- Output intermediate results to buffer when buffer has space
- Other modes (AUTO, FORCE_STREAMING, LIMITED_MEM) return unsupported error

## What NovaRocks Needs

### Component 1: AggregateStreamingSinkOperator

New operator implementing `ProcessorOperator` (sink convention):

```rust
struct AggregateStreamingSinkOperator {
    // Aggregation state (owned, not shared)
    key_table: Option<KeyTable>,
    group_states: Vec<agg::AggStatePtr>,
    kernels: Option<agg::AggKernelSet>,
    state_arena: agg::AggStateArena,

    // Shared buffer with Source
    streaming_state: AggregateStreamingState,

    // TopN runtime filter (existing logic, moved from ProcessorOperator)
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    topn_rf_rows_since_publish: usize,

    // State flags
    finishing: bool,
    finished: bool,
}
```

Key behaviors:
- `need_input()`: `!finishing && !finished`
- `push_chunk()`:
  1. Evaluate group-by exprs and aggregate inputs
  2. Insert/update hash table (reuse existing `process()` logic)
  3. Call `try_publish_topn_runtime_filter()` (existing code, unchanged)
  4. **No intermediate output** — FORCE_PREAGGREGATION accumulates all data in hash table.
     The pipeline split alone provides the yield points needed for TopN RF to work.
- `set_finishing()`:
  1. Materialize all groups from hash table into chunks
  2. Push all result chunks into buffer via `offer_chunk()`
  3. Call `streaming_state.mark_sink_finished()`
- `has_output()`: `false` (sink convention)
- `pull_chunk()`: `Ok(None)` (sink convention)

### Component 2: AggregateStreamingSourceOperator

New operator implementing `ProcessorOperator` (source convention):

```rust
struct AggregateStreamingSourceOperator {
    streaming_state: AggregateStreamingState,
}
```

Key behaviors:
- `has_output()`: `streaming_state.has_chunks()` (buffer non-empty, or sink finished with remaining)
- `pull_chunk()`: `streaming_state.poll_chunk()`
- `is_finished()`: `streaming_state.is_done()` (sink finished AND buffer empty)
- `need_input()`: `false` (source convention)
- `push_chunk()`: error (source convention)
- `source_observable()`: `Some(streaming_state.observable())`

### Component 3: AggregateStreamingState

Shared state between Sink and Source (described in Design Decisions above). Lives in its own file
following the `analytic_shared.rs` pattern.

### Component 4: Pipeline Builder Changes

When the pipeline builder encounters an `AggregateNode` with `streaming_preaggregation_mode` set
(and the mode is not `None`):

```
Pipeline 1: [upstream operators] → AggregateStreamingSink
Pipeline 2: AggregateStreamingSource → [downstream operators]
```

The builder:
1. Creates `AggregateStreamingState` (shared between factories)
2. Creates `AggregateStreamingSinkFactory` with the shared state → appends to current pipeline
3. Creates `AggregateStreamingSourceFactory` with the same shared state → starts new pipeline
4. Continues building downstream operators on the new pipeline

This is analogous to how the builder handles `AnalyticNode` (creates `AnalyticSinkFactory` +
`AnalyticSourceFactory` with shared `AnalyticSharedState`).

When streaming mode is not set (blocking aggregation), the existing `AggregateProcessorOperator`
is used unchanged.

### Component 5: Lowering Changes

Add `streaming_preaggregation_mode` field to `AggregateNode`:

```rust
pub struct AggregateNode {
    // ... existing fields ...
    pub streaming_preaggregation_mode: Option<StreamingPreaggregationMode>,
}

pub enum StreamingPreaggregationMode {
    Auto,
    ForceStreaming,
    ForcePreaggregation,
    LimitedMem,
}
```

Parse from `TAggregationNode.streaming_preaggregation_mode` (thrift field 7) in
`lower_aggregate_node()`.

## Interaction with TopN Runtime Filter

The TopN RF publishing code already exists in the current `AggregateProcessorOperator`:
- `try_publish_topn_runtime_filter()` computes min/max from key table and publishes to hub
- Called in `push_chunk()` after each batch

When the operator is split into Streaming Sink/Source:
- The **Sink** retains `try_publish_topn_runtime_filter()` (called per-chunk in `push_chunk()`)
- The **Source** does NOT publish (it only reads from buffer)
- The pipeline split ensures Scan and AGG Sink are in different pipelines **but within the same
  pipeline group** — Scan is in Pipeline 1 upstream of the Sink
- Backpressure: when buffer is full, `Sink.need_input() = false` → driver stops pulling from Scan
  → Scan's driver yields → on next scheduling, Scan checks RuntimeFilterHub for new filters
- This yield-and-check cycle is the mechanism that makes TopN RF effective

## Thrift Fields to Parse

From `TAggregationNode`:
- `streaming_preaggregation_mode: TStreamingPreaggregationMode` (field 7) — controls streaming behavior
- `need_finalize: bool` (field 4) — if false, use streaming; if true, may use blocking
- `build_runtime_filters: list<TRuntimeFilterDescription>` (field 30) — already parsed
- `use_sort_agg: bool` (field 27) — sorted streaming variant (future)

From `TStreamingPreaggregationMode` enum:
```thrift
enum TStreamingPreaggregationMode {
    AUTO,
    FORCE_STREAMING,
    FORCE_PREAGGREGATION,
    LIMITED_MEM
}
```

## Files to Create/Modify

| File | Change |
|------|--------|
| `src/exec/operators/aggregate/streaming_state.rs` | **New**: AggregateStreamingState (shared buffer) |
| `src/exec/operators/aggregate/streaming_sink.rs` | **New**: AggregateStreamingSinkOperator + Factory |
| `src/exec/operators/aggregate/streaming_source.rs` | **New**: AggregateStreamingSourceOperator + Factory |
| `src/exec/operators/aggregate/mod.rs` | Re-export new types, keep existing ProcessorOperator for blocking |
| `src/exec/node/aggregate.rs` | Add `streaming_preaggregation_mode` field and enum |
| `src/lower/node/aggregate.rs` | Parse `streaming_preaggregation_mode` from thrift |
| `src/exec/pipeline/builder.rs` | Create two-pipeline structure for streaming AGG |

## Out of Scope (for initial implementation)

- AUTO mode (dynamic streaming/preaggregation switching)
- LIMITED_MEM mode
- FORCE_STREAMING mode (pass-through without aggregation)
- SortedAggregateStreaming variant
- Spillable aggregation variants
- AGG In-Filter (separate from TopN RF, already partially exists)
- Multi-column TopN filters
- Remote filter publishing
- Source directly iterating hash table (all output goes through buffer)
