# Aggregate Streaming Sink/Source Operator for NovaRocks

## Status: Spec Only — Not Yet Implemented

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

## What NovaRocks Needs

### Phase 1: Streaming Sink/Source (Required for TopN RF)

Split the current `AggregateProcessorOperator` into:

**`AggregateStreamingSinkOperator`** (new):
- Implements `SinkOperator` trait (push-only, no pull)
- Accepts chunks via `push_chunk()`
- Aggregates into hash table
- Publishes TopN RF via `try_publish_topn_runtime_filter()` per-chunk
- Outputs partial results into a **bounded chunk buffer** (shared with source)
- Returns `need_input() = false` when buffer is full (backpressure)

**`AggregateStreamingSourceOperator`** (new):
- Implements `SourceOperator` trait (pull-only, no push)
- Pulls from the shared chunk buffer
- When buffer empty and sink finished, pulls remaining groups from hash table

**Shared state** between Sink and Source:
- `Arc<AggregatorState>` containing:
  - Hash table / key table
  - Aggregate state arena
  - Bounded chunk buffer (e.g., `crossbeam::ArrayQueue` or `tokio::sync::mpsc`)
  - Finishing flag

### Phase 2: Pipeline Builder Changes

When FE sends a streaming pre-aggregation node (`streaming_preaggregation_mode` is set):

```
Pipeline 1: ScanSource → AggStreamingSink
Pipeline 2: AggStreamingSource → [HashExchangeSink or next operator]
```

The pipeline builder creates two separate pipelines connected by the shared aggregator state.
This is analogous to how `LocalExchangeSink/Source` splits pipelines, but the exchange medium
is the aggregator's chunk buffer instead of a generic exchange channel.

When FE sends a blocking aggregation (merge/finalize phase):

```
Pipeline: ExchangeSource → AggBlockingSink → AggBlockingSource → next operator
```

This can remain as a single `ProcessorOperator` (current behavior) or be split into Sink/Source
for consistency.

### Phase 3: Streaming Mode Selection

Parse `TAggregationNode.streaming_preaggregation_mode` from thrift to decide:
- `FORCE_PREAGGREGATION` → always aggregate, buffer when full
- `FORCE_STREAMING` → pass through (useful for distribution-only pre-shuffle)
- `AUTO` → switch dynamically based on aggregation ratio (advanced)

For initial implementation, `FORCE_PREAGGREGATION` is sufficient.

## Interaction with TopN Runtime Filter

The TopN RF publishing code already exists in the current `AggregateProcessorOperator`:
- `try_publish_topn_runtime_filter()` computes min/max from key table and publishes to hub
- Called in `push_chunk()` after each batch

When the operator is split into Streaming Sink/Source:
- The **Sink** retains `try_publish_topn_runtime_filter()` (called per-chunk in `push_chunk()`)
- The **Source** does NOT publish (it only reads from buffer/hash table)
- The pipeline split ensures Scan and AGG Sink are in different pipelines
- Backpressure from the bounded buffer throttles Scan
- Scan checks for new filters on each `pull_chunk()` → `execute_iter()` per morsel

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
| `src/exec/operators/aggregate/streaming_sink.rs` | **New**: AggregateStreamingSinkOperator |
| `src/exec/operators/aggregate/streaming_source.rs` | **New**: AggregateStreamingSourceOperator |
| `src/exec/operators/aggregate/shared_state.rs` | **New**: Shared aggregator state with bounded buffer |
| `src/exec/operators/aggregate/mod.rs` | Re-export new types, keep existing ProcessorOperator for blocking |
| `src/exec/node/aggregate.rs` | Add `streaming_mode` field to AggregateNode |
| `src/lower/node/aggregate.rs` | Parse `streaming_preaggregation_mode` from thrift |
| `src/exec/pipeline/builder.rs` | Create two-pipeline structure for streaming AGG |

## Out of Scope (for initial implementation)

- AUTO mode (dynamic streaming/preaggregation switching)
- LIMITED_MEM mode
- SortedAggregateStreaming variant
- Spillable aggregation variants
- AGG In-Filter (separate from TopN RF, already partially exists)
- Multi-column TopN filters
- Remote filter publishing
