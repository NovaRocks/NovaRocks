# AGG TopN Runtime Filter for NovaRocks

## Goal

Implement aggregate TopN runtime filter in NovaRocks BE (aligned with StarRocks C++ implementation),
then write red-light tests using StarRocks FE + NovaRocks BE to reproduce the FE `exprOrder` bug
(StarRocksTest#11224).

## Background

StarRocks PR #67452 added AGG TopN runtime filter: for `GROUP BY + ORDER BY + LIMIT` queries,
the AGG operator incrementally builds a `MinMaxRuntimeFilter` from the TopN boundary of the
ORDER BY column and publishes it to the Scan operator. Scan pushes it down to the storage layer
as range predicates for segment/page pruning.

The FE has a bug: `exprOrder` is hardcoded to 0, but the ORDER BY column may not be the first
GROUP BY column. This causes:
1. **Type mismatch error** when types differ (e.g., GROUP BY date, datetime ORDER BY datetime)
2. **Silent wrong results** when types match but column data differs

## Data Flow (aligned with StarRocks)

```
Scan ←──── storage layer applies MinMaxPredicate for segment pruning
  │                          ↑
  │           convert RuntimeMinMaxFilter → MinMaxPredicate
  │                          ↑
  │           RuntimeFilterHub (stores versioned MinMax filters)
  │                          ↑
  ↓                          │ publish periodically
AGG (streaming: push_chunk → update hash map → compute min/max of
     group_by_columns[expr_order] → publish MinMaxRuntimeFilter)
  │
  ↓
Sort → Output
```

Key: Scan and AGG run in parallel. AGG publishes the filter mid-stream;
Scan applies it to subsequent morsels via the storage predicate pushdown path.

## Implementation

### 1. Lowering: Thrift → ExecNode

**File**: `src/lower/node/aggregate.rs`

- Parse `TAggregationNode.build_runtime_filters` (list of `TRuntimeFilterDescription`)
- Filter for `filter_type == TOPN_FILTER`
- Extract: `filter_id`, `expr_order`, `build_expr` type (TPrimitiveType), probe slot mapping
- Store as `TopNRuntimeFilterSpec` on `AggregateNode`
- Also parse probe-side specs in `src/lower/node/starrocks_scan.rs` for TOPN_FILTER type

### 2. RuntimeFilterHub: MinMax Filter Support

**File**: `src/runtime/runtime_filter_hub.rs`

- Add `published_min_max_filters: RwLock<HashMap<i32, Arc<RuntimeMinMaxFilter>>>` to `RuntimeFilterHubInner`
- Add `publish_min_max_filter(filter_id, filter)` method
- Add `min_max_filters: Vec<Arc<RuntimeMinMaxFilter>>` to `RuntimeFilterSnapshot`
- Update `snapshot()` to include min_max_filters

**File**: `src/exec/runtime_filter/min_max.rs`

- Add `RuntimeMinMaxFilter::from_array(ltype, array: &ArrayRef) -> Result<Self>` - compute min/max from an Arrow array
- Add `RuntimeMinMaxFilter::to_min_max_predicates(column_name: &str) -> Vec<MinMaxPredicate>` - convert to storage predicates

### 3. AGG Operator: Incremental Build + Publish

**File**: `src/exec/operators/aggregate/mod.rs`

Add to `AggregateProcessorFactory`:
- `topn_rf_specs: Vec<TopNRuntimeFilterSpec>`
- `runtime_filter_hub: Option<Arc<RuntimeFilterHub>>`

Add to `AggregateProcessorOperator`:
- `topn_rf_specs: Vec<TopNRuntimeFilterSpec>`
- `runtime_filter_hub: Option<Arc<RuntimeFilterHub>>`
- `topn_rf_rows_since_publish: usize` (throttle publishing)
- `topn_rf_published: bool`

In `push_chunk()`, after processing each batch:
```
rows_since_publish += chunk.num_rows()
if topn_rf_specs is not empty
   AND key_table.num_groups() >= limit
   AND rows_since_publish >= threshold (e.g., 4096):
    for each spec in topn_rf_specs:
        let column_array = key_table.key_column(spec.expr_order)
        let filter = RuntimeMinMaxFilter::from_array(spec.ltype, &column_array)?
        runtime_filter_hub.publish_min_max_filter(spec.filter_id, filter)
    rows_since_publish = 0
```

### 4. Pipeline Builder: Thread Hub to AGG

**File**: `src/exec/pipeline/builder.rs`

- When building AggregateProcessorFactory for nodes with `topn_rf_specs`:
  - Pass `ctx.runtime_filter_hub.clone()` to factory
  - Register build specs with hub: `hub.register_filter_specs(node_id, specs)`

### 5. Scan Probe: Storage-Level Range Pruning

**File**: `src/connector/starrocks/scan/op.rs`

In `StarRocksScanOp::execute_iter()`:
- Extract `RuntimeMinMaxFilter` from `RuntimeFilterContext`
- Convert to `Vec<MinMaxPredicate>` using `filter.to_min_max_predicates(column_name)`
- Merge with existing `cfg.min_max_predicates`
- Storage reader applies them at segment level via existing zone-map pruning

**File**: `src/exec/operators/scan/runner.rs`

- In `prepare_runtime_filters()`, register for TOPN_FILTER type
- Between morsels, check `RuntimeFilterHandle.version()` for updates
- Pass latest `RuntimeFilterContext` to each `execute_iter()` call

**File**: `src/exec/node/scan.rs`

- Add `min_max_filters: Vec<Arc<RuntimeMinMaxFilter>>` to `RuntimeFilterSnapshot`
- Add to `RuntimeFilterContext` so `StarRocksScanOp` can access them

### 6. Lowering: Scan Probe Registration

**File**: `src/lower/node/starrocks_scan.rs`

- For TOPN_FILTER type probe descriptors, register with RuntimeFilterHub
- Map `probe_expr` slot to scan output column name for MinMaxPredicate creation

## Files to Modify

| File | Change |
|------|--------|
| `src/lower/node/aggregate.rs` | Parse `build_runtime_filters` for TOPN_FILTER |
| `src/lower/node/starrocks_scan.rs` | Register TOPN_FILTER probe specs |
| `src/exec/node/aggregate.rs` | Add `topn_rf_specs` field |
| `src/exec/node/scan.rs` | Add min_max_filters to RuntimeFilterSnapshot/Context |
| `src/exec/operators/aggregate/mod.rs` | Factory takes hub; operator builds + publishes MinMax |
| `src/exec/pipeline/builder.rs` | Thread hub to AGG factory |
| `src/runtime/runtime_filter_hub.rs` | MinMax filter storage, publish, snapshot |
| `src/exec/runtime_filter/min_max.rs` | `from_array()`, `to_min_max_predicates()` |
| `src/connector/starrocks/scan/op.rs` | Extract MinMax from RuntimeFilterContext → MinMaxPredicate |
| `src/exec/operators/scan/runner.rs` | Pass runtime filters to execute_iter per morsel |

## Red-Light Tests

Using sql-test-runner with **unpatched** StarRocks FE + NovaRocks BE.

### Test 1: Type mismatch (error)

```sql
CREATE TABLE t_topn_rf_type_mismatch (
    k1 DATE, k2 DATETIME, v BIGINT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;

-- Insert enough data to trigger AGG TopN filter (> limit rows)
INSERT INTO t_topn_rf_type_mismatch VALUES
    ('2024-01-01', '2024-06-15 10:00:00', 1),
    ('2024-01-02', '2024-06-16 11:00:00', 2),
    ... -- hundreds of distinct (k1, k2) pairs

SET enable_topn_runtime_filter = true;
-- GROUP BY k1(DATE), k2(DATETIME), ORDER BY k2(DATETIME)
-- FE sends expr_order=0 → BE builds MinMax<DATE> → probe expects DATETIME
-- Expected: type error or incorrect behavior
SELECT k1, k2, count(*) FROM t_topn_rf_type_mismatch
GROUP BY k1, k2 ORDER BY k2 LIMIT 5;
```

### Test 2: Same type, wrong data (silent corruption)

```sql
CREATE TABLE t_topn_rf_wrong_data (
    k1 INT, k2 INT, v BIGINT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;

-- k1 has wide range [1, 10000], k2 has narrow range [1, 10]
-- So k1's min/max applied to k2 would NOT filter anything useful,
-- but k2's actual top-5 by ORDER BY k2 should be deterministic
INSERT INTO t_topn_rf_wrong_data
SELECT generate_series, (generate_series % 10) + 1, generate_series
FROM TABLE(generate_series(1, 10000));

-- Baseline: correct result with TopN RF disabled
SET enable_topn_runtime_filter = false;
SELECT k1, k2, count(*) FROM t_topn_rf_wrong_data
GROUP BY k1, k2 ORDER BY k2 LIMIT 5;

-- Bug: with TopN RF enabled, wrong expr_order causes wrong filter
SET enable_topn_runtime_filter = true;
SELECT k1, k2, count(*) FROM t_topn_rf_wrong_data
GROUP BY k1, k2 ORDER BY k2 LIMIT 5;
-- Results should differ from baseline (demonstrating the bug)
```

## Out of Scope

- Heap-based incremental TopN tracking with eviction (use simple min/max from all current group keys)
- Multi-column TopN filters (only first ORDER BY column)
- Remote filter publishing (local only, sufficient for single-BE test)
- TopN filter from standalone SortNode
- AGG In-Filter (separate feature)
